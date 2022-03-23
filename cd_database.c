#include "internal.h"

uint64_t cd_database_create(const char *name)
{
	uint64_t return_value = 0;

	CC_String db_name = cc_string_create(name, 0);

	CC_String schema_file_path;
	{
		CC_StringBuffer *buffer = cc_string_buffer_create(CD_NAME_LENGTH);

		cc_string_buffer_insert_string(buffer, db_name);
		cc_string_buffer_insert_char(buffer, '/');
		cc_string_buffer_insert_string(buffer, db_name);
		CC_String file_extension = cc_string_create(".schema", 0);
		cc_string_buffer_insert_string(buffer, file_extension);
		cc_string_destroy(file_extension);

		schema_file_path = cc_string_buffer_to_string_and_destroy(buffer);
	}

	if(cf_directory_exists(db_name))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to create database '%s'. Database already exists", db_name.data);
		goto schema_file_path_destroy;
	}

	if(!cf_directory_create(db_name))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to create folder '%s'", db_name.data);
		goto schema_file_path_destroy;
	}

	if (!cf_file_exists(schema_file_path))
	{
		if (!cf_file_create(schema_file_path, sizeof(uint64_t)))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to create database schema file '%s'.", schema_file_path.data);
			goto schema_file_path_destroy;
		}
	}

	CF_File *schema_file = cf_file_open(schema_file_path);
	if (schema_file == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open database schema file '%s'.", schema_file_path.data);
		goto schema_file_path_destroy;
	}

	CF_FileView *schema_count_view = cf_file_view_open(schema_file, 0, sizeof(uint64_t));
	if (schema_count_view == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open file view '%s' for table counts.", schema_file_path.data);
		goto schema_file_close;
	}

	uint64_t count = 0;
	if (!cf_file_view_write(schema_count_view, 0, sizeof(uint64_t), &count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to read schema count from '%s' file", schema_file_path.data);
		goto schema_count_view_close;
	}

	return_value = 1;

schema_count_view_close:
	cf_file_view_close(schema_count_view);
schema_file_close:
	cf_file_close(schema_file);
schema_file_path_destroy:
	cc_string_destroy(schema_file_path);
db_name_destroy:
	cc_string_destroy(db_name);

	return return_value;
}

uint64_t cd_database_exists(const char *name)
{
	CC_String db_name = cc_string_create(name, 0);
	uint64_t exists = cf_directory_exists(db_name);
	cc_string_destroy(db_name);
	return exists;
}

CD_Database *cd_database_open(const char *name)
{
	CC_String db_name = cc_string_create(name, 0);

	CC_String schema_file_path;
	{
		CC_StringBuffer *buffer = cc_string_buffer_create(CD_NAME_LENGTH);

		cc_string_buffer_insert_string(buffer, db_name);
		cc_string_buffer_insert_char(buffer, '/');
		cc_string_buffer_insert_string(buffer, db_name);
		CC_String file_extension = cc_string_create(".schema", 0);
		cc_string_buffer_insert_string(buffer, file_extension);
		cc_string_destroy(file_extension);

		schema_file_path = cc_string_buffer_to_string_and_destroy(buffer);
	}

	// create if not exists
	if(!cf_directory_exists(db_name))
	{
		_cd_make_error(CD_ERROR_DATABASE_DOES_NOT_EXIST, "Failed to open database '%s'. It does not exist.", db_name.data);
		goto schema_file_path_destroy;
	}

	// load table count
	CF_File *schema_file = cf_file_open(schema_file_path);
	if (schema_file == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open database schema file '%s'.", schema_file_path.data);
		goto schema_file_path_destroy;
	}

	CF_FileView *schema_count_view = cf_file_view_open(schema_file, 0, sizeof(uint64_t));
	if (schema_count_view == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open file view '%s' for table counts.", schema_file_path.data);
		goto schema_file_close;
	}

	uint64_t table_count;
	if (!cf_file_view_read(schema_count_view, 0, sizeof(uint64_t), &table_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to read schema count from '%s' file", schema_file_path.data);
		goto schema_count_view_close;
	}

	uint64_t table_offset = sizeof(uint64_t);

	CC_HashMap *table_schemas = cc_hash_map_create(sizeof(CD_TableSchema), table_count);

	// load schemas
	for (uint64_t table_index = 0; table_index < table_count; table_index++)
	{
		uint64_t _error = 0;

		_CD_File_TableSchema file_table_schema;

		CF_FileView *table_data_view = cf_file_view_open(schema_file, table_offset, sizeof(file_table_schema));
		if (table_data_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open table_data_view of schema at index %d for file '%s'", table_index, schema_file_path.data);
			_error = 1;
			goto table_schemas_destroy;
		}

		if (!cf_file_view_read(table_data_view, 0, sizeof(file_table_schema), &file_table_schema))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to read table_data of schema at index %d for file '%s'", table_index, schema_file_path.data);
			_error = 1;
			goto table_data_view_close;
		}

		CF_FileView *table_attributes_view = cf_file_view_open(schema_file, table_offset + sizeof(file_table_schema), file_table_schema.attrib_count_c * sizeof(CD_Attribute));
		if (table_attributes_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open table_attributes_view of schema at index %d for file '%s'", table_index, schema_file_path.data);
			_error = 1;
			goto table_data_view_close;
		}

		CD_TableSchema schema = 
		{
			.attribute_indices = cc_hash_map_create(sizeof(uint64_t), file_table_schema.attrib_count_c),
			.attributes = malloc(sizeof(CD_AttributeEx) * file_table_schema.attrib_count_c),
			.stride = 0
		};

		for(uint64_t attrib_index = 0; attrib_index < file_table_schema.attrib_count_c; attrib_index++)
		{
			CD_Attribute file_attribute;

			if (!cf_file_view_read(table_attributes_view, attrib_index * sizeof(file_attribute), sizeof(file_attribute), &file_attribute))
			{
				_cd_make_error(CD_ERROR_FILE, "Failed to read attribute_data of schema at attribute_index %d and at table_index %d for file '%s'", attrib_index, table_index, schema_file_path.data);
				_error = 1;
				goto schema_destroy;
			}

			// insert attrib index into hash map
			{
				CC_String attribute_name = cc_string_create(file_attribute.name, 0);
				cc_hash_map_insert(schema.attribute_indices, attribute_name, &attrib_index);
				cc_string_destroy(attribute_name);
			}

			CD_AttributeEx *attribute = schema.attributes + attrib_index;

			memset(attribute->name, 0, CD_NAME_LENGTH);
			strcpy_s(attribute->name, CD_NAME_LENGTH, file_attribute.name);
			attribute->type = file_attribute.type;
			attribute->count = file_attribute.count;
			attribute->constraints = file_attribute.constraints;
			attribute->offset = schema.stride;
			attribute->size = cd_attribute_size(file_attribute.type, file_attribute.count);

			schema.stride += attribute->size;
		}

		table_offset += sizeof(file_table_schema) + file_table_schema.attrib_count_m * sizeof(CD_Attribute);

		// insert schema into hash map
		{
			CC_String table_name = cc_string_create(file_table_schema.name, 0);
			cc_hash_map_insert(table_schemas, table_name, &schema);
			cc_string_destroy(table_name);
		}

	schema_destroy:
		if(_error)
		{
			cc_hash_map_destroy(schema.attribute_indices);
			free(schema.attributes);
		}
	table_data_view_close:
		cf_file_view_close(table_data_view);
		if(_error)
		{
			goto table_schemas_destroy;
		}
	}

	CD_Database *db = malloc(sizeof(*db));

	db->name = db_name;
	db->schema_file_path = schema_file_path;

	db->table_schemas = table_schemas;

	db->schema_file = schema_file;
	db->schema_count_view = schema_count_view;

	return db;

table_schemas_destroy:
	for (CC_HashMap_Element *element = cc_hash_map_iterator_begin(table_schemas); element != cc_hash_map_iterator_end(table_schemas); element = cc_hash_map_iterator_next(table_schemas, element))
	{
		CD_TableSchema *schema = (CD_TableSchema *)element->data;

		cc_hash_map_destroy(schema->attribute_indices);
		free(schema->attributes);
	}
	cc_hash_map_destroy(table_schemas);
schema_count_view_close:
	cf_file_view_close(schema_count_view);
schema_file_close:
	cf_file_close(schema_file);
schema_file_path_destroy:
	cc_string_destroy(schema_file_path);
	cc_string_destroy(db_name);

	return NULL;
}

void cd_database_close(CD_Database *db)
{
	for (CC_HashMap_Element *element = cc_hash_map_iterator_begin(db->table_schemas); element != cc_hash_map_iterator_end(db->table_schemas); element = cc_hash_map_iterator_next(db->table_schemas, element))
	{
		CD_TableSchema *schema = (CD_TableSchema *)element->data;

		cc_hash_map_destroy(schema->attribute_indices);
		free(schema->attributes);
	}
	cc_hash_map_destroy(db->table_schemas);

	cf_file_view_close(db->schema_count_view);
	cf_file_close(db->schema_file);
}
