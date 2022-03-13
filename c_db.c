#include "internal.h"

CD_Database *cd_database_open(CC_String _db_name)
{
	CD_Database *db = NULL;
	
	CC_String db_name = cc_string_copy(_db_name);

	CC_String schema_file_path;
	{
		CC_StringBuffer *buffer = cc_string_buffer_create(256);

		cc_string_buffer_insert_string(buffer, db_name);
		cc_string_buffer_insert_char(buffer, '/');
		cc_string_buffer_insert_string(buffer, db_name);
		CC_String file_extension = cc_string_create(".schema", 0);
		cc_string_buffer_insert_string(buffer, file_extension);
		cc_string_destroy(file_extension);

		schema_file_path = cc_string_buffer_to_string_and_destroy(buffer);
	}

	// create if not exists
	if (cf_directory_create(db_name))
	{
		uint64_t _error = 0;

		if (!cf_file_exists(schema_file_path))
		{
			if (!cf_file_create(schema_file_path, sizeof(uint64_t)))
			{
				_cd_make_error(CD_ERROR_FILE, "Failed to create database schema file '%s'.", schema_file_path.data);
				_error = 1;
				goto schema_file_path_destroy;
			}
		}
		CF_File *schema_file = cf_file_open(schema_file_path);
		if (schema_file == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open database schema file '%s'.", schema_file_path.data);
			_error = 1;
			goto schema_file_path_destroy;
		}

		CF_FileView *schema_count_view = cf_file_view_open(schema_file, 0, sizeof(uint64_t));
		if (schema_count_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open file view '%s' for table counts.", schema_file_path.data);
			_error = 1;
			goto _schema_file_close;
		}

		uint64_t count = 0;
		if (!cf_file_view_write(schema_count_view, 0, sizeof(uint64_t), &count))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to read schema count from '%s' file", schema_file_path.data);
			_error = 1;
			goto _schema_count_view_close;
		}

	_schema_count_view_close:
		cf_file_view_close(schema_count_view);
	_schema_file_close:
		cf_file_close(schema_file);
		if (_error)
		{
			goto schema_file_path_destroy;
		}
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

	CC_HashMap *tables = cc_hash_map_create(sizeof(CD_Table), table_count);

	// for load tables
	for (uint64_t table_index = 0; table_index < table_count; table_index++)
	{
		uint64_t _error = 0;

		_CD_TableSchemaData table_data;

		CF_FileView *table_data_view = cf_file_view_open(schema_file, table_offset, sizeof(table_data));
		if (table_data_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open table_data_view of schema at index %d for file '%s'", table_index, schema_file_path.data);
			_error = 1;
			goto schema_count_view_close;
		}

		if (!cf_file_view_read(table_data_view, 0, sizeof(table_data), &table_data))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to read table_data of schema at index %d for file '%s'", table_index, schema_file_path.data);
			_error = 1;
			goto table_data_view_close;
		}

		CF_FileView *table_attributes_view = cf_file_view_open(schema_file, table_offset + sizeof(table_data), table_data.attrib_count_c * sizeof(CD_Attribute));
		if (table_attributes_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open table_attributes_view of schema at index %d for file '%s'", table_index, schema_file_path.data);
			_error = 1;
			goto table_data_view_close;
		}

		CC_String table_name =
		{
			.data = table_data.name,
			.length = strlen(table_data.name)
		};

		CC_String table_file_path;
		{
			CC_StringBuffer *buffer = cc_string_buffer_create(256);
			cc_string_buffer_insert_string(buffer, db_name);
			cc_string_buffer_insert_char(buffer, '/');
			cc_string_buffer_insert_string(buffer, table_name);
			CC_String file_extension = cc_string_create(".table", 0);
			cc_string_buffer_insert_string(buffer, file_extension);
			cc_string_destroy(file_extension);

			table_file_path = cc_string_buffer_to_string_and_destroy(buffer);
		}

		CF_File *table_file = cf_file_open(table_file_path);
		if (table_file == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open table_file at index %d for file '%s'", table_index, table_file_path.data);
			_error = 1;
			goto table_file_path_destroy;
		}

		struct
		{
			uint64_t count_c;
			uint64_t count_m;
		} table_data_count;

		CF_FileView *data_count_view = cf_file_view_open(table_file, 0, sizeof(table_data_count));
		if (data_count_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open data_count_view at index %d for file '%s'", table_index, table_file_path.data);
			_error = 1;
			goto table_file_close;
		}

		if (!cf_file_view_read(data_count_view, 0, sizeof(table_data_count), &table_data_count))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to read data count at index %d for file '%s'", table_index, table_file_path.data);
			_error = 1;
			goto data_count_view_close;
		}

		CD_Table table =
		{
			.name = cc_string_copy(table_name),
			.file_path = table_file_path,

			.stride = 0,
			.attribute_indices = cc_hash_map_create(sizeof(uint64_t), table_data.attrib_count_c),
			.attributes = malloc(sizeof(_CD_Attribute) * table_data.attrib_count_c),

			.attribute_count_view = table_data_view,
			.attributes_view = table_attributes_view,

			.data_file = table_file,
			.data_count_view = data_count_view,
			.data_view = NULL
		};

		// for load attributes
		for (uint64_t attrib_index = 0; attrib_index < table_data.attrib_count_c; attrib_index++)
		{
			CD_Attribute attribute;

			if (!cf_file_view_read(table_attributes_view, attrib_index * sizeof(attribute), sizeof(attribute), &attribute))
			{
				_cd_make_error(CD_ERROR_FILE, "Failed to read attribute_data of schema at attribute_index %d and at table_index %d for file '%s'", attrib_index, table_index, schema_file_path.data);
				_error = 1;
				goto table_destroy;
			}

			CC_String attribute_name =
			{
				.data = attribute.name,
				.length = strlen(attribute.name)
			};

			_CD_Attribute _attribute =
			{
				.count = attribute.count,
				.offset = table.stride,
				.type = attribute.type
			};
			strcpy(_attribute.name, attribute.name);

			table.attributes[attrib_index] = _attribute;

			table.stride += attribute.count * cd_attribute_type_size(attribute.type);

			cc_hash_map_insert(table.attribute_indices, attribute_name, &attrib_index);
		}

		CF_FileView *data_view = cf_file_view_open(table_file, sizeof(table_data_count), table_data_count.count_m * table.stride);
		if (data_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open data view at index %d for file '%s'", table_index, table_file_path.data);
			_error = 1;
			goto data_count_view_close;
		}

		table.data_view = data_view;

		cc_hash_map_insert(tables, table.name, &table);

		table_offset += sizeof(table_data) + table_data.attrib_count_m * sizeof(CD_Attribute);

	data_count_view_close:
		if (_error)
		{
			cf_file_view_close(data_count_view);
		}
	table_file_close:
		if (_error)
		{
			cf_file_close(table_file);
		}
	table_file_path_destroy:
		if (_error)
		{
			cc_string_destroy(table_file_path);
		}
	table_destroy:
		if (_error)
		{
			cc_hash_map_destroy(table.attribute_indices);
		}
	table_data_view_close:
		if (_error)
		{
			cf_file_view_close(table_data_view);
		}
		if (_error)
		{
			goto schema_count_view_close;
		}
	}

	// create database and return
	db = malloc(sizeof(*db));

	db->name = db_name;
	db->schema_file_path = schema_file_path;

	db->schema_file = schema_file;
	db->table_count_view = schema_count_view;
	db->tables = tables;

schema_count_view_close:
	if (db == NULL)
	{
		cf_file_view_close(schema_count_view);
	}
schema_file_close:
	if (db == NULL)
	{
		cf_file_close(schema_file);
	}
schema_file_path_destroy:
	if (db == NULL)
	{
		cc_string_destroy(schema_file_path);
	}
db_name_destroy:
	if (db == NULL)
	{
		cc_string_destroy(db_name);
	}

	return db;
}

void cd_database_close(CD_Database *db)
{
	// close files and file views
	for (CC_HashMap_Element *element = cc_hash_map_iterator_begin(db->tables); element != cc_hash_map_iterator_end(db->tables); element = cc_hash_map_iterator_next(db->tables, element))
	{
		CD_Table *table = (CD_Table *)element->data;

		cc_string_destroy(table->name);
		cc_hash_map_destroy(table->attribute_indices);
		free(table->attributes);

		cf_file_view_close(table->attribute_count_view);
		cf_file_view_close(table->attributes_view);

		cf_file_view_close(table->data_count_view);
		cf_file_view_close(table->data_view);
		cf_file_close(table->data_file);
	}

	// free db
	cc_string_destroy(db->name);
	cc_string_destroy(db->schema_file_path);
	cc_hash_map_destroy(db->tables);

	cf_file_view_close(db->table_count_view);
	cf_file_close(db->schema_file);

	free(db);
}

uint64_t cd_table_create(CD_Database *db, CC_String _table_name, uint64_t attribute_count, CD_Attribute *attributes)
{
	CD_Table table =
	{
		.name = cc_string_copy(_table_name),
		.file_path = {
			.data = NULL,
			.length = 0
		},

		.stride = 0,
		.attribute_indices = cc_hash_map_create(sizeof(uint64_t), attribute_count),
		.attributes = malloc(sizeof(_CD_Attribute) * attribute_count),

		.attribute_count_view = NULL,
		.attributes_view = NULL,

		.data_file = NULL,
		.data_count_view = NULL,
		.data_view = NULL,
	};

	// table.file_path
	{
		CC_StringBuffer *buffer = cc_string_buffer_create(256);
		cc_string_buffer_insert_string(buffer, db->name);
		cc_string_buffer_insert_char(buffer, '/');
		cc_string_buffer_insert_string(buffer, table.name);
		CC_String file_extension = cc_string_create(".table", 0);
		cc_string_buffer_insert_string(buffer, file_extension);
		cc_string_destroy(file_extension);

		table.file_path = cc_string_buffer_to_string_and_destroy(buffer);
	}

	// if table exists
	if (cc_hash_map_lookup(db->tables, table.name) != NULL)
	{
		_cd_make_error(CD_ERROR_TABLE_EXISTS, "Table %s already exists. Skipping!", table.name.data);
		goto table_destroy_1;
	}

	// insert attributes into hash map and 
	for (uint64_t i = 0; i < attribute_count; i++)
	{
		CD_Attribute *attrib = attributes + i;

		CC_String attribute_name =
		{
			.data = attrib->name,
			.length = strlen(attrib->name)
		};

		_CD_Attribute _attrib =
		{
			.count = attrib->count,
			.offset = table.stride,
			.type = attrib->type
		};

		strcpy(_attrib.name, attrib->name);

		table.attributes[i] = _attrib;

		cc_hash_map_insert(table.attribute_indices, attribute_name, &i);

		table.stride += cd_attribute_size(attrib->type, attrib->count);
	}

	// increase count in schema file
	{
		uint64_t table_count = cc_hash_map_count(db->tables) + 1;
		if (!cf_file_view_write(db->table_count_view, 0, sizeof(table_count), &table_count))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to write table count to file '%s'", db->schema_file_path.data);
			goto table_destroy_1;
		}
	}

	// increase size of schema file and write data
	{
		_CD_TableSchemaData table_schema_data =
		{
			.attrib_count_c = attribute_count,
			.attrib_count_m = attribute_count
		};
		strcpy_s(table_schema_data.name, sizeof(table_schema_data.name), table.name.data);


		// resize file
		uint64_t file_size_c = cf_file_size_get(db->schema_file);
		if (!cf_file_resize(db->schema_file, file_size_c + sizeof(_CD_TableSchemaData) + attribute_count * sizeof(CD_Attribute)))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to resize file '%s' and table '%s'", db->schema_file_path.data, table.name);
			goto table_destroy_1;
		}

		// open attrib count view
		table.attribute_count_view = cf_file_view_open(db->schema_file, file_size_c, sizeof(_CD_TableSchemaData));
		if(table.attribute_count_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open attribute count view for file '%s' and table '%s'", db->schema_file_path.data, table.name);
			goto table_destroy_1;
		}

		// open attributes view
		table.attributes_view = cf_file_view_open(db->schema_file, file_size_c + sizeof(_CD_TableSchemaData), attribute_count * sizeof(CD_Attribute));
		if (table.attribute_count_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open attributes view for file '%s' and table '%s'", db->schema_file_path.data, table.name);
			goto attribute_count_view_close;
		}

		// write attrib count
		if (!cf_file_view_write(table.attribute_count_view, 0, sizeof(table_schema_data), &table_schema_data))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to write attribute count for file '%s' and table '%s'", db->schema_file_path.data, table.name);
			goto attributes_view_close;
		}

		// write attributes
		if (!cf_file_view_write(table.attributes_view, 0, attribute_count * sizeof(CD_Attribute), attributes))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to write attributes for file '%s' and table '%s'", db->schema_file_path.data, table.name);
			goto attributes_view_close;
		}
	}

	// table file
	{
		_CD_TableCount table_count =
		{
			.count_c = 0,
			.count_m = 32
		};

		// create table file
		if (!cf_file_create(table.file_path, sizeof(_CD_TableCount) + table.stride * table_count.count_m))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to create '%s' file.", table.file_path.data);
			goto attributes_view_close;
		}

		// open data file
		table.data_file = cf_file_open(table.file_path);
		if (table.data_file == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open '%s' file.", table.file_path.data);
			goto attributes_view_close;
		}

		// open count view
		table.data_count_view = cf_file_view_open(table.data_file, 0, sizeof(table_count));
		if (table.data_count_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open data count view of file '%s'.", table.file_path.data);
			goto data_file_close;
		}

		// open data view
		table.data_view = cf_file_view_open(table.data_file, sizeof(table_count), table.stride * table_count.count_m);
		if (table.data_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to open data view of file '%s'.", table.file_path.data);
			goto data_count_view_close;
		}

		// write count to view
		if (!cf_file_view_write(table.data_count_view, 0, sizeof(table_count), &table_count))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to write data count to file '%s'.", table.file_path.data);
			goto data_view_close;
		}

	}

	cc_hash_map_insert(db->tables, table.name, &table);

	return 1;

data_view_close:
	cf_file_view_close(table.data_view);
data_count_view_close:
	cf_file_view_close(table.data_count_view);
data_file_close:
	cf_file_close(table.data_file);
attributes_view_close:
	cf_file_view_close(table.attributes_view);
attribute_count_view_close:
	cf_file_view_close(table.attribute_count_view);
table_destroy_1:
	cc_string_destroy(table.file_path);
	free(table.attributes);
	cc_hash_map_destroy(table.attribute_indices);
	cc_string_destroy(table.name);

	return 0;
}

uint64_t cd_table_insert(CD_Database *db, CC_String _table_name, uint64_t attribute_count, char *attribute_names[256], const void *data)
{
	uint64_t return_value = 0;

	CC_String table_name = cc_string_copy(_table_name);

	// get table
	CD_Table *table = (CD_Table *)cc_hash_map_lookup(db->tables, table_name);
	if (table == NULL)
	{
		_cd_make_error(CD_ERROR_TABLE_DOES_NOT_EXIST, "Table '%s' does not exist.", table_name.data);
		goto table_name_destroy;
	}

	_CD_TableCount table_count;
	if (!cf_file_view_read(table->data_count_view, 0, sizeof(table_count), &table_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to read data count of table '%s'", table_name.data);
		goto table_name_destroy;
	}

	struct
	{
		uint64_t data_offset;
		uint64_t file_offset;
		uint64_t size;
	} *attribute_data = malloc(sizeof(attribute_data[0]) * attribute_count);

	uint64_t data_stride = 0;

	for (uint64_t i = 0; i < attribute_count; i++)
	{
		CC_String attrib_name =
		{
			.data = attribute_names[i],
			.length = strlen(attribute_names[i])
		};

		uint64_t *index_ptr = (uint64_t *)cc_hash_map_lookup(table->attribute_indices, attrib_name);

		if (index_ptr == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[i], table->name.data);
			goto attribute_data_free;
		}

		uint64_t index = *index_ptr;

		_CD_Attribute *attribute = table->attributes + index;

		attribute_data[i].data_offset = data_stride;
		attribute_data[i].file_offset = attribute->offset;
		attribute_data[i].size = cd_attribute_size(attribute->type, attribute->count);

		data_stride += attribute_data[i].size;
	}

	// check if primary key already exists
	{
		// find primary key in attribute_names
		uint64_t primary_key_index = attribute_count;
		printf("primary_key = %s\n", table->attributes[0].name);
		for (uint64_t i = 0; i < attribute_count; i++)
		{
			if (strcmp(attribute_names[i], table->attributes[0].name) == 0)
			{
				primary_key_index = i;
				break;
			}
		}
		if (primary_key_index == attribute_count)
		{
			_cd_make_error(CD_ERROR_PRIMARY_KEY_NOT_SPECIFIED, "A primary key needs to have a value when inserting a record. table: '%s'", table->name.data);
			goto attribute_data_free;
		}

		uint64_t _error = 0;

		void *primary_key_data = malloc(attribute_data[primary_key_index].size);

		for (uint64_t i = 0; i < table_count.count_c; i++)
		{
			if (!cf_file_view_read(table->data_view, attribute_data[primary_key_index].file_offset + i * table->stride, attribute_data[primary_key_index].size, primary_key_data))
			{
				_cd_make_error(CD_ERROR_FILE, "Failed to read primary key at index %llu from table '%s'", i, table->name.data);
				_error = 1;
				goto primary_key_data_free;
			}

			if (memcmp((uint8_t *)data + attribute_data[primary_key_index].data_offset, primary_key_data, attribute_data[primary_key_index].size) == 0)
			{
				_cd_make_error(CD_ERROR_PRIMARY_KEY_IS_UNIQUE, "Primary key value is already in the table '%s' at index %llu", table->name.data, i);
				_error = 1;
				goto primary_key_data_free;
			}
		}

	primary_key_data_free:
		free(primary_key_data);
		if (_error)
		{
			goto attribute_data_free;
		}
	}

	if (table_count.count_c == table_count.count_m)
	{
		// increase size
		table_count.count_m += 32;

		if (!cf_file_resize(table->data_file, cf_file_size_get(table->data_file) + 32 * table->stride))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to resize data file '%s'.", table->file_path);
			goto attribute_data_free;
		}

		cf_file_view_close(table->data_view);
		
		table->data_view = cf_file_view_open(table->data_file, sizeof(table_count), table_count.count_m * table->stride);
		if (table->data_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to reopen data view of file '%s'.", table->file_path);
			goto attribute_data_free;
		}
	}

	uint8_t *file_data = malloc(table->stride);
	memset(file_data, 0, table->stride);

	for (uint64_t i = 0; i < attribute_count; i++)
	{
		memcpy(file_data + attribute_data[i].file_offset, (uint8_t *)data + attribute_data[i].data_offset, attribute_data[i].size);
	}

	if (!cf_file_view_write(table->data_view, table_count.count_c * table->stride, table->stride, file_data))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write record at index %llu for table %s", table_count.count_c, table->name);
		goto file_data_free;
	}

	table_count.count_c++;
	if (!cf_file_view_write(table->data_count_view, 0, sizeof(table_count), &table_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write count_c at index %llu for table %s", table_count.count_c - 1, table->name);
		goto file_data_free;
	}

	return_value = 1;

file_data_free:
	free(file_data);
attribute_data_free:
	free(attribute_data);
table_name_destroy:
	cc_string_destroy(table_name);

	return return_value;
}

CD_TableView *cd_table_select(CD_Database *db, CC_String table_name, uint64_t attribute_count, const char *attribute_names[256], const char comparison_name[256], const void *comparison_data)
{
	// get table
	CD_Table *table = (CD_Table *)cc_hash_map_lookup(db->tables, table_name);
	if (table == NULL)
	{
		_cd_make_error(CD_ERROR_TABLE_DOES_NOT_EXIST, "Table '%s' does not exist.", table_name.data);
		goto _return;
	}

	_CD_TableCount table_count;
	if (!cf_file_view_read(table->data_count_view, 0, sizeof(table_count), &table_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to read data count of table '%s'", table_name.data);
		goto _return;
	}

	struct
	{
		uint64_t data_offset;
		uint64_t file_offset;
		uint64_t size;
	} *attribute_data = malloc(sizeof(attribute_data[0]) * attribute_count);


	CD_TableView *table_view = malloc(sizeof(*table_view));

	table_view->count_c = 0;
	table_view->count_m = 32;
	table_view->stride = 0;
	table_view->attributes = malloc(sizeof(table_view->attributes[0]) * attribute_count);

	for (uint64_t i = 0; i < attribute_count; i++)
	{
		CC_String attrib_name =
		{
			.data = attribute_names[i],
			.length = strlen(attribute_names[i])
		};

		uint64_t *index_ptr = (uint64_t *)cc_hash_map_lookup(table->attribute_indices, attrib_name);

		if (index_ptr == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[i], table->name.data);
			goto table_view_attributes_free;
		}

		uint64_t index = *index_ptr;

		_CD_Attribute *attribute = table->attributes + index;

		attribute_data[i].data_offset = table_view->stride;
		attribute_data[i].file_offset = attribute->offset;
		attribute_data[i].size = cd_attribute_size(attribute->type, attribute->count);

		table_view->attributes[i].count = attribute->count;
		table_view->attributes[i].type = attribute->type;
		strcpy(table_view->attributes[i].name, attribute->name);

		table_view->stride += attribute_data[i].size;
	}

	table_view->data = malloc(table_view->count_m * table_view->stride);

	// find comparison attribute
	_CD_Attribute *comparison_attribute;
	{
		CC_String comparison_attribute_name =
		{
			.data = comparison_name,
			.length = strlen(comparison_name)
		};
		uint64_t *comparison_attribute_index = cc_hash_map_lookup(table->attribute_indices, comparison_attribute_name);
		if (comparison_attribute_index == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", comparison_name, table->name.data);
			goto table_view_data_free;
		}
		comparison_attribute = table->attributes + *comparison_attribute_index;
	}

	uint64_t comparison_attribute_size = cd_attribute_size(comparison_attribute->type, comparison_attribute->count);
	void *comparison_attribute_data = malloc(comparison_attribute_size);

	for (uint64_t row = 0; row < table_count.count_c; row++)
	{
		if (!cf_file_view_read(table->data_view, row * table->stride + comparison_attribute->offset, comparison_attribute_size, comparison_attribute_data))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to read comparison attribute '%s' at row %llu from table '%s'", comparison_attribute->name, row, table->name.data);
			goto comparison_attribute_data_free;
		}

		if (memcmp((uint8_t *)comparison_data, comparison_attribute_data, comparison_attribute_size) == 0)
		{
			if(table_view->count_c == table_view->count_m)
			{
				table_view->count_m += 32;
				table_view->data = realloc(table_view->data, table_view->count_m * table_view->stride);
			}

			for (uint64_t i = 0; i < attribute_count; i++)
			{
				if (!cf_file_view_read(table->data_view, row * table->stride + attribute_data[i].file_offset, attribute_data[i].size, (uint8_t *)table_view->data + table_view->count_c * table_view->stride + attribute_data[i].data_offset))
				{
					_cd_make_error(CD_ERROR_FILE, "Failed to read attribute '%s' at row %llu from table '%s'", table_view->attributes[i].name, row, table->name.data);
					goto comparison_attribute_data_free;
				}
			}

			table_view->count_c++;
		}
	}

	free(comparison_attribute_data);

	free(attribute_data);

	return table_view;

comparison_attribute_data_free:
	free(comparison_attribute_data);
table_view_data_free:
	free(table_view->data);
table_view_attributes_free:
	free(table_view->attributes);
table_view_free:
	free(table_view);
attribute_data_free:
	free(attribute_data);
_return:
	return NULL;
}

uint64_t cd_attribute_type_size(CD_AttributeType type)
{
	switch (type)
	{
	case CD_ATTRIBUTE_UINT_8:
	case CD_ATTRIBUTE_SINT_8:
	case CD_ATTRIBUTE_CHAR:
	case CD_ATTRIBUTE_VARCHAR:
		return 1;
	case CD_ATTRIBUTE_UINT_16:
	case CD_ATTRIBUTE_SINT_16:
		return 2;
	case CD_ATTRIBUTE_UINT_32:
	case CD_ATTRIBUTE_SINT_32:
		return 4;
	case CD_ATTRIBUTE_UINT_64:
	case CD_ATTRIBUTE_SINT_64:
	case CD_ATTRIBUTE_FLOAT:
		return 8;
	default:
		printf("ERROR invalid type.\n");
		return 0;
	}
}

uint64_t cd_attribute_size(CD_AttributeType type, uint64_t count)
{
	return cd_attribute_type_size(type) * count;
}
