#include "internal.h"

uint64_t cd_table_exists(CD_Database *db, const char *_table_name)
{
	CC_String table_name = cc_string_create(_table_name, 0);
	const CD_TableSchema *schema = cc_hash_map_lookup(db->table_schemas, table_name);
	cc_string_destroy(table_name);
	return schema != NULL;
}

uint64_t cd_table_create(CD_Database *db, const char *_table_name, uint64_t attribute_count, CD_Attribute attributes[])
{
	CC_String table_name = cc_string_create(_table_name, 0);

	if (cc_hash_map_lookup(db->table_schemas, table_name) != NULL)
	{
		_cd_make_error(CD_ERROR_TABLE_EXISTS, "Table %s already exists. Skipping!", _table_name);
		goto table_name_destroy;
	}

	CC_String file_path;
	{
		CC_StringBuffer *buffer = cc_string_buffer_create(256);
		cc_string_buffer_insert_string(buffer, db->name);
		cc_string_buffer_insert_char(buffer, '/');
		cc_string_buffer_insert_string(buffer, table_name);
		CC_String file_extension = cc_string_create(".table", 0);
		cc_string_buffer_insert_string(buffer, file_extension);
		cc_string_destroy(file_extension);

		file_path = cc_string_buffer_to_string_and_destroy(buffer);
	}

	// resize schema file
	uint64_t file_size = cf_file_size_get(db->schema_file);
	uint64_t extra_size = sizeof(_CD_File_TableSchema) + attribute_count * sizeof(_CD_File_Attribute);
	if (!cf_file_resize(db->schema_file, file_size + extra_size))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to resize schema file when creating table '%s'.", _table_name);
		goto file_path_destroy;
	}

	// open schema view
	CF_FileView *schema_view = cf_file_view_open(db->schema_file, file_size, extra_size);
	if (schema_view == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open schema view of table '%s'", _table_name);
		goto file_path_destroy;
	}

	// insert schema into file
	_CD_File_TableSchema table_schema_data =
		{
			.attrib_count_c = attribute_count,
			.attrib_count_m = attribute_count};
	memset(table_schema_data.name, 0, CD_NAME_LENGTH);
	strcpy_s(table_schema_data.name, CD_NAME_LENGTH, _table_name);

	if (!cf_file_view_write(schema_view, 0, sizeof(table_schema_data), &table_schema_data))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write schema data of table '%s'", _table_name);
		goto schema_view_close;
	}

	_CD_File_Attribute *file_attributes = malloc(sizeof(_CD_File_Attribute) * attribute_count);

	for (uint64_t attrib_index = 0; attrib_index < attribute_count; attrib_index++)
	{
		file_attributes[attrib_index].type = attributes[attrib_index].type;
		file_attributes[attrib_index].count = attributes[attrib_index].count;
		file_attributes[attrib_index].constraints = attributes[attrib_index].constraints;
		memset(file_attributes[attrib_index].name, 0, CD_NAME_LENGTH);
		strcpy_s(file_attributes[attrib_index].name, CD_NAME_LENGTH, attributes[attrib_index].name);
	}

	if (!cf_file_view_write(schema_view, sizeof(table_schema_data), sizeof(_CD_File_Attribute) * attribute_count, file_attributes))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write schema attributes of table '%s'", _table_name);
		goto file_attributes_free;
	}

	free(file_attributes);

	// increase count of tables in file
	uint64_t table_count = cc_hash_map_count(db->table_schemas) + 1;
	if (!cf_file_view_write(db->schema_count_view, 0, sizeof(uint64_t), &table_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write table count of database '%s'", db->name.data);
		goto schema_view_close;
	}

	CD_TableSchema schema =
		{
			.attribute_indices = cc_hash_map_create(sizeof(uint64_t), attribute_count),
			.attributes = malloc(sizeof(CD_AttributeEx) * attribute_count),
			.stride = 0};

	for (uint64_t attrib_index = 0; attrib_index < attribute_count; attrib_index++)
	{
		CD_AttributeEx *attribute = schema.attributes + attrib_index;
		CD_Attribute *in_attribute = attributes + attrib_index;

		// insert attrib index into hash map
		{
			CC_String attribute_name = cc_string_create(in_attribute->name, 0);
			cc_hash_map_insert(schema.attribute_indices, attribute_name, &attrib_index);
			cc_string_destroy(attribute_name);
		}

		memset(attribute->name, 0, CD_NAME_LENGTH);
		strcpy_s(attribute->name, CD_NAME_LENGTH, in_attribute->name);
		attribute->type = in_attribute->type;
		attribute->count = in_attribute->count;
		attribute->constraints = in_attribute->constraints;
		attribute->offset = schema.stride;
		attribute->size = cd_attribute_size(in_attribute->type, in_attribute->count);

		schema.stride += attribute->size;
	}

	cc_hash_map_insert(db->table_schemas, table_name, &schema);

	// create table file
	_CD_File_RowCount row_count =
		{
			.count_c = 0,
			.count_m = CD_ROW_COUNT_START};

	if (!cf_file_create(file_path, sizeof(_CD_File_RowCount) + schema.stride * CD_ROW_COUNT_START))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to create file '%s'", file_path.data);
		goto schema_destroy;
	}

	CF_File *table_file = cf_file_open(file_path);
	if (table_file == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open file '%s'", file_path.data);
		goto schema_destroy;
	}

	CF_FileView *table_count_view = cf_file_view_open(table_file, 0, sizeof(row_count));
	if (table_count_view == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open file count view of file '%s'", file_path.data);
		goto table_file_close;
	}

	if (!cf_file_view_write(table_count_view, 0, sizeof(row_count), &row_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write counts to file count view of file '%s'", file_path.data);
		goto table_file_close;
	}

	cf_file_view_close(table_count_view);
	cf_file_close(table_file);
	cf_file_view_close(schema_view);
	cc_string_destroy(file_path);
	cc_string_destroy(table_name);

	return 1;

	// table_count_view_close:
	cf_file_view_close(table_count_view);
table_file_close:
	cf_file_close(table_file);
schema_destroy:
	free(schema.attributes);
	cc_hash_map_destroy(schema.attribute_indices);
file_attributes_free:
	free(file_attributes);
schema_view_close:
	cf_file_view_close(schema_view);
file_path_destroy:
	cc_string_destroy(file_path);
table_name_destroy:
	cc_string_destroy(table_name);
	return 0;
}

CD_Table *cd_table_open(CD_Database *db, const char *_table_name)
{
	CC_String table_name = cc_string_create(_table_name, 0);

	const CD_TableSchema *schema = cc_hash_map_lookup(db->table_schemas, table_name);
	if (schema == NULL)
	{
		_cd_make_error(CD_ERROR_TABLE_DOES_NOT_EXIST, "Table '%s' does not exist.", _table_name);
		goto table_name_destroy;
	}

	CC_String file_path;
	{
		CC_StringBuffer *buffer = cc_string_buffer_create(256);
		cc_string_buffer_insert_string(buffer, db->name);
		cc_string_buffer_insert_char(buffer, '/');
		cc_string_buffer_insert_string(buffer, table_name);
		CC_String file_extension = cc_string_create(".table", 0);
		cc_string_buffer_insert_string(buffer, file_extension);
		cc_string_destroy(file_extension);

		file_path = cc_string_buffer_to_string_and_destroy(buffer);
	}

	CF_File *file = cf_file_open(file_path);
	if (file == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open file '%s'", file_path.data);
		goto file_path_destroy;
	}

	_CD_File_RowCount row_count;
	CF_FileView *count_view = cf_file_view_open(file, 0, sizeof(row_count));
	if (count_view == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open row count file view of file '%s'", file_path.data);
		goto file_close;
	}

	if (!cf_file_view_read(count_view, 0, sizeof(row_count), &row_count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to read row count from file '%s'", file_path.data);
		goto count_view_close;
	}

	CF_FileView *data_view = cf_file_view_open(file, sizeof(row_count), row_count.count_m * schema->stride);
	if (data_view == NULL)
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to open row count file view of file '%s'", file_path.data);
		goto count_view_close;
	}

	CD_Table *table = malloc(sizeof(*table));

	table->name = table_name;
	table->file_path = file_path;

	table->count = row_count;
	table->schema = schema;

	table->file = file;
	table->count_view = count_view;
	table->data_view = data_view;

	return table;

	// data_view_close:
	cf_file_view_close(data_view);
count_view_close:
	cf_file_view_close(count_view);
file_close:
	cf_file_close(file);
file_path_destroy:
	cc_string_destroy(file_path);
table_name_destroy:
	cc_string_destroy(table_name);

	return NULL;
}

void cd_table_close(CD_Table *table)
{
	cf_file_view_close(table->data_view);
	cf_file_view_close(table->count_view);
	cf_file_close(table->file);

	cc_string_destroy(table->file_path);
	cc_string_destroy(table->name);
}

const CD_AttributeEx *cd_table_attribute_by_name(CD_Table *table, const char *attrib_name)
{
	CC_String cc_attrib_name = cc_string_create(attrib_name, 0);

	const uint64_t *index_ptr = cc_hash_map_lookup(table->schema->attribute_indices, cc_attrib_name);
	if (index_ptr == NULL)
	{
		_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attrib_name, table->name.data);
		cc_string_destroy(cc_attrib_name);
		return NULL;
	}

	cc_string_destroy(cc_attrib_name);

	return table->schema->attributes + *index_ptr;
}

const CD_AttributeEx *cd_table_attribute_by_index(CD_Table *table, uint64_t index)
{
	if (index < cc_hash_map_count(table->schema->attribute_indices))
	{
		return table->schema->attributes + index;
	}
	else
	{
		return NULL;
	}
}

uint64_t cd_table_stride(CD_Table *table)
{
	return table->schema->stride;
}

uint64_t cd_table_count(CD_Table *table)
{
	return table->count.count_c;
}

uint64_t cd_table_insert(CD_Table *table, uint64_t attribute_count, const char *attribute_names[], const void *data)
{
	uint64_t return_value = 0;

	CC_String table_name = cc_string_copy(table->name);

	struct
	{
		uint64_t data_offset;
		uint64_t file_offset;
		uint64_t size;
	} *attribute_data = malloc(sizeof(attribute_data[0]) * attribute_count);

	uint64_t data_stride = 0;

	for (uint64_t i = 0; i < attribute_count; i++)
	{
		CC_String attrib_name = cc_string_create(attribute_names[i], 0);
		const uint64_t *index_ptr = (const uint64_t *)cc_hash_map_lookup(table->schema->attribute_indices, attrib_name);
		cc_string_destroy(attrib_name);

		if (index_ptr == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[i], table->name.data);
			goto attribute_data_free;
		}

		uint64_t index = *index_ptr;

		CD_AttributeEx *attribute = table->schema->attributes + index;

		attribute_data[i].data_offset = data_stride;
		attribute_data[i].file_offset = attribute->offset;
		attribute_data[i].size = attribute->size;

		data_stride += attribute_data[i].size;
	}

	// check constrains
	for (uint64_t table_attrib_index = 0; table_attrib_index < cc_hash_map_count(table->schema->attribute_indices); table_attrib_index++)
	{
		CD_AttributeEx *attrib = table->schema->attributes + table_attrib_index;

		// check not null
		uint64_t not_null = attrib->constraints & CD_CONSTRAINT_NOT_NULL;
		if (not_null)
		{
			uint64_t attrib_index = attribute_count;
			for (uint64_t i = 0; i < attribute_count; i++)
			{
				if (strcmp(attribute_names[i], table->schema->attributes[table_attrib_index].name) == 0)
				{
					attrib_index = i;
					break;
				}
			}
			if (attrib_index == attribute_count)
			{
				_cd_make_error(CD_ERROR_ATTRIBUTE_IS_NOT_NULL, "Attribute '%s' is NOT NULL and so needs to have a value when inserting a record. table: '%s'", table->schema->attributes[table_attrib_index].name, table->name.data);
				goto attribute_data_free;
			}
		}
	}

	// check unique
	for (uint64_t attrib_index = 0; attrib_index < attribute_count; attrib_index++)
	{
		CC_String attrib_name = cc_string_create(attribute_names[attrib_index], 0);
		const uint64_t *table_attrib_index = cc_hash_map_lookup(table->schema->attribute_indices, attrib_name);
		cc_string_destroy(attrib_name);

		if (table_attrib_index == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[attrib_index], table->name.data);
			goto attribute_data_free;
		}

		CD_AttributeEx *attrib = table->schema->attributes + (*table_attrib_index);
		uint64_t unique = attrib->constraints & CD_CONSTRAINT_UNIQUE;

		if (unique)
		{
			uint64_t _error = 0;

			void *buffer = malloc(attribute_data[attrib_index].size);

			for (uint64_t row = 0; row < table->count.count_c; row++)
			{
				if (!cf_file_view_read(table->data_view, attribute_data[attrib_index].file_offset + row * table->schema->stride, attribute_data[attrib_index].size, buffer))
				{
					_cd_make_error(CD_ERROR_FILE, "Failed to read primary key at row %llu from table '%s'", row, table->name.data);
					_error = 1;
					goto buffer_free;
				}

				if (memcmp((uint8_t *)data + attribute_data[attrib_index].data_offset, buffer, attribute_data[attrib_index].size) == 0)
				{
					_cd_make_error(CD_ERROR_ATTRIBUTE_IS_UNIQUE, "Attribute '%s' is UNIQUE and is already in the table '%s' at row %llu", attribute_names[attrib_index], table->name.data, row);
					_error = 1;
					goto buffer_free;
				}
			}

		buffer_free:
			free(buffer);
			if (_error)
			{
				goto attribute_data_free;
			}
		}
	}

	if (table->count.count_c == table->count.count_m)
	{
		// increase size
		table->count.count_m += 32;

		if (!cf_file_resize(table->file, cf_file_size_get(table->file) + 32 * table->schema->stride))
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to resize data file '%s'.", table->file_path);
			goto attribute_data_free;
		}

		cf_file_view_close(table->data_view);

		table->data_view = cf_file_view_open(table->file, sizeof(table->count), table->count.count_m * table->schema->stride);
		if (table->data_view == NULL)
		{
			_cd_make_error(CD_ERROR_FILE, "Failed to reopen data view of file '%s'.", table->file_path);
			goto attribute_data_free;
		}
	}

	uint8_t *file_data = malloc(table->schema->stride);
	memset(file_data, 0, table->schema->stride);

	for (uint64_t i = 0; i < attribute_count; i++)
	{
		memcpy(file_data + attribute_data[i].file_offset, (uint8_t *)data + attribute_data[i].data_offset, attribute_data[i].size);
	}

	if (!cf_file_view_write(table->data_view, table->count.count_c * table->schema->stride, table->schema->stride, file_data))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write record at index %llu for table %s", table->count.count_c, table->name);
		goto file_data_free;
	}

	table->count.count_c++;
	if (!cf_file_view_write(table->count_view, 0, sizeof(table->count), &table->count))
	{
		_cd_make_error(CD_ERROR_FILE, "Failed to write count_c at index %llu for table %s", table->count.count_c - 1, table->name);
		goto file_data_free;
	}

	return_value = 1;

file_data_free:
	free(file_data);
attribute_data_free:
	free(attribute_data);
	// table_name_destroy:
	cc_string_destroy(table_name);

	return return_value;
}

CD_TableView *cd_table_select(CD_Table *table, uint64_t attribute_count, const char *attribute_names[], uint64_t condition_count, CD_Condition *conditions)
{
	struct
	{
		uint64_t data_offset;
		uint64_t file_offset;
		uint64_t size;
	} *attribute_data = malloc(sizeof(attribute_data[0]) * attribute_count);

	CD_TableView *table_view = cd_table_view_create(table, attribute_count, attribute_names);

	uint64_t offset = 0;

	for (uint64_t i = 0; i < attribute_count; i++)
	{
		CC_String attrib_name = cc_string_create(attribute_names[i], 0);
		const uint64_t *index_ptr = (const uint64_t *)cc_hash_map_lookup(table->schema->attribute_indices, attrib_name);
		cc_string_destroy(attrib_name);

		if (index_ptr == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[i], table->name.data);
			goto table_view_attributes_free;
		}

		uint64_t index = *index_ptr;

		CD_AttributeEx *attribute = table->schema->attributes + index;

		attribute_data[i].data_offset = offset;
		attribute_data[i].file_offset = attribute->offset;
		attribute_data[i].size = cd_attribute_size(attribute->type, attribute->count);

		offset += attribute_data[i].size;
	}

	uint8_t *should_add_row = malloc(sizeof(*should_add_row) * table->count.count_c);
	for (uint64_t row = 0; row < table->count.count_c; row++)
	{
		should_add_row[row] = 1;
	}

	if (conditions != NULL)
	{
		// find comparison attribute
		for (uint64_t condition_index = 0; condition_index < condition_count; condition_index++)
		{
			CD_Condition *condition = conditions + condition_index;

			CC_String condition_name = cc_string_create(condition->name, 0);
			const uint64_t *condition_attribute_index_ptr = (const uint64_t *)cc_hash_map_lookup(table->schema->attribute_indices, condition_name);
			cc_string_destroy(condition_name);

			if (condition_attribute_index_ptr == NULL)
			{
				_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", condition->name, table->name.data);
				goto table_view_data_free;
			}

			uint64_t _error = 0;

			uint64_t condition_attribute_index = *condition_attribute_index_ptr;

			CD_AttributeEx *condition_attribute = table->schema->attributes + condition_attribute_index;

			uint64_t condition_attribute_size = cd_attribute_size(condition_attribute->type, condition_attribute->count);
			void *condition_attribute_buffer = malloc(condition_attribute_size);

			switch (condition->operator)
			{
			case CD_CONDITION_OPERATOR_EQUALS:
			{
				for (uint64_t row = 0; row < table->count.count_c; row++)
				{
					if (!should_add_row[row])
						continue;

					if (!cf_file_view_read(table->data_view, row * table->schema->stride + condition_attribute->offset, condition_attribute_size, condition_attribute_buffer))
					{
						_cd_make_error(CD_ERROR_FILE, "Failed to read condition attribute '%s' at row %llu from table '%s'", condition_attribute->name, row, table->name.data);
						_error = 1;
						goto condition_attribute_buffer_free;
					}

					if (!_cd_funcs_equal[condition_attribute->type](condition_attribute_buffer, condition->data, condition_attribute->count))
					{
						should_add_row[row] = 0;
					}
				}
				break;
			}
			case CD_CONDITION_OPERATOR_DIFFERENT:
			{
				for (uint64_t row = 0; row < table->count.count_c; row++)
				{
					if (!should_add_row[row])
						continue;

					if (!cf_file_view_read(table->data_view, row * table->schema->stride + condition_attribute->offset, condition_attribute_size, condition_attribute_buffer))
					{
						_cd_make_error(CD_ERROR_FILE, "Failed to read condition attribute '%s' at row %llu from table '%s'", condition_attribute->name, row, table->name.data);
						_error = 1;
						goto condition_attribute_buffer_free;
					}

					if (_cd_funcs_equal[condition_attribute->type](condition_attribute_buffer, condition->data, condition_attribute->count))
					{
						should_add_row[row] = 0;
					}
				}
				break;
			}
			case CD_CONDITION_OPERATOR_BIGGER:
			{
				_cd_make_error(CD_ERROR_UNKNOWN_OPERATOR, "Operator %llu is not implemented yet. table: '%s'", condition->operator, table->name.data);
				_error = 1;
				goto condition_attribute_buffer_free;
			}
			case CD_CONDITION_OPERATOR_SMALLER:
			{
				_cd_make_error(CD_ERROR_UNKNOWN_OPERATOR, "Operator %llu is not implemented yet. table: '%s'", condition->operator, table->name.data);
				_error = 1;
				goto condition_attribute_buffer_free;
			}
			case CD_CONDITION_OPERATOR_CONTAINS:
			{
				_cd_make_error(CD_ERROR_UNKNOWN_OPERATOR, "Operator %llu is not implemented yet. table: '%s'", condition->operator, table->name.data);
				_error = 1;
				goto condition_attribute_buffer_free;
			}
			default:
			{
				_cd_make_error(CD_ERROR_UNKNOWN_OPERATOR, "Operator %llu is not recognized. table: '%s'", condition->operator, table->name.data);
				_error = 1;
				goto condition_attribute_buffer_free;
			}
			}

		condition_attribute_buffer_free:
			free(condition_attribute_buffer);
			if (_error)
			{
				goto should_add_row_free;
			}
		}
	}

	for (uint64_t row = 0; row < table->count.count_c; row++)
	{
		if (!should_add_row[row])
			continue;

		uint8_t *row_ptr = cd_table_view_get_next_row(table_view);
		for (uint64_t attrib_index = 0; attrib_index < attribute_count; attrib_index++)
		{
			if (!cf_file_view_read(table->data_view, row * table->schema->stride + attribute_data[attrib_index].file_offset, attribute_data[attrib_index].size, row_ptr + attribute_data[attrib_index].data_offset))
			{
				_cd_make_error(CD_ERROR_FILE, "Failed to read attribute '%s' at row %llu from table '%s'", attribute_names[attrib_index], row, table->name.data);
				goto should_add_row_free;
			}
		}
	}

	free(should_add_row);

	free(attribute_data);

	return table_view;

should_add_row_free:
	free(should_add_row);
table_view_data_free:
	free(table_view->data);
table_view_attributes_free:
	free(table_view->attributes);
// table_view_free:
	free(table_view);
// attribute_data_free:
	free(attribute_data);
//_return:
	return NULL;
}
