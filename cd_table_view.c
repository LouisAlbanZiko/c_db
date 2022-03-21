#include "internal.h"

CD_TableView *cd_table_view_create(CD_Database *db, CC_String table_name, uint64_t attribute_count, const char *attribute_names[256])
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

	CD_TableView *table_view = malloc(sizeof(*table_view));

	table_view->count_c = 0;
	table_view->count_m = 32;
	table_view->stride = 0;
	table_view->attributes = malloc(sizeof(table_view->attributes[0]) * attribute_count);
	table_view->data = NULL;

	for (uint64_t attrib_index = 0; attrib_index < attribute_count; attrib_index++)
	{
		CC_String attrib_name =
			{
				.data = attribute_names[attrib_index],
				.length = strlen(attribute_names[attrib_index])};

		const uint64_t *index_ptr = (const uint64_t *)cc_hash_map_lookup(table->attribute_indices, attrib_name);

		if (index_ptr == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[attrib_index], table->name.data);
			goto table_destroy;
		}

		uint64_t index = *index_ptr;

		_CD_Attribute *table_attribute = table->attributes + index;

		table_view->attributes[attrib_index].count = table_attribute->count;
		table_view->attributes[attrib_index].type = table_attribute->type;
		table_view->attributes[attrib_index].constraints = table_attribute->constraints;
		memset(table_view->attributes[attrib_index].name, 0, 256);
		strcpy_s(table_view->attributes[attrib_index].name, 256, table_attribute->name);

		table_view->stride += cd_attribute_size(table_attribute->type, table_attribute->count);
	}

	table_view->data = malloc(table_view->count_m * table_view->stride);

	return table_view;

	// errors
table_destroy:
	cd_table_view_destroy(table_view);
_return:
	return NULL;
}

void cd_table_view_destroy(CD_TableView *view)
{
	if (view != NULL)
	{
		if (view->data != NULL)
		{
			free(view->data);
		}
		if (view->attributes != NULL)
		{
			free(view->attributes);
		}
		free(view);
	}
}

void *cd_table_view_get_next_row(CD_TableView *table_view)
{
	if (table_view->count_c == table_view->count_m)
	{
		table_view->count_m += 32;
		table_view->data = realloc(table_view->data, table_view->count_m * table_view->stride);
	}
	void *ptr = (uint8_t *)table_view->data + table_view->count_c * table_view->stride;
	table_view->count_c++;
	return ptr;
}
