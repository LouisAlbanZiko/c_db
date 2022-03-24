#include "internal.h"

CD_TableView *cd_table_view_create(CD_Table *table, uint64_t attribute_count, const char *attribute_names[])
{
	CD_TableView *table_view = malloc(sizeof(*table_view));

	table_view->count_c = 0;
	table_view->count_m = 32;
	table_view->stride = 0;
	table_view->attribute_count = attribute_count;
	table_view->attributes = malloc(sizeof(table_view->attributes[0]) * attribute_count);
	table_view->data = NULL;

	for (uint64_t attrib_index = 0; attrib_index < attribute_count; attrib_index++)
	{
		CC_String attrib_name = cc_string_create(attribute_names[attrib_index], 0);
		const uint64_t *index_ptr = (const uint64_t *)cc_hash_map_lookup(table->schema->attribute_indices, attrib_name);
		cc_string_destroy(attrib_name);

		if (index_ptr == NULL)
		{
			_cd_make_error(CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST, "Attribute '%s' does not exist in table '%s'", attribute_names[attrib_index], table->name.data);
			goto table_destroy;
		}

		uint64_t index = *index_ptr;

		CD_AttributeEx *table_attribute = table->schema->attributes + index;

		memset(table_view->attributes[attrib_index].name, 0, 256);
		strcpy_s(table_view->attributes[attrib_index].name, 256, table_attribute->name);
		table_view->attributes[attrib_index].count = table_attribute->count;
		table_view->attributes[attrib_index].type = table_attribute->type;
		table_view->attributes[attrib_index].constraints = table_attribute->constraints;
		table_view->attributes[attrib_index].offset = table_view->stride;
		table_view->attributes[attrib_index].size = table_attribute->size;

		table_view->stride += cd_attribute_size(table_attribute->type, table_attribute->count);
	}

	table_view->data = malloc(table_view->count_m * table_view->stride);

	return table_view;

	// errors
table_destroy:
	cd_table_view_destroy(table_view);
//_return:
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

CD_TableView_Iterator cd_table_view_iterator_begin(CD_TableView *view, uint64_t row)
{
	CD_TableView_Iterator iterator =
	{
		.data = (uint8_t *)view->data + row * view->stride,
		.attribute = view->attributes
	};
	return iterator;
}

CD_TableView_Iterator cd_table_view_iterator_next(CD_TableView *view, uint64_t row, CD_TableView_Iterator iterator)
{
	iterator.data = (uint8_t *)iterator.data + iterator.attribute->size;
	iterator.attribute++;
	return iterator;
}

uint64_t cd_table_view_iterator_is_end(CD_TableView *view, uint64_t row, CD_TableView_Iterator iterator)
{
	return (iterator.data == (uint8_t *)view->data + (row + 1) * view->stride);
}
