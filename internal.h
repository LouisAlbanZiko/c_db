#ifndef _C_DATABASE_internal_H_
#define _C_DATABASE_internal_H_

#include "c_db.h"

#include <stdarg.h>

typedef struct _CD_Attribute
{
	uint64_t type;
	uint64_t count;
	uint64_t offset;
	uint64_t constraints;
	char name[256];
} _CD_Attribute;

typedef struct CD_Table
{
	CC_String name;
	CC_String file_path;

	// schema
	uint64_t stride;
	CC_HashMap *attribute_indices; // type(uint64_t)
	_CD_Attribute *attributes;

	// schema views
	CF_FileView *attribute_count_view;
	CF_FileView *attributes_view;

	// data views
	CF_File *data_file;
	CF_FileView *data_count_view;
	CF_FileView *data_view;
} CD_Table;

typedef struct CD_Database
{
	CC_String name;
	CC_String schema_file_path;

	CF_File *schema_file;
	CF_FileView *table_count_view;
	CC_HashMap *tables; // type(CD_Table)
} CD_Database;

typedef struct _CD_TableSchemaData
{
	char name[256];
	uint64_t attrib_count_c;
	uint64_t attrib_count_m;
} _CD_TableSchemaData;

typedef struct _CD_TableCount
{
	uint64_t count_c;
	uint64_t count_m;
} _CD_TableCount;

// type comparison
typedef uint64_t (*_cd_func_equal)(const void *data1, const void *data2, uint64_t size);
extern const _cd_func_equal _cd_funcs_equal[];

uint64_t _cd_equal_BYTE(const void *data1, const void *data2, uint64_t size);
uint64_t _cd_equal_UINT(const void *data1, const void *data2, uint64_t count);
uint64_t _cd_equal_SINT(const void *data1, const void *data2, uint64_t count);
uint64_t _cd_equal_FLOAT(const void *data1, const void *data2, uint64_t count);
uint64_t _cd_equal_CHAR(const void *data1, const void *data2, uint64_t count);
uint64_t _cd_equal_WCHAR(const void *data1, const void *data2, uint64_t count);
uint64_t _cd_equal_VARCHAR(const void *data1, const void *data2, uint64_t count);
uint64_t _cd_equal_WVARCHAR(const void *data1, const void *data2, uint64_t count);

// error
void _cd_make_error(uint64_t error_type, const char *format, ...);

#endif