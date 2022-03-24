#ifndef _C_DATABASE_internal_H_
#define _C_DATABASE_internal_H_

#include "c_db.h"

#include <stdarg.h>

// file data structs
typedef struct _CD_File_TableSchema
{
	char name[CD_NAME_LENGTH];
	uint64_t attrib_count_c;
	uint64_t attrib_count_m;
} _CD_File_TableSchema;

#define CD_ROW_COUNT_START 32

typedef struct _CD_File_RowCount
{
	uint64_t count_c;
	uint64_t count_m;
} _CD_File_RowCount;

typedef struct _CD_File_Attribute
{
	char name[CD_NAME_LENGTH];
	uint64_t type;
	uint64_t count;
	uint64_t constraints;
} _CD_File_Attribute;

// structs
typedef struct CD_TableSchema
{
	uint64_t stride;
	CC_HashMap *attribute_indices; // type(uint64_t)
	CD_AttributeEx *attributes;
} CD_TableSchema;

typedef struct CD_Table
{
	CC_String name;
	CC_String file_path;

	_CD_File_RowCount count;

	// schema
	const CD_TableSchema *schema;

	// data views
	CF_File *file;
	CF_FileView *count_view;
	CF_FileView *data_view;
} CD_Table;

typedef struct CD_Database
{
	CC_String name;
	CC_String schema_file_path;

	CC_HashMap *table_schemas; // type(CD_TableSchema)

	CF_File *schema_file;
	CF_FileView *schema_count_view;
} CD_Database;

// type comparison
typedef uint64_t (*_cd_func_equal)(const void *data1, const void *data2, uint64_t count);
extern const _cd_func_equal _cd_funcs_equal[];

uint64_t _cd_equal_BYTE(const void *data1, const void *data2, uint64_t count);
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