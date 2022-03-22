#ifndef _C_DATABASE_H_
#define _C_DATABASE_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#include <c_core/c_core.h>
#include <c_file/c_file.h>

typedef enum CD_AttributeType
{
	CD_TYPE_BYTE = 0, // 8 bit
	CD_TYPE_UINT, // 64 bit
	CD_TYPE_SINT, // 64 bit
	CD_TYPE_FLOAT, // 64 bit
	CD_TYPE_CHAR, // uses char 8 bit, char is always the same length (ex: an id); no null termination
	CD_TYPE_WCHAR, // uses wchar_t which is 16 bit
	CD_TYPE_VARCHAR, // uses char 8 bit, varchar can be different lengths but has the same max length; null termination
	CD_TYPE_WVARCHAR // uses wchar_t which is 16 bit
} CD_AttributeType;

typedef uint8_t CD_byte_t;
typedef uint64_t CD_uint_t;
typedef int64_t CD_sint_t;
typedef double CD_float_t;
typedef char CD_char_t;
typedef wchar_t CD_wchar_t;
typedef char CD_varchar_t;
typedef wchar_t CD_wvarchar_t;

uint64_t cd_attribute_type_size(CD_AttributeType type);
uint64_t cd_attribute_size(CD_AttributeType type, uint64_t count);

#define CD_CONSTRAINT_NONE 		0
#define CD_CONSTRAINT_NOT_NULL	0b01
#define CD_CONSTRAINT_UNIQUE	0b10

typedef struct CD_Attribute
{
	uint64_t type;
	uint64_t count;
	uint64_t constraints;
	char name[256];
} CD_Attribute;

typedef struct CD_Database CD_Database;

CD_Database *cd_database_open(CC_String name);
void cd_database_close(CD_Database *db);
uint64_t cd_table_create(CD_Database *db, CC_String table_name, uint64_t attribute_count, CD_Attribute *attributes);
uint64_t cd_table_insert(CD_Database *db, CC_String table_name, uint64_t attribute_count, const char *attribute_names[256], const void *data);

typedef struct CD_TableView
{
	uint64_t stride;
	uint64_t count_c;
	uint64_t count_m;
	uint64_t attribute_count;
	CD_Attribute *attributes;
	void *data;
} CD_TableView;

CD_TableView *cd_table_view_create(CD_Database *db, CC_String table_name, uint64_t attribute_count, const char *attribute_names[256]);
void cd_table_view_destroy(CD_TableView *view);

void *cd_table_view_get_next_row(CD_TableView *view);

typedef struct CD_TableView_Iterator
{
	const void *data;
	const CD_Attribute *attribute;
} CD_TableView_Iterator;
CD_TableView_Iterator cd_table_view_iterator_begin(CD_TableView *view, uint64_t row);
CD_TableView_Iterator cd_table_view_iterator_next(CD_TableView *view, uint64_t row, CD_TableView_Iterator iterator);
uint64_t cd_table_view_iterator_is_end(CD_TableView *view, uint64_t row, CD_TableView_Iterator iterator);

typedef enum CD_ConditionOperator
{
	CD_CONDITION_OPERATOR_EQUALS = 0,
	CD_CONDITION_OPERATOR_DIFFERENT,
	CD_CONDITION_OPERATOR_BIGGER,
	CD_CONDITION_OPERATOR_SMALLER,
	CD_CONDITION_OPERATOR_CONTAINS
} CD_ConditionOperator;

typedef struct CD_Condition
{
	char name[256];
	uint64_t operator;
	const void *data;
} CD_Condition;

CD_TableView *cd_table_select(CD_Database *db, CC_String table_name, uint64_t attribute_count, const char *attribute_names[256], uint64_t condition_count, CD_Condition *conditions);

// error
typedef struct CD_Error
{
	uint64_t error_type;
	CC_String message;
} CD_Error;

typedef enum CD_ErrorType
{
	CD_ERROR_NONE = 0,
	CD_ERROR_FILE,
	CD_ERROR_TABLE_EXISTS,
	CD_ERROR_TABLE_DOES_NOT_EXIST,
	CD_ERROR_ATTRIBUTE_EXISTS,
	CD_ERROR_ATTRIBUTE_DOES_NOT_EXIST,
	CD_ERROR_ATTRIBUTE_IS_NOT_NULL,
	CD_ERROR_ATTRIBUTE_IS_UNIQUE,
	CD_ERROR_UNKNOWN_OPERATOR,
	CD_ERROR_UNKNOWN_TYPE
} CD_ErrorType;

CD_Error cd_get_last_error();

#endif