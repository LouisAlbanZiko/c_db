#ifndef _C_DATABASE_H_
#define _C_DATABASE_H_

#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>

#include <c_core/c_core.h>
#include <c_file/c_file.h>

typedef enum CD_AttributeType
{
	CD_ATTRIBUTE_UINT_8,
	CD_ATTRIBUTE_UINT_16,
	CD_ATTRIBUTE_UINT_32,
	CD_ATTRIBUTE_UINT_64,
	CD_ATTRIBUTE_SINT_8,
	CD_ATTRIBUTE_SINT_16,
	CD_ATTRIBUTE_SINT_32,
	CD_ATTRIBUTE_SINT_64,
	CD_ATTRIBUTE_FLOAT,
	CD_ATTRIBUTE_CHAR, // char is always the same length (ex: an id); no null termination
	CD_ATTRIBUTE_VARCHAR // varchar can be different lengths but has the same max length; null termination
} CD_AttributeType;

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
	CD_Attribute *attributes;
	void *data;
} CD_TableView;

CD_TableView *cd_table_view_create(CD_Database *db, CC_String table_name, uint64_t attribute_count, const char *attribute_names[256]);
void cd_table_view_destroy(CD_TableView *view);

void *cd_table_view_get_next_row(CD_TableView *view);

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
	CD_ERROR_UNKNOWN_OPERATOR
} CD_ErrorType;

CD_Error cd_get_last_error();

#endif