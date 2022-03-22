#include "internal.h"

// equal

const _cd_func_equal _cd_funcs_equal[] =
{
	_cd_equal_BYTE,
	_cd_equal_UINT,
	_cd_equal_SINT,
	_cd_equal_FLOAT,
	_cd_equal_CHAR,
	_cd_equal_WCHAR,
	_cd_equal_VARCHAR,
	_cd_equal_WVARCHAR
};

uint64_t _cd_equal_BYTE(const void *data1, const void *data2, uint64_t count)
{
	const uint8_t *bytes1 = data1;
	const uint8_t *bytes2 = data2;
	for(uint64_t i = 0; i < count; i++)
	{
		if(bytes1[i] != bytes2[i])
		{
			return 0;
		}
	}
	return 1;
}

uint64_t _cd_equal_UINT(const void *data1, const void *data2, uint64_t count)
{
	const uint64_t *nr_ptr1 = data1;
	const uint64_t *nr_ptr2 = data2;
	for(uint64_t i = 0; i < count; i++)
	{
		if(nr_ptr1[i] != nr_ptr2[i])
		{
			return 0;
		}
	}
	return 1;
}

uint64_t _cd_equal_SINT(const void *data1, const void *data2, uint64_t count)
{
	const int64_t *nr_ptr1 = data1;
	const int64_t *nr_ptr2 = data2;
	for(uint64_t i = 0; i < count; i++)
	{
		if(nr_ptr1[i] != nr_ptr2[i])
		{
			return 0;
		}
	}
	return 1;
}

uint64_t _cd_equal_FLOAT(const void *data1, const void *data2, uint64_t count)
{
	const double *nr_ptr1 = data1;
	const double *nr_ptr2 = data2;
	for(uint64_t i = 0; i < count; i++)
	{
		if(nr_ptr1[i] != nr_ptr2[i])
		{
			return 0;
		}
	}
	return 1;
}

uint64_t _cd_equal_CHAR(const void *data1, const void *data2, uint64_t count)
{
	const char *nr_ptr1 = data1;
	const char *nr_ptr2 = data2;
	for(uint64_t i = 0; i < count; i++)
	{
		if(nr_ptr1[i] != nr_ptr2[i])
		{
			return 0;
		}
	}
	return 1;
}

uint64_t _cd_equal_WCHAR(const void *data1, const void *data2, uint64_t count)
{
	const wchar_t *nr_ptr1 = data1;
	const wchar_t *nr_ptr2 = data2;
	for(uint64_t i = 0; i < count; i++)
	{
		if(nr_ptr1[i] != nr_ptr2[i])
		{
			return 0;
		}
	}
	return 1;
}

uint64_t _cd_equal_VARCHAR(const void *data1, const void *data2, uint64_t count)
{
	const char *iter1 = data1;
	const char *iter2 = data2;
	while(iter1 != 0 && iter2 != 0 && (uint64_t)iter1 - (uint64_t)data1 < count * sizeof(char))
	{
		if(*iter1 != *iter2)
		{
			return 0;
		}
		iter1++;
		iter2++;
	}
	return 1;
}

uint64_t _cd_equal_WVARCHAR(const void *data1, const void *data2, uint64_t count)
{
	const wchar_t *iter1 = data1;
	const wchar_t *iter2 = data2;
	while(iter1 != 0 && iter2 != 0 && (uint64_t)iter1 - (uint64_t)data1 < count * sizeof(wchar_t))
	{
		if(*iter1 != *iter2)
		{
			return 0;
		}
		iter1++;
		iter2++;
	}
	return 1;
}

uint64_t cd_attribute_type_size(CD_AttributeType type)
{
	switch (type)
	{
	case CD_TYPE_BYTE:
	case CD_TYPE_CHAR:
	case CD_TYPE_VARCHAR:
		return 1;
	case CD_TYPE_WCHAR:
	case CD_TYPE_WVARCHAR:
		return 2;
	case CD_TYPE_UINT:
	case CD_TYPE_SINT:
	case CD_TYPE_FLOAT:
		return 8;
	default:
		_cd_make_error(CD_ERROR_UNKNOWN_TYPE, "Unknown type of value %llu", type);
		return 0;
	}
}

uint64_t cd_attribute_size(CD_AttributeType type, uint64_t count)
{
	return cd_attribute_type_size(type) * count;
}
