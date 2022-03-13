#include "internal.h"

#define ERROR_STRING_SIZE ((uint64_t)1024)

uint64_t _cd_error_type = 0;
char _cd_error_message[ERROR_STRING_SIZE];

CD_Error cd_get_last_error()
{
	CD_Error error =
	{
		.error_type = _cd_error_type,
		.message = {
			.data = _cd_error_message,
			.length = strlen(_cd_error_message)
		}
	};
	return error;
}

void _cd_make_error(uint64_t error_type, const char *format, ...)
{
	_cd_error_type = error_type;

	va_list args;
	va_start(args, format);

	vsprintf_s(_cd_error_message, ERROR_STRING_SIZE, format, args);

	va_end(args);
}