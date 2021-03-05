#define _GNU_SOURCE
#include <stdlib.h>

char *getenv(const char *name);

char *secure_getenv(const char *name)
{
	// NOTE: Let's assume we're not running as SUID
	return getenv(name);
	/* return libc.secure ? NULL : getenv(name); */
}
