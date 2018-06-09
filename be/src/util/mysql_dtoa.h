/****************************************************************
 *
 * The author of this software is David M. Gay.
 *
 * Copyright (c) 1991, 2000, 2001 by Lucent Technologies.
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose without fee is hereby granted, provided that this entire notice
 * is included in all copies of any software which is or includes a copy
 * or modification of this software and in all copies of the supporting
 * documentation for such software.
 *
 * THIS SOFTWARE IS BEING PROVIDED "AS IS", WITHOUT ANY EXPRESS OR IMPLIED
 * WARRANTY.  IN PARTICULAR, NEITHER THE AUTHOR NOR LUCENT MAKES ANY
 * REPRESENTATION OR WARRANTY OF ANY KIND CONCERNING THE MERCHANTABILITY
 * OF THIS SOFTWARE OR ITS FITNESS FOR ANY PARTICULAR PURPOSE.
 *
 ***************************************************************/

#ifndef BDG_PALO_BE_SRC_QUERY_MYSQL_MYSQL_DTOA_H
#define BDG_PALO_BE_SRC_QUERY_MYSQL_MYSQL_DTOA_H

#include <stddef.h>
namespace palo {
/* Conversion routines */
typedef enum {
    MY_GCVT_ARG_FLOAT,
    MY_GCVT_ARG_DOUBLE
} my_gcvt_arg_type;

size_t my_gcvt(double x, my_gcvt_arg_type type, int width, char *to, bool *error);
}
#endif
