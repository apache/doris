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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_ENCRYPTION_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_ENCRYPTION_FUNCTIONS_H

#include <stdint.h>
#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace palo {

class Expr;
struct ExprValue;
class TupleRow;

class EncryptionFunctions {
public:
    static void init();
    static palo_udf::StringVal from_base64(palo_udf::FunctionContext* context,
            const palo_udf::StringVal& val1);
    static palo_udf::StringVal to_base64(palo_udf::FunctionContext* context,
            const palo_udf::StringVal& val1);
    static palo_udf::StringVal md5sum(palo_udf::FunctionContext* ctx, 
                                      int num_args, const palo_udf::StringVal* args);
    static palo_udf::StringVal md5(palo_udf::FunctionContext* ctx, 
                                   const palo_udf::StringVal& src);
};

}

#endif
