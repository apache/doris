// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    static palo_udf::StringVal aes_encrypt(palo_udf::FunctionContext* context,
            const palo_udf::StringVal& val1, const palo_udf::StringVal& val2);
    static palo_udf::StringVal aes_decrypt(palo_udf::FunctionContext* context,
            const palo_udf::StringVal& val1, const palo_udf::StringVal& val2);
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
