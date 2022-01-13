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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_ENCRYPTION_FUNCTIONS_H
#define DORIS_BE_SRC_QUERY_EXPRS_ENCRYPTION_FUNCTIONS_H

#include <stdint.h>

#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {

class Expr;
struct ExprValue;
class TupleRow;

class EncryptionFunctions {
public:
    static void init();
    static doris_udf::StringVal aes_encrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key);
    static doris_udf::StringVal aes_decrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key);
    static doris_udf::StringVal aes_encrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key,
                                            const doris_udf::StringVal& iv,
                                            const doris_udf::StringVal& mode);
    static doris_udf::StringVal aes_decrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key,
                                            const doris_udf::StringVal& iv,
                                            const doris_udf::StringVal& mode);
    static doris_udf::StringVal sm4_encrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key);
    static doris_udf::StringVal sm4_decrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key);
    static doris_udf::StringVal sm4_encrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key,
                                            const doris_udf::StringVal& iv,
                                            const doris_udf::StringVal& mode);
    static doris_udf::StringVal sm4_decrypt(doris_udf::FunctionContext* ctx,
                                            const doris_udf::StringVal& src,
                                            const doris_udf::StringVal& key,
                                            const doris_udf::StringVal& iv,
                                            const doris_udf::StringVal& mode);
    static doris_udf::StringVal from_base64(doris_udf::FunctionContext* context,
                                            const doris_udf::StringVal& val1);
    static doris_udf::StringVal to_base64(doris_udf::FunctionContext* context,
                                          const doris_udf::StringVal& val1);
    static doris_udf::StringVal md5sum(doris_udf::FunctionContext* ctx, int num_args,
                                       const doris_udf::StringVal* args);
    static doris_udf::StringVal md5(doris_udf::FunctionContext* ctx,
                                    const doris_udf::StringVal& src);
    static doris_udf::StringVal sm3sum(doris_udf::FunctionContext* ctx, int num_args,
                                       const doris_udf::StringVal* args);
    static doris_udf::StringVal sm3(doris_udf::FunctionContext* ctx,
                                    const doris_udf::StringVal& src);
};

} // namespace doris

#endif
