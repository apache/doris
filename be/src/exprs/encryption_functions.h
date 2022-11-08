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

#pragma once

#include <stdint.h>

#include "udf/udf.h"
#include "util/encryption_util.h"
#include "util/string_util.h"

namespace doris {

class Expr;
struct ExprValue;
class TupleRow;
static StringCaseUnorderedMap<EncryptionMode> aes_mode_map {
        {"AES_128_ECB", EncryptionMode::AES_128_ECB},
        {"AES_192_ECB", EncryptionMode::AES_192_ECB},
        {"AES_256_ECB", EncryptionMode::AES_256_ECB},
        {"AES_128_CBC", EncryptionMode::AES_128_CBC},
        {"AES_192_CBC", EncryptionMode::AES_192_CBC},
        {"AES_256_CBC", EncryptionMode::AES_256_CBC},
        {"AES_128_CFB", EncryptionMode::AES_128_CFB},
        {"AES_192_CFB", EncryptionMode::AES_192_CFB},
        {"AES_256_CFB", EncryptionMode::AES_256_CFB},
        {"AES_128_CFB1", EncryptionMode::AES_128_CFB1},
        {"AES_192_CFB1", EncryptionMode::AES_192_CFB1},
        {"AES_256_CFB1", EncryptionMode::AES_256_CFB1},
        {"AES_128_CFB8", EncryptionMode::AES_128_CFB8},
        {"AES_192_CFB8", EncryptionMode::AES_192_CFB8},
        {"AES_256_CFB8", EncryptionMode::AES_256_CFB8},
        {"AES_128_CFB128", EncryptionMode::AES_128_CFB128},
        {"AES_192_CFB128", EncryptionMode::AES_192_CFB128},
        {"AES_256_CFB128", EncryptionMode::AES_256_CFB128},
        {"AES_128_CTR", EncryptionMode::AES_128_CTR},
        {"AES_192_CTR", EncryptionMode::AES_192_CTR},
        {"AES_256_CTR", EncryptionMode::AES_256_CTR},
        {"AES_128_OFB", EncryptionMode::AES_128_OFB},
        {"AES_192_OFB", EncryptionMode::AES_192_OFB},
        {"AES_256_OFB", EncryptionMode::AES_256_OFB}};
static StringCaseUnorderedMap<EncryptionMode> sm4_mode_map {
        {"SM4_128_ECB", EncryptionMode::SM4_128_ECB},
        {"SM4_128_CBC", EncryptionMode::SM4_128_CBC},
        {"SM4_128_CFB128", EncryptionMode::SM4_128_CFB128},
        {"SM4_128_OFB", EncryptionMode::SM4_128_OFB},
        {"SM4_128_CTR", EncryptionMode::SM4_128_CTR}};
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
