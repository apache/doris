// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include "exprs/encryption_functions.h"

#include <openssl/md5.h>
#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "util/debug_util.h"
#include "runtime/tuple_row.h"
#include "exprs/base64.h"
#include <boost/smart_ptr.hpp>
#include "runtime/string_value.h"

namespace palo {
void EncryptionFunctions::init() {
}

StringVal EncryptionFunctions::from_base64(FunctionContext* ctx, const StringVal &src) {
    if (src.len == 0) {
        return StringVal::null();
    }

    int cipher_len = src.len;
    boost::scoped_array<char> p;
    p.reset(new char[cipher_len]);

    int ret_code = base64_decode2((const char *)src.ptr, src.len, p.get());
    if (ret_code < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, p.get(), ret_code);
}

StringVal EncryptionFunctions::to_base64(FunctionContext* ctx, const StringVal &src) {
    if (src.len == 0) {
        return StringVal::null();
    }

    int cipher_len = src.len * 4 / 3 + 1;
    boost::scoped_array<char> p;
    p.reset(new char[cipher_len]);

    int ret_code = base64_encode2((unsigned char *)src.ptr, src.len, (unsigned char *)p.get());
    if (ret_code < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, p.get(), ret_code);
}

StringVal EncryptionFunctions::md5sum(
        FunctionContext* ctx, int num_args, const StringVal* args) {
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);
    for (int i = 0; i < num_args; ++i) {
        const StringVal& arg = args[i];
        if (arg.is_null) {
            continue;
        }
        MD5_Update(&md5_ctx, arg.ptr, arg.len);
    }
    unsigned char buf[MD5_DIGEST_LENGTH];
    MD5_Final(buf, &md5_ctx);
    unsigned char hex_buf[2 * MD5_DIGEST_LENGTH];

    static char dig_vec_lower[] = "0123456789abcdef";
    unsigned char* to = hex_buf;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        *to++= dig_vec_lower[buf[i] >> 4];
        *to++= dig_vec_lower[buf[i] & 0x0F];
    }

    return AnyValUtil::from_buffer_temp(ctx, (char*)hex_buf, 2 * MD5_DIGEST_LENGTH);
}

StringVal EncryptionFunctions::md5(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return StringVal::null();
    }
    MD5_CTX md5_ctx;
    MD5_Init(&md5_ctx);
    MD5_Update(&md5_ctx, src.ptr, src.len);

    unsigned char buf[MD5_DIGEST_LENGTH];
    MD5_Final(buf, &md5_ctx);
    unsigned char hex_buf[2 * MD5_DIGEST_LENGTH];

    static char dig_vec_lower[] = "0123456789abcdef";
    unsigned char* to = hex_buf;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        *to++= dig_vec_lower[buf[i] >> 4];
        *to++= dig_vec_lower[buf[i] & 0x0F];
    }

    return AnyValUtil::from_buffer_temp(ctx, (char*)hex_buf, 2 * MD5_DIGEST_LENGTH);
}

}
