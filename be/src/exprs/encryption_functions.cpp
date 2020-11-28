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

#include <boost/smart_ptr.hpp>

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/aes_util.h"
#include "util/debug_util.h"
#include "util/md5.h"
#include "util/url_coding.h"

namespace doris {
void EncryptionFunctions::init() {}

StringVal EncryptionFunctions::aes_encrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key) {
    if (src.len == 0) {
        return StringVal::null();
    }

    // cipher_len = (clearLen/16 + 1) * 16;
    int cipher_len = src.len + 16;
    boost::scoped_array<char> p;
    p.reset(new char[cipher_len]);

    int ret_code =
            AesUtil::encrypt(AES_128_ECB, (unsigned char*)src.ptr, src.len, (unsigned char*)key.ptr,
                             key.len, NULL, true, (unsigned char*)p.get());
    if (ret_code < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, p.get(), ret_code);
}

StringVal EncryptionFunctions::aes_decrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key) {
    if (src.len == 0) {
        return StringVal::null();
    }

    int cipher_len = src.len;
    boost::scoped_array<char> p;
    p.reset(new char[cipher_len]);

    int ret_code =
            AesUtil::decrypt(AES_128_ECB, (unsigned char*)src.ptr, src.len, (unsigned char*)key.ptr,
                             key.len, NULL, true, (unsigned char*)p.get());
    if (ret_code < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, p.get(), ret_code);
}

StringVal EncryptionFunctions::from_base64(FunctionContext* ctx, const StringVal& src) {
    if (src.len == 0 || src.is_null) {
        return StringVal::null();
    }

    int cipher_len = src.len;
    boost::scoped_array<char> p;
    p.reset(new char[cipher_len]);

    int ret_code = base64_decode((const char*)src.ptr, src.len, p.get());
    if (ret_code < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, p.get(), ret_code);
}

StringVal EncryptionFunctions::to_base64(FunctionContext* ctx, const StringVal& src) {
    if (src.len == 0 || src.is_null) {
        return StringVal::null();
    }

    int cipher_len = (size_t)(4.0 * ceil((double)src.len / 3.0));
    boost::scoped_array<char> p;
    p.reset(new char[cipher_len]);

    int ret_code = base64_encode((unsigned char*)src.ptr, src.len, (unsigned char*)p.get());
    if (ret_code < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, p.get(), ret_code);
}

StringVal EncryptionFunctions::md5sum(FunctionContext* ctx, int num_args, const StringVal* args) {
    Md5Digest digest;
    for (int i = 0; i < num_args; ++i) {
        const StringVal& arg = args[i];
        if (arg.is_null) {
            continue;
        }
        digest.update(arg.ptr, arg.len);
    }
    digest.digest();
    return AnyValUtil::from_buffer_temp(ctx, digest.hex().c_str(), digest.hex().size());
}

StringVal EncryptionFunctions::md5(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return StringVal::null();
    }
    Md5Digest digest;
    digest.update(src.ptr, src.len);
    digest.digest();
    return AnyValUtil::from_buffer_temp(ctx, digest.hex().c_str(), digest.hex().size());
}

} // namespace doris
