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

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/debug_util.h"
#include "util/md5.h"
#include "util/sm3.h"
#include "util/url_coding.h"

namespace doris {
void EncryptionFunctions::init() {}

StringVal encrypt(FunctionContext* ctx, const StringVal& src, const StringVal& key,
                  const StringVal& iv, EncryptionMode mode) {
    if (src.len == 0 || src.is_null) {
        return StringVal::null();
    }
    /*
     * Buffer for ciphertext. Ensure the buffer is long enough for the
     * ciphertext which may be longer than the plaintext, depending on the
     * algorithm and mode.
     */

    int cipher_len = src.len + 16;
    std::unique_ptr<char[]> cipher_text;
    cipher_text.reset(new char[cipher_len]);
    int cipher_text_len = 0;
    cipher_text_len = EncryptionUtil::encrypt(mode, (unsigned char*)src.ptr, src.len,
                                              (unsigned char*)key.ptr, key.len, (char*)iv.ptr, true,
                                              (unsigned char*)cipher_text.get());
    if (cipher_text_len < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, cipher_text.get(), cipher_text_len);
}

StringVal decrypt(FunctionContext* ctx, const StringVal& src, const StringVal& key,
                  const StringVal& iv, EncryptionMode mode) {
    if (src.len == 0 || src.is_null) {
        return StringVal::null();
    }
    int cipher_len = src.len;
    std::unique_ptr<char[]> plain_text;
    plain_text.reset(new char[cipher_len]);
    int plain_text_len = 0;
    plain_text_len =
            EncryptionUtil::decrypt(mode, (unsigned char*)src.ptr, src.len, (unsigned char*)key.ptr,
                                    key.len, (char*)iv.ptr, true, (unsigned char*)plain_text.get());
    if (plain_text_len < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, plain_text.get(), plain_text_len);
}

StringVal EncryptionFunctions::aes_encrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key) {
    return aes_encrypt(ctx, src, key, StringVal::null(), StringVal("AES_128_ECB"));
}

StringVal EncryptionFunctions::aes_decrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key) {
    return aes_decrypt(ctx, src, key, StringVal::null(), StringVal("AES_128_ECB"));
}

StringVal EncryptionFunctions::aes_encrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key, const StringVal& iv,
                                           const StringVal& mode) {
    EncryptionMode encryption_mode = AES_128_ECB;
    if (mode.len != 0 && !mode.is_null) {
        std::string mode_str(reinterpret_cast<char*>(mode.ptr), mode.len);
        if (aes_mode_map.count(mode_str) == 0) {
            return StringVal::null();
        }
        encryption_mode = aes_mode_map.at(mode_str);
    }
    return encrypt(ctx, src, key, iv, encryption_mode);
}

StringVal EncryptionFunctions::aes_decrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key, const StringVal& iv,
                                           const StringVal& mode) {
    EncryptionMode encryption_mode = AES_128_ECB;
    if (mode.len != 0 && !mode.is_null) {
        std::string mode_str(reinterpret_cast<char*>(mode.ptr), mode.len);
        if (aes_mode_map.count(mode_str) == 0) {
            return StringVal::null();
        }
        encryption_mode = aes_mode_map.at(mode_str);
    }
    return decrypt(ctx, src, key, iv, encryption_mode);
}

StringVal EncryptionFunctions::sm4_encrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key) {
    return sm4_encrypt(ctx, src, key, StringVal::null(), StringVal("SM4_128_ECB"));
}

StringVal EncryptionFunctions::sm4_decrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key) {
    return sm4_decrypt(ctx, src, key, StringVal::null(), StringVal("SM4_128_ECB"));
}

StringVal EncryptionFunctions::sm4_encrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key, const StringVal& iv,
                                           const StringVal& mode) {
    EncryptionMode encryption_mode = SM4_128_ECB;
    if (mode.len != 0 && !mode.is_null) {
        std::string mode_str(reinterpret_cast<char*>(mode.ptr), mode.len);
        if (sm4_mode_map.count(mode_str) == 0) {
            return StringVal::null();
        }
        encryption_mode = sm4_mode_map.at(mode_str);
    }
    return encrypt(ctx, src, key, iv, encryption_mode);
}

StringVal EncryptionFunctions::sm4_decrypt(FunctionContext* ctx, const StringVal& src,
                                           const StringVal& key, const StringVal& iv,
                                           const StringVal& mode) {
    EncryptionMode encryption_mode = SM4_128_ECB;
    if (mode.len != 0 && !mode.is_null) {
        std::string mode_str(reinterpret_cast<char*>(mode.ptr), mode.len);
        if (sm4_mode_map.count(mode_str) == 0) {
            return StringVal::null();
        }
        encryption_mode = sm4_mode_map.at(mode_str);
    }
    return decrypt(ctx, src, key, iv, encryption_mode);
}

StringVal EncryptionFunctions::from_base64(FunctionContext* ctx, const StringVal& src) {
    if (src.len == 0 || src.is_null) {
        return StringVal::null();
    }

    int encoded_len = src.len;
    std::unique_ptr<char[]> plain_text;
    plain_text.reset(new char[encoded_len]);

    int plain_text_len = base64_decode((const char*)src.ptr, src.len, plain_text.get());
    if (plain_text_len < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, plain_text.get(), plain_text_len);
}

StringVal EncryptionFunctions::to_base64(FunctionContext* ctx, const StringVal& src) {
    if (src.len == 0 || src.is_null) {
        return StringVal::null();
    }

    int encoded_len = (size_t)(4.0 * ceil((double)src.len / 3.0));
    std::unique_ptr<char[]> encoded_text;
    encoded_text.reset(new char[encoded_len]);

    int encoded_text_len =
            base64_encode((unsigned char*)src.ptr, src.len, (unsigned char*)encoded_text.get());
    if (encoded_text_len < 0) {
        return StringVal::null();
    }
    return AnyValUtil::from_buffer_temp(ctx, encoded_text.get(), encoded_text_len);
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

StringVal EncryptionFunctions::sm3sum(FunctionContext* ctx, int num_args, const StringVal* args) {
    SM3Digest digest;
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

StringVal EncryptionFunctions::sm3(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return StringVal::null();
    }
    SM3Digest digest;
    digest.update(src.ptr, src.len);
    digest.digest();
    return AnyValUtil::from_buffer_temp(ctx, digest.hex().c_str(), digest.hex().size());
}

} // namespace doris
