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

namespace doris {

enum class EncryptionMode {
    AES_128_ECB,
    AES_192_ECB,
    AES_256_ECB,
    AES_128_CBC,
    AES_192_CBC,
    AES_256_CBC,
    AES_128_CFB,
    AES_192_CFB,
    AES_256_CFB,
    AES_128_CFB1,
    AES_192_CFB1,
    AES_256_CFB1,
    AES_128_CFB8,
    AES_192_CFB8,
    AES_256_CFB8,
    AES_128_CFB128,
    AES_192_CFB128,
    AES_256_CFB128,
    AES_128_CTR,
    AES_192_CTR,
    AES_256_CTR,
    AES_128_OFB,
    AES_192_OFB,
    AES_256_OFB,
    AES_128_GCM,
    AES_192_GCM,
    AES_256_GCM,
    SM4_128_ECB,
    SM4_128_CBC,
    SM4_128_CFB128,
    SM4_128_OFB,
    SM4_128_CTR
};

enum EncryptionState { AES_SUCCESS = 0, AES_BAD_DATA = -1 };

class EncryptionUtil {
public:
    static bool is_gcm_mode(EncryptionMode mode) {
        return mode == EncryptionMode::AES_128_GCM || mode == EncryptionMode::AES_192_GCM ||
               mode == EncryptionMode::AES_256_GCM;
    }

    // https://tools.ietf.org/html/rfc5116#section-5.1
    static const int GCM_TAG_SIZE = 16;

    static int encrypt(EncryptionMode mode, const unsigned char* source, uint32_t source_length,
                       const unsigned char* key, uint32_t key_length, const char* iv_str,
                       int iv_input_length, bool padding, unsigned char* encrypt,
                       const unsigned char* aad = nullptr, uint32_t aad_length = 0);

    static int decrypt(EncryptionMode mode, const unsigned char* encrypt, uint32_t encrypt_length,
                       const unsigned char* key, uint32_t key_length, const char* iv_str,
                       int iv_input_length, bool padding, unsigned char* decrypt_content,
                       const unsigned char* aad = nullptr, uint32_t aad_length = 0);
};

} // namespace doris
