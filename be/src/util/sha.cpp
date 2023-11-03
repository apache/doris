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

#include "util/sha.h"

#include <openssl/sha.h>

#include <string_view>

namespace doris {

constexpr static char dig_vec_lower[] = "0123456789abcdef";

void SHA1Digest::reset(const void* data, size_t length) {
    SHA1_Init(&_sha_ctx);
    SHA1_Update(&_sha_ctx, data, length);
}

std::string_view SHA1Digest::digest() {
    unsigned char buf[SHA_DIGEST_LENGTH];
    SHA1_Final(buf, &_sha_ctx);

    char* to = _reuse_hex;
    for (int i = 0; i < SHA_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }

    return std::string_view {_reuse_hex, _reuse_hex + 2 * SHA_DIGEST_LENGTH};
}

void SHA224Digest::reset(const void* data, size_t length) {
    SHA224_Init(&_sha224_ctx);
    SHA224_Update(&_sha224_ctx, data, length);
}

std::string_view SHA224Digest::digest() {
    unsigned char buf[SHA224_DIGEST_LENGTH];
    SHA224_Final(buf, &_sha224_ctx);

    char* to = _reuse_hex;
    for (int i = 0; i < SHA224_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }

    return std::string_view {_reuse_hex, _reuse_hex + 2 * SHA224_DIGEST_LENGTH};
}

void SHA256Digest::reset(const void* data, size_t length) {
    SHA256_Init(&_sha256_ctx);
    SHA256_Update(&_sha256_ctx, data, length);
}

std::string_view SHA256Digest::digest() {
    unsigned char buf[SHA256_DIGEST_LENGTH];
    SHA256_Final(buf, &_sha256_ctx);

    char* to = _reuse_hex;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }

    return std::string_view {_reuse_hex, _reuse_hex + 2 * SHA256_DIGEST_LENGTH};
}

void SHA384Digest::reset(const void* data, size_t length) {
    SHA384_Init(&_sha384_ctx);
    SHA384_Update(&_sha384_ctx, data, length);
}

std::string_view SHA384Digest::digest() {
    unsigned char buf[SHA384_DIGEST_LENGTH];
    SHA384_Final(buf, &_sha384_ctx);

    char* to = _reuse_hex;
    for (int i = 0; i < SHA384_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }

    return std::string_view {_reuse_hex, _reuse_hex + 2 * SHA384_DIGEST_LENGTH};
}

void SHA512Digest::reset(const void* data, size_t length) {
    SHA512_Init(&_sha512_ctx);
    SHA512_Update(&_sha512_ctx, data, length);
}

std::string_view SHA512Digest::digest() {
    unsigned char buf[SHA512_DIGEST_LENGTH];
    SHA512_Final(buf, &_sha512_ctx);

    char* to = _reuse_hex;
    for (int i = 0; i < SHA512_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }

    return std::string_view {_reuse_hex, _reuse_hex + 2 * SHA512_DIGEST_LENGTH};
}

} // namespace doris
