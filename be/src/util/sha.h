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

#include <openssl/sha.h>

#include <string_view>

namespace doris {

class SHA1Digest {
public:
    void reset(const void* data, size_t length);
    std::string_view digest();

private:
    SHA_CTX _sha_ctx;
    char _reuse_hex[2 * SHA_DIGEST_LENGTH];
};

class SHA224Digest {
public:
    void reset(const void* data, size_t length);
    std::string_view digest();

private:
    SHA256_CTX _sha224_ctx;
    char _reuse_hex[2 * SHA224_DIGEST_LENGTH];
};

class SHA256Digest {
public:
    void reset(const void* data, size_t length);
    std::string_view digest();

private:
    SHA256_CTX _sha256_ctx;
    char _reuse_hex[2 * SHA256_DIGEST_LENGTH];
};

class SHA384Digest {
public:
    void reset(const void* data, size_t length);
    std::string_view digest();

private:
    SHA512_CTX _sha384_ctx;
    char _reuse_hex[2 * SHA384_DIGEST_LENGTH];
};

class SHA512Digest {
public:
    void reset(const void* data, size_t length);
    std::string_view digest();

private:
    SHA512_CTX _sha512_ctx;
    char _reuse_hex[2 * SHA512_DIGEST_LENGTH];
};
} // namespace doris
