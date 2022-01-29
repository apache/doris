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

#include "util/sm3.h"

#include "util/logging.h"

namespace doris {

SM3Digest::SM3Digest() {
    _md = EVP_sm3();
    _ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(_ctx, _md, NULL);
}

SM3Digest::~SM3Digest() {
    EVP_MD_CTX_free(_ctx);
}

void SM3Digest::update(const void* data, size_t length) {
    EVP_DigestUpdate(_ctx, data, length);
}

void SM3Digest::digest() {
    unsigned char buf[SM3_DIGEST_LENGTH];
    uint32_t length;
    EVP_DigestFinal_ex(_ctx, buf, &length);
    DCHECK_EQ(SM3_DIGEST_LENGTH, length);

    char hex_buf[2 * SM3_DIGEST_LENGTH];

    static char dig_vec_lower[] = "0123456789abcdef";
    char* to = hex_buf;
    for (int i = 0; i < SM3_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }
    _hex.assign(hex_buf, 2 * SM3_DIGEST_LENGTH);
}

} // namespace doris
