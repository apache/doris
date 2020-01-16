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

#include "util/md5.h"

namespace doris {

Md5Digest::Md5Digest() {
    MD5_Init(&_md5_ctx);
}

void Md5Digest::update(const void* data, size_t length) {
    MD5_Update(&_md5_ctx, data, length);
}

void Md5Digest::digest() {
    unsigned char buf[MD5_DIGEST_LENGTH];
    MD5_Final(buf, &_md5_ctx);

    char hex_buf[2 * MD5_DIGEST_LENGTH];

    static char dig_vec_lower[] = "0123456789abcdef";
    char* to = hex_buf;
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        *to++ = dig_vec_lower[buf[i] >> 4];
        *to++ = dig_vec_lower[buf[i] & 0x0F];
    }
    _hex.assign(hex_buf, 2 * MD5_DIGEST_LENGTH);
}

} // namespace doris
