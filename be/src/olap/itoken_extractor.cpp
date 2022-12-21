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

#include "itoken_extractor.h"

#include "util/simd/vstring_function.h"

namespace doris {

bool NgramTokenExtractor::next_in_string(const char* data, size_t length, size_t* __restrict pos,
                                         size_t* __restrict token_start,
                                         size_t* __restrict token_length) const {
    *token_start = *pos;
    *token_length = 0;
    size_t code_points = 0;
    for (; code_points < n && *token_start + *token_length < length; ++code_points) {
        size_t sz = get_utf8_byte_length(static_cast<uint8_t>(data[*token_start + *token_length]));
        *token_length += sz;
    }
    *pos += get_utf8_byte_length(static_cast<uint8_t>(data[*pos]));
    return code_points == n;
}

bool NgramTokenExtractor::next_in_string_like(const char* data, size_t length, size_t* pos,
                                              std::string& token) const {
    token.clear();

    size_t code_points = 0;
    bool escaped = false;
    for (size_t i = *pos; i < length;) {
        if (escaped && (data[i] == '%' || data[i] == '_' || data[i] == '\\')) {
            token += data[i];
            ++code_points;
            escaped = false;
            ++i;
        } else if (!escaped && (data[i] == '%' || data[i] == '_')) {
            /// This token is too small, go to the next.
            token.clear();
            code_points = 0;
            escaped = false;
            *pos = ++i;
        } else if (!escaped && data[i] == '\\') {
            escaped = true;
            ++i;
        } else {
            const size_t sz = get_utf8_byte_length(static_cast<uint8_t>(data[i]));
            for (size_t j = 0; j < sz; ++j) {
                token += data[i + j];
            }
            i += sz;
            ++code_points;
            escaped = false;
        }

        if (code_points == n) {
            *pos += get_utf8_byte_length(static_cast<uint8_t>(data[*pos]));
            return true;
        }
    }

    return false;
}
} // namespace doris
