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

#include <cstdint>
#include <string>
#include <string_view>

namespace doris::segment_v2::inverted_index::kuromoji {

// CJK width folding (a subset of Lucene's CJKWidthFilter): full-width ASCII
// variants U+FF01..U+FF5E -> basic-latin U+0021..U+007E, and the ideographic
// space U+3000 -> ' '. So ＡＢＣ１２３ -> ABC123. Everything else is preserved
// byte-for-byte. (Half-width katakana -> full-width composition is a TODO.)
inline std::string cjk_width_normalize(std::string_view in) {
    std::string out;
    out.reserve(in.size());
    std::size_t i = 0;
    const std::size_t n = in.size();
    while (i < n) {
        const auto b0 = static_cast<unsigned char>(in[i]);
        std::size_t len = 1;
        char32_t cp = b0;
        if (b0 >= 0xF0 && i + 3 < n) {
            cp = static_cast<char32_t>(((b0 & 0x07U) << 18) |
                                       ((static_cast<unsigned char>(in[i + 1]) & 0x3FU) << 12) |
                                       ((static_cast<unsigned char>(in[i + 2]) & 0x3FU) << 6) |
                                       (static_cast<unsigned char>(in[i + 3]) & 0x3FU));
            len = 4;
        } else if (b0 >= 0xE0 && i + 2 < n) {
            cp = static_cast<char32_t>(((b0 & 0x0FU) << 12) |
                                       ((static_cast<unsigned char>(in[i + 1]) & 0x3FU) << 6) |
                                       (static_cast<unsigned char>(in[i + 2]) & 0x3FU));
            len = 3;
        } else if (b0 >= 0xC0 && i + 1 < n) {
            cp = static_cast<char32_t>(((b0 & 0x1FU) << 6) |
                                       (static_cast<unsigned char>(in[i + 1]) & 0x3FU));
            len = 2;
        }

        if (cp >= 0xFF01 && cp <= 0xFF5E) {
            out.push_back(static_cast<char>(cp - 0xFEE0)); // full-width ASCII -> ASCII
        } else if (cp == 0x3000) {
            out.push_back(' '); // ideographic space -> space
        } else {
            out.append(in.substr(i, len)); // keep original bytes
        }
        i += len;
    }
    return out;
}

} // namespace doris::segment_v2::inverted_index::kuromoji
