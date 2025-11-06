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

#include "chinese_util.h"

#include <unicode/unistr.h>
#include <unicode/utf8.h>

namespace doris::segment_v2::inverted_index {

std::vector<std::string> ChineseUtil::segmentChinese(const std::vector<UChar32>& codepoints) {
    std::vector<std::string> out;
    out.reserve(codepoints.size());

    for (UChar32 cp : codepoints) {
        if (cp >= CJK_UNIFIED_IDEOGRAPHS_START && cp <= CJK_UNIFIED_IDEOGRAPHS_END) {
            char utf8_buffer[4];
            int32_t utf8_len = 0;
            U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
            out.emplace_back(utf8_buffer, utf8_len);
        } else {
            out.emplace_back("");
        }
    }
    return out;
}

} // namespace doris::segment_v2::inverted_index
