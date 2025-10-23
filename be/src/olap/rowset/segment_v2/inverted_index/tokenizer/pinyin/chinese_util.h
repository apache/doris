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

#include <string>
#include <vector>

#include "unicode/uchar.h"

namespace doris::segment_v2::inverted_index {
class ChineseUtil {
public:
    static constexpr UChar32 CJK_UNIFIED_IDEOGRAPHS_START = 0x4E00;
    static constexpr UChar32 CJK_UNIFIED_IDEOGRAPHS_END = 0x9FA5;

    static std::vector<std::string> segmentChinese(const std::string& utf8_text);

    static std::vector<std::string> segmentChinese(const std::vector<UChar32>& codepoints);
};

} // namespace doris::segment_v2::inverted_index
