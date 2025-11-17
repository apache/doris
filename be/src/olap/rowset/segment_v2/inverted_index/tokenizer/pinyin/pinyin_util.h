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

#include <memory>
#include <string>
#include <vector>

#include "pinyin_format.h"
#include "smart_forest.h"
#include "unicode/uchar.h"

namespace doris::segment_v2::inverted_index {

class PinyinUtil {
public:
    static PinyinUtil& instance();

    std::string to_pinyin(uint32_t cp) const;

    std::vector<std::string> convert(const std::vector<UChar32>& codepoints,
                                     const PinyinFormat& format) const;

    void insertPinyin(const std::string& word, const std::vector<std::string>& pinyins);

private:
    PinyinUtil();
    void load_pinyin_mapping();
    void load_polyphone_mapping();

    std::vector<std::string> convert_with_raw_pinyin(const std::string& text) const;

    std::string to_raw_pinyin(uint32_t cp) const;

    std::vector<std::string> _pinyin_dict;
    std::unique_ptr<PolyphoneForest> polyphone_dict_;
    int max_polyphone_len_;
};

} // namespace doris::segment_v2::inverted_index
