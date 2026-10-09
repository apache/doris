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

#include "common/exception.h"

namespace doris::segment_v2 {

// Segmentation mode, mirroring Lucene's JapaneseTokenizer.Mode. Normal returns
// the minimum-cost segmentation. Search additionally decomposes long compounds
// into their shorter parts (via a length-based cost penalty) for better search
// recall. Extended applies the Search penalty and also splits unknown
// (out-of-vocabulary) words into per-character unigrams.
enum class KuromojiMode { Normal, Search, Extended };

inline KuromojiMode kuromoji_mode_from_string(const std::string& mode) {
    if (mode.empty() || mode == "search") {
        return KuromojiMode::Search;
    }
    if (mode == "normal") {
        return KuromojiMode::Normal;
    }
    if (mode == "extended") {
        return KuromojiMode::Extended;
    }
    throw doris::Exception(doris::ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                           "Invalid kuromoji parser_mode: '{}', must be search, normal or extended",
                           mode);
}

} // namespace doris::segment_v2
