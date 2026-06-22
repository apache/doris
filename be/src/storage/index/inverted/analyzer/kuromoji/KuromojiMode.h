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

namespace doris::segment_v2 {

// Segmentation mode, mirroring Lucene's JapaneseTokenizer.Mode. Normal returns
// the minimum-cost segmentation. Search additionally decomposes long compounds
// into their shorter parts (via a length-based cost penalty) for better search
// recall. Extended currently behaves like Search (its extra step of splitting
// unknown words into unigrams is not implemented yet).
enum class KuromojiMode { Normal, Search, Extended };

inline KuromojiMode kuromoji_mode_from_string(const std::string& mode) {
    if (mode == "normal") {
        return KuromojiMode::Normal;
    }
    if (mode == "extended") {
        return KuromojiMode::Extended;
    }
    return KuromojiMode::Search; // default (matches OpenSearch/Lucene)
}

} // namespace doris::segment_v2
