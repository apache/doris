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

#include <algorithm>
#include <string_view>

namespace doris::snii::format {

inline constexpr std::string_view kPhraseBigramTermMarker =
        "\x1F"
        "SNII_PHRASE_BIGRAM"
        "\x1F";

inline bool is_phrase_bigram_term(std::string_view term) {
    return term.starts_with(kPhraseBigramTermMarker);
}

// SNII 的 term 键就是分词后的原始字节，没有任何转义。唯一的内部命名空间是上面这个以 \x1F
// 开头的 phrase-bigram 标记：用户 term（或前缀展开的前缀）若与它重叠，查询必须绕过 SNII，
// 否则用户词项会命中内部词项。
inline bool term_overlaps_internal_namespace(std::string_view term) {
    return term.starts_with(kPhraseBigramTermMarker);
}

inline bool prefix_overlaps_internal_namespace(std::string_view prefix) {
    const size_t common = std::min(prefix.size(), kPhraseBigramTermMarker.size());
    return prefix.substr(0, common) == kPhraseBigramTermMarker.substr(0, common);
}

} // namespace doris::snii::format
