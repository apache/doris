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
#include <string_view>
#include <vector>

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dictionary.h"

namespace doris::segment_v2::kuromoji {

// One morpheme on the best path. `surface` is identified by byte range into the
// analyzed input; `known` distinguishes a system-dictionary word from a
// synthesized unknown word, and `word_id` indexes the system or unknown entries
// (so callers can fetch features / base forms in later phases).
struct KuromojiMorpheme {
    uint32_t byte_start = 0;
    uint32_t byte_len = 0;
    bool known = false;
    uint32_t word_id = 0;
};

// Viterbi morphological segmenter. It builds a lattice over the input (known
// words from the system dictionary via common-prefix search, unknown words from
// the character-category rules) and returns the minimum-cost path, where cost is
// the sum of word costs and connection costs. It segments in normal mode;
// compound decomposition (search mode) is not implemented.
class KuromojiViterbi {
public:
    explicit KuromojiViterbi(const KuromojiDictionary& dict) : _dict(dict) {}

    void segment(std::string_view text, std::vector<KuromojiMorpheme>* out) const;

private:
    const KuromojiDictionary& _dict;
};

} // namespace doris::segment_v2::kuromoji
