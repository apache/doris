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

#include <parallel_hashmap/phmap.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include "common/status.h"

namespace doris::segment_v2::inverted_index {

inline constexpr std::string_view BUILTIN_COMMON_WORDS_RESOURCE = "builtin:lucene_english_stop:v1";
inline constexpr std::string_view WORDSET_FORMAT_V1 = "wordset:v1";

class CommonWordSet {
public:
    static const CommonWordSet& builtin_english_stop_words_v1();

    // Parses a newline-separated word list. Blank lines and '#' comments are skipped.
    static Result<CommonWordSet> parse_words(std::string_view content);

    // The BE-local word list every CommonGrams analyzer grams against, read once from
    // <inverted_index_dict_path>/common_grams/default_words.txt -- the same layout the icu, ik and
    // pinyin dictionaries use. Falls back to builtin_english_stop_words_v1() when the file is
    // absent or unparseable. The set is deliberately not selectable per index policy: every replica
    // of a tablet must gram identically, and a policy-supplied list would have to be distributed
    // and acknowledged before any index could use it.
    static std::shared_ptr<const CommonWordSet> default_word_set();

    // Exposed so tests can assert the layout without duplicating the literal.
    static std::string default_word_set_path();

    // Stamped into a segment's CommonGrams metadata and compared against the querying analyzer's
    // identity, so a segment is only read with gram expectations that match how it was written.
    // Because the word list is now a BE-local file, this MUST derive from the content: two BEs
    // pointed at different files, or one BE after the file is edited, would otherwise stamp the
    // same identity onto incompatible segments and silently mis-plan phrase queries.
    const std::string& identity() const { return _identity; }

    bool contains(std::string_view term) const;
    size_t size() const { return _words.size(); }

private:
    struct TransparentStringHash {
        using is_transparent = void;

        size_t operator()(std::string_view value) const {
            return std::hash<std::string_view> {}(value);
        }
    };

    using WordContainer = phmap::flat_hash_set<std::string, TransparentStringHash, std::equal_to<>>;

    CommonWordSet(WordContainer words, std::string identity);

    WordContainer _words;
    // Never empty. See identity() for why this is content-derived rather than a fixed constant.
    std::string _identity;
    std::array<uint64_t, 4> _first_byte_mask {};
    uint16_t _min_word_bytes = std::numeric_limits<uint16_t>::max();
    uint16_t _max_word_bytes = 0;
};

#ifdef BE_TEST
namespace common_grams_testing {
uint64_t common_word_membership_lookup_count();
void reset_common_word_membership_lookup_count();
} // namespace common_grams_testing
#endif

} // namespace doris::segment_v2::inverted_index
