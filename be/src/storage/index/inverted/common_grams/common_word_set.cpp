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

#include "storage/index/inverted/common_grams/common_word_set.h"

#include <algorithm>
#include <atomic>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "util/md5.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {
namespace {

#ifdef BE_TEST
std::atomic<uint64_t> g_common_word_membership_lookups {0};
std::atomic<uint64_t> g_common_word_hash_lookups {0};
#endif

ResultError wordset_error(std::string_view message) {
    return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>("{}", message));
}

} // namespace

CommonWordSet::CommonWordSet(WordContainer words, std::string identity)
        : _words(std::move(words)), _identity(std::move(identity)) {
    DORIS_CHECK(!_identity.empty());
    for (const std::string& word : _words) {
        DORIS_CHECK(!word.empty());
        DORIS_CHECK_LE(word.size(), std::numeric_limits<uint16_t>::max());
        const auto bytes = static_cast<uint16_t>(word.size());
        _min_word_bytes = std::min(_min_word_bytes, bytes);
        _max_word_bytes = std::max(_max_word_bytes, bytes);
        const uint8_t first = static_cast<uint8_t>(word.front());
        _first_byte_mask[first >> 6] |= uint64_t {1} << (first & 63);
    }
}

const CommonWordSet& CommonWordSet::builtin_english_stop_words_v1() {
    static const CommonWordSet words(
            WordContainer {
                    "a",    "an",   "and",  "are",  "as",   "at",    "be",   "but",   "by",
                    "for",  "if",   "in",   "into", "is",   "it",    "no",   "not",   "of",
                    "on",   "or",   "such", "that", "the",  "their", "then", "there", "these",
                    "they", "this", "to",   "was",  "will", "with",
            },
            std::string(BUILTIN_COMMON_WORDS_RESOURCE));
    return words;
}

Result<CommonWordSet> CommonWordSet::parse_words(std::string_view content) {
    // Digest first: the parse loop below advances `content`, so hashing it afterwards would hash
    // an empty view. Content-derived so a segment records exactly which list grammed it, and
    // digesting the raw bytes (not the parsed set) means a comment-only edit also yields a new
    // identity -- the safe direction, since that re-plans instead of risking a stale match.
    Md5Digest digest;
    digest.update(content.data(), content.size());
    digest.digest();
    const std::string identity = "wordset:md5:" + digest.hex();

    if (content.find('\0') != std::string_view::npos) {
        return wordset_error("CommonGrams word list contains NUL");
    }
    if (!validate_utf8(content.data(), content.size())) {
        return wordset_error("CommonGrams word list is not valid UTF-8");
    }

    WordContainer words;
    while (!content.empty()) {
        const size_t newline = content.find('\n');
        std::string_view term = content.substr(0, newline);
        if (newline != std::string_view::npos && !term.empty() && term.back() == '\r') {
            term.remove_suffix(1);
        }
        if (!term.empty() && term.front() != '#') {
            if (term.size() > COMMON_GRAM_MAX_ENCODED_BYTES) {
                return wordset_error("wordset:v1 term exceeds the 16383-byte token limit");
            }
            words.emplace(term);
        }
        if (newline == std::string_view::npos) {
            break;
        }
        content.remove_prefix(newline + 1);
    }
    return CommonWordSet(std::move(words), identity);
}

std::string CommonWordSet::default_word_set_path() {
    return config::inverted_index_dict_path + "/common_grams/default_words.txt";
}

std::shared_ptr<const CommonWordSet> CommonWordSet::default_word_set() {
    // Lazy singleton: the word list is immutable for the process, so every analyzer shares one
    // copy and the fallback warning below is emitted at most once no matter how many analyzers
    // are built. A later edit to inverted_index_dict_path is therefore ignored, which is intended
    // -- swapping the list under live segments would change what their grams mean.
    static const std::shared_ptr<const CommonWordSet> instance = [] {
        auto builtin = [] {
            return std::shared_ptr<const CommonWordSet>(&builtin_english_stop_words_v1(),
                                                        [](const CommonWordSet*) {});
        };
        const std::string path = default_word_set_path();
        std::ifstream input(path, std::ios::binary);
        if (!input.is_open()) {
            // Expected on a stock install: shipping no file means "use the built-in list", so this
            // is INFO. A file that exists but cannot be read or parsed is an operator mistake and
            // warns below.
            LOG(INFO) << "No CommonGrams word list at " << path
                      << ", using the built-in English stop words";
            return builtin();
        }
        std::ostringstream buffer;
        buffer << input.rdbuf();
        if (input.bad()) {
            LOG(WARNING) << "CommonGrams word list at " << path
                         << " could not be read, falling back to the built-in English stop words";
            return builtin();
        }
        const std::string content = buffer.str();
        auto parsed = parse_words(content);
        if (!parsed.has_value()) {
            LOG(WARNING) << "CommonGrams word list at " << path << " is invalid: " << parsed.error()
                         << ", falling back to the built-in English stop words";
            return builtin();
        }
        LOG(INFO) << "Loaded " << parsed.value().size() << " CommonGrams words from " << path;
        return std::shared_ptr<const CommonWordSet>(
                std::make_shared<const CommonWordSet>(std::move(parsed.value())));
    }();
    return instance;
}

bool CommonWordSet::contains(std::string_view term) const {
#ifdef BE_TEST
    g_common_word_membership_lookups.fetch_add(1, std::memory_order_relaxed);
#endif
    if (term.size() < _min_word_bytes || term.size() > _max_word_bytes) {
        return false;
    }
    const auto first = static_cast<uint8_t>(term.front());
    if ((_first_byte_mask[first >> 6] & (uint64_t {1} << (first & 63))) == 0) {
        return false;
    }
#ifdef BE_TEST
    g_common_word_hash_lookups.fetch_add(1, std::memory_order_relaxed);
#endif
    return _words.contains(term);
}

#ifdef BE_TEST
namespace common_grams_testing {

uint64_t common_word_membership_lookup_count() {
    return g_common_word_membership_lookups.load(std::memory_order_relaxed);
}

void reset_common_word_membership_lookup_count() {
    g_common_word_membership_lookups.store(0, std::memory_order_relaxed);
}

uint64_t common_word_hash_lookup_count() {
    return g_common_word_hash_lookups.load(std::memory_order_relaxed);
}

void reset_common_word_hash_lookup_count() {
    g_common_word_hash_lookups.store(0, std::memory_order_relaxed);
}

} // namespace common_grams_testing
#endif

} // namespace doris::segment_v2::inverted_index
