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

#ifndef DORIS_ITOKEN_EXTRACTOR_H
#define DORIS_ITOKEN_EXTRACTOR_H

#include <stddef.h>

#include <stack>
#include <string>

#include "common/logging.h"
#include "olap/inverted_index_parser.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "util/unicode.h"

namespace doris {
class Trie;
/// Interface for string parsers.
struct ITokenExtractor {
    virtual ~ITokenExtractor() = default;

    /// Fast inplace implementation for regular use.
    /// Gets string (data ptr and len) and start position for extracting next token (state of extractor).
    /// Returns false if parsing is finished, otherwise returns true.
    virtual bool next_in_string(const char* data, size_t length, size_t* __restrict pos,
                                size_t* __restrict token_start,
                                size_t* __restrict token_length) const = 0;

    /// Special implementation for creating bloom filter for LIKE function.
    /// It skips unescaped `%` and `_` and supports escaping symbols, but it is less lightweight.
    virtual bool next_in_string_like(const char* data, size_t length, size_t* pos,
                                     std::string& out) const = 0;

    virtual void string_to_bloom_filter(const char* data, size_t length,
                                        segment_v2::BloomFilter& bloom_filter) const = 0;

    virtual bool string_like_to_bloom_filter(const char* data, size_t length,
                                             segment_v2::BloomFilter& bloom_filter) const = 0;
};

template <typename Derived>
class ITokenExtractorHelper : public ITokenExtractor {
public:
    void string_to_bloom_filter(const char* data, size_t length,
                                segment_v2::BloomFilter& bloom_filter) const override {
        size_t cur = 0;
        size_t token_start = 0;
        size_t token_len = 0;

        while (cur < length && static_cast<const Derived*>(this)->next_in_string(
                                       data, length, &cur, &token_start, &token_len))
            bloom_filter.add_bytes(data + token_start, token_len);
    }

    bool string_like_to_bloom_filter(const char* data, size_t length,
                                     segment_v2::BloomFilter& bloom_filter) const override {
        size_t cur = 0;
        bool added = false;
        std::string token;
        while (cur < length &&
               static_cast<const Derived*>(this)->next_in_string_like(data, length, &cur, token)) {
            bloom_filter.add_bytes(token.data(), token.size());
            added = true;
        }

        return added;
    }
};

/// Parser extracting all ngrams from string.
class NgramTokenExtractor final : public ITokenExtractorHelper<NgramTokenExtractor> {
public:
    explicit NgramTokenExtractor(size_t n_) : n(n_) {}

    bool next_in_string(const char* data, size_t length, size_t* __restrict pos,
                        size_t* __restrict token_start,
                        size_t* __restrict token_length) const override;

    bool next_in_string_like(const char* data, size_t length, size_t* pos,
                             std::string& token) const override;

private:
    size_t n;
};

class AlphaNumTokenExtractor final : public ITokenExtractorHelper<AlphaNumTokenExtractor> {
public:
    AlphaNumTokenExtractor() = default;
    bool next_in_string(const char* data, size_t length, size_t* __restrict pos,
                        size_t* __restrict token_start,
                        size_t* __restrict token_length) const override;

    bool next_in_string_like(const char* data, size_t length, size_t* pos,
                             std::string& token) const override;
};

struct ChineseTokenDict {
    Trie* trie;
    std::vector<Unicode> static_node_infos;

    explicit ChineseTokenDict(const std::string& dict_path);
    std::vector<Unicode> load_dict(const std::string& file_path);
};

template <InvertedIndexParserMode parser_mode>
class ChineseTokenExtractor final
        : public ITokenExtractorHelper<ChineseTokenExtractor<parser_mode>> {
public:
    explicit ChineseTokenExtractor(const ChineseTokenDict* dict) : _dict(dict) {};
    bool next_in_string(const char* data, size_t length, size_t* __restrict pos,
                        size_t* __restrict token_start,
                        size_t* __restrict token_length) const override;

    bool next_in_string_like(const char* data, size_t length, size_t* pos,
                             std::string& token) const override {
        LOG_FATAL("next_in_string_like is not implemented in ChineseTokenExtractor");
        __builtin_unreachable();
    };
    bool cut(const char* sentence, std::string_view& word) const;
    bool cut_all(const char* sentence, std::string_view& word) const;
    void find(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
              vector<WordRange>& res) const;
    void reset() const {
        _runes.clear();
        _currentIter = _runes.end();
        _endIter = _runes.end();
        _matchedWords.clear();
        _matchedWordIndex = 0;
    };

private:
    mutable RuneStrArray _runes;
    mutable RuneStrArray::const_iterator _currentIter;
    mutable RuneStrArray::const_iterator _endIter;
    mutable std::vector<WordRange> _matchedWords;
    mutable size_t _matchedWordIndex = 0;
    const ChineseTokenDict* _dict;
};

} // namespace doris

#endif //DORIS_ITOKEN_EXTRACTOR_H
