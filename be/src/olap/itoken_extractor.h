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

#include <string>

#include "olap/rowset/segment_v2/bloom_filter.h"

namespace doris {

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
struct NgramTokenExtractor final : public ITokenExtractorHelper<NgramTokenExtractor> {
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
} // namespace doris

#endif //DORIS_ITOKEN_EXTRACTOR_H
