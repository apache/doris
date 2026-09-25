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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"
#include "util/utf8_check.h"

struct hs_database;
struct hs_scratch;

namespace doris::segment_v2 {
struct IndexQueryContext;
} // namespace doris::segment_v2

namespace doris::index_query {

// The byte length of the UTF-8 sequence `lead` starts.
inline size_t utf8_sequence_length(char lead) {
    const auto byte = static_cast<uint8_t>(lead);
    if (byte < 0x80) {
        return 1;
    }
    if (byte < 0xE0) {
        return 2;
    }
    if (byte < 0xF0) {
        return 3;
    }
    return 4;
}

// UTF-8 glob matcher. '*' matches zero or more code points, '?' exactly one, and every other
// code point itself; the whole term has to match. A term that is not valid UTF-8 is matched byte
// by byte, since keyword indexes may hold arbitrary bytes.
class WildcardMatcher {
public:
    explicit WildcardMatcher(std::string_view pattern)
            : _pattern(pattern),
              _pattern_valid(is_valid_utf8(pattern)),
              // ASCII literals and '*' match the same text bytes whether the text is read as code
              // points or bytes, so only '?' and non-ASCII literals need the text validated.
              _needs_code_points(pattern.find('?') != std::string_view::npos ||
                                 !std::ranges::all_of(pattern, [](char c) {
                                     return static_cast<uint8_t>(c) < 0x80;
                                 })) {}

    bool operator()(std::string_view text) const {
        if (!_pattern_valid) {
            return false;
        }
        const bool code_points = _needs_code_points && is_valid_utf8(text);
        const auto width = [code_points](char lead) {
            return code_points ? utf8_sequence_length(lead) : size_t {1};
        };
        size_t p = 0;
        size_t t = 0;
        // Where the pattern resumes after the last '*', and where that '*' stopped absorbing.
        size_t star_p = std::string_view::npos;
        size_t star_t = 0;
        while (t < text.size()) {
            const size_t text_width = width(text[t]);
            if (p < _pattern.size() && _pattern[p] == '*') {
                star_p = ++p;
                star_t = t;
            } else if (p < _pattern.size() && _pattern[p] == '?') {
                ++p;
                t += text_width;
            } else if (p < _pattern.size() && width(_pattern[p]) == text_width &&
                       std::memcmp(_pattern.data() + p, text.data() + t, text_width) == 0) {
                p += text_width;
                t += text_width;
            } else if (star_p != std::string_view::npos) {
                // The last '*' absorbs one more character and the rest is matched again.
                p = star_p;
                star_t += width(text[star_t]);
                t = star_t;
            } else {
                return false;
            }
        }
        while (p < _pattern.size() && _pattern[p] == '*') {
            ++p;
        }
        return p == _pattern.size();
    }

    bool pattern_valid() const { return _pattern_valid; }

private:
    static bool is_valid_utf8(std::string_view text) {
        return text.empty() || validate_utf8(text.data(), text.size());
    }

    std::string _pattern;
    bool _pattern_valid = false;
    bool _needs_code_points = false;
};

enum class TermPatternKind : uint8_t { kPrefix, kWildcard, kRegexp, kSuffix, kContains };

// The dictionary terms a PREFIX, WILDCARD or REGEXP clause, or an edge of a phrase, expands to:
// kSuffix takes the terms that end with the text and kContains the terms that hold it anywhere.
// Every term the pattern matches starts with enumeration_prefix(), so a format enumerates its
// dictionary in order from there and stops at the first term that does not start with it.
class TermPattern {
public:
    // Fails for a glob that is not valid UTF-8 and for a regular expression Hyperscan runs slowly.
    static Status create(TermPatternKind kind, std::string_view pattern, TermPattern* out);

    const std::string& enumeration_prefix() const { return _enumeration_prefix; }

    // Text every matching term holds, empty when the pattern names none. A format may skip the
    // terms without it before decoding them.
    const std::string& required_text() const { return _text; }

    // False when no term can match, as for a regular expression Hyperscan cannot compile.
    bool can_match() const { return _can_match; }

    // Whether a term that starts with the enumeration prefix matches. A regular expression
    // matches anywhere in a term unless the pattern anchors it.
    bool matches(std::string_view term);

private:
    struct HyperscanDeleter {
        void operator()(hs_database* database) const;
        void operator()(hs_scratch* scratch) const;
    };

    TermPatternKind _kind = TermPatternKind::kPrefix;
    std::string _enumeration_prefix;
    std::string _text;
    bool _can_match = true;
    std::optional<WildcardMatcher> _wildcard;
    std::unique_ptr<hs_database, HyperscanDeleter> _database;
    std::unique_ptr<hs_scratch, HyperscanDeleter> _scratch;
};

// The number of terms a pattern may expand to, 0 for no limit: the session's
// inverted_index_max_expansions, or 50 when the query has no runtime state.
int32_t max_expansions(const segment_v2::IndexQueryContext& context);

// The number of terms a pattern of `kind` may expand to under a session limit of
// `max_expansions`, 0 for no limit: a contains pattern takes every term that holds its text.
int32_t expansion_limit(TermPatternKind kind, int32_t max_expansions);

} // namespace doris::index_query
