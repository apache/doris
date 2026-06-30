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

#include "storage/index/snii/query/regexp_query.h"

#include <re2/re2.h>

#include <algorithm>
#include <iterator>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/snii/query/internal/regex_prefix.h"
#include "storage/index/snii/query/internal/term_expansion.h"

namespace doris::snii::query {

namespace {

bool is_regex_metachar(char c) {
    switch (c) {
    case '.':
    case '^':
    case '$':
    case '|':
    case '(':
    case ')':
    case '[':
    case ']':
    case '*':
    case '+':
    case '?':
    case '{':
    case '}':
    case '\\':
        return true;
    default:
        return false;
    }
}

std::string literal_prefix_for_regex(std::string_view pattern) {
    std::string out;
    size_t i = 0;
    if (!pattern.empty() && pattern.front() == '^') {
        i = 1;
    }
    for (; i < pattern.size(); ++i) {
        const char c = pattern[i];
        if (is_regex_metachar(c)) {
            break;
        }
        out.push_back(c);
    }
    return out;
}

} // namespace

namespace internal {

std::string regex_enum_prefix(std::string_view pattern, const re2::RE2& re) {
    // Left-anchored patterns can yield a tighter enumeration prefix via RE2's
    // PossibleMatchRange than the conservative literal scan (which stops at the
    // first metacharacter). The prefix only bounds how many dictionary terms are
    // enumerated; final acceptance is still decided by RE2::FullMatch, so an
    // over-wide prefix is always safe. Fall back to the literal scan otherwise.
    if (!pattern.empty() && pattern.front() == '^' && re.ok()) {
        std::string min_prefix;
        std::string max_prefix;
        if (re.PossibleMatchRange(&min_prefix, &max_prefix, 256) && !min_prefix.empty() &&
            !max_prefix.empty() && min_prefix.front() == max_prefix.front()) {
            const auto mismatch_pair = std::ranges::mismatch(min_prefix, max_prefix);
            const auto common_len =
                    static_cast<size_t>(std::distance(min_prefix.begin(), mismatch_pair.in1));
            if (common_len > 0) {
                return min_prefix.substr(0, common_len);
            }
        }
    }
    return literal_prefix_for_regex(pattern);
}

} // namespace internal

Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, int32_t max_expansions) {
    if (docids == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("regexp_query: null out");
    }
    docids->clear();
    VectorDocIdSink sink(*docids);
    return regexp_query(idx, pattern, &sink, max_expansions);
}

Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                    std::vector<uint32_t>* const docids, QueryProfile* profile,
                    int32_t max_expansions) {
    QueryProfileScope profile_scope(idx.reader(), profile);
    return regexp_query(idx, pattern, docids, max_expansions);
}

Status regexp_query(const reader::LogicalIndexReader& idx, std::string_view pattern,
                    DocIdSink* const sink, int32_t max_expansions) {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("regexp_query: null sink");
    }

    re2::RE2::Options options;
    options.set_log_errors(false); // Do not spam the BE log on user-supplied bad patterns.
    const re2::RE2 re(re2::StringPiece(pattern.data(), pattern.size()), options);
    if (!re.ok()) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                std::string("regexp_query: invalid regex: ") + re.error());
    }

    // RE2::FullMatch is anchored at both ends, matching std::regex_match whole-term
    // semantics. Unsupported constructs (backreferences, lookaround) fail re.ok()
    // above and return InvalidArgument rather than throwing.
    const std::string enum_prefix = internal::regex_enum_prefix(pattern, re);
    return internal::emit_expanded_docid_union(
            idx, enum_prefix,
            [&re](std::string_view term) {
                return re2::RE2::FullMatch(re2::StringPiece(term.data(), term.size()), re);
            },
            sink, max_expansions);
}

} // namespace doris::snii::query
