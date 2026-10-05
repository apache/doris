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

#include <hs/hs.h>
#include <re2/re2.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "storage/index/snii/query/internal/regex_prefix.h"
#include "storage/index/snii/query/internal/term_expansion.h"

namespace doris::snii::query {

namespace {

template <typename Deleter, Deleter deleter>
struct HyperscanDeleter {
    template <typename T>
    void operator()(T* ptr) const {
        deleter(ptr);
    }
};

using HyperscanDatabasePtr =
        std::unique_ptr<hs_database_t,
                        HyperscanDeleter<decltype(&hs_free_database), &hs_free_database>>;
using HyperscanScratchPtr =
        std::unique_ptr<hs_scratch_t,
                        HyperscanDeleter<decltype(&hs_free_scratch), &hs_free_scratch>>;
using HyperscanCompileErrorPtr =
        std::unique_ptr<hs_compile_error_t,
                        HyperscanDeleter<decltype(&hs_free_compile_error), &hs_free_compile_error>>;

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
    // enumerated. Unanchored patterns cannot use a prefix because Hyperscan may
    // match the pattern after the beginning of a dictionary term.
    if (pattern.empty() || pattern.front() != '^') {
        return {};
    }

    if (re.ok()) {
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

    const std::string compiled_pattern(pattern);
    hs_database_t* raw_database = nullptr;
    hs_compile_error_t* raw_compile_error = nullptr;
    const auto compile_status =
            hs_compile(compiled_pattern.c_str(), HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                       HS_MODE_BLOCK, nullptr, &raw_database, &raw_compile_error);
    HyperscanCompileErrorPtr compile_error(raw_compile_error);
    if (compile_status != HS_SUCCESS) {
        return Status::OK();
    }
    HyperscanDatabasePtr database(raw_database);

    hs_scratch_t* raw_scratch = nullptr;
    if (hs_alloc_scratch(database.get(), &raw_scratch) != HS_SUCCESS) {
        return Status::MemoryAllocFailed("regexp_query: failed to allocate Hyperscan scratch");
    }
    HyperscanScratchPtr scratch(raw_scratch);

    std::string enum_prefix;
    if (!pattern.empty() && pattern.front() == '^') {
        re2::RE2::Options options;
        options.set_log_errors(false);
        const re2::RE2 re(re2::StringPiece(pattern.data(), pattern.size()), options);
        enum_prefix = internal::regex_enum_prefix(pattern, re);
    }
    const auto on_match = [](unsigned int, unsigned long long, unsigned long long, unsigned int,
                             void* context) -> int {
        *static_cast<bool*>(context) = true;
        return 0;
    };
    return internal::emit_expanded_docid_union(
            idx, enum_prefix,
            [&database, &scratch, on_match](std::string_view term) {
                bool matched = false;
                const auto scan_status =
                        hs_scan(database.get(), term.data(), static_cast<uint32_t>(term.size()), 0,
                                scratch.get(), on_match, &matched);
                DCHECK_EQ(scan_status, HS_SUCCESS);
                return matched;
            },
            sink, max_expansions);
}

} // namespace doris::snii::query
