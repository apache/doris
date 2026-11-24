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

#include "storage/index/inverted/query/exec/term_pattern.h"

#include <hs/hs.h>
#include <re2/re2.h>

#include "common/logging.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/query/logical/gram/regex_ast.h"
#include "util/hyperscan_util.h"

namespace doris::index_query {
namespace {

// Every string an anchored regular expression matches starts with the bytes the smallest and
// largest strings RE2 bounds its matches by share. A pattern without '^' may match inside a term.
std::string regexp_enumeration_prefix(std::string_view pattern) {
    if (!pattern.starts_with('^')) {
        return {};
    }
    re2::RE2::Options options;
    options.set_log_errors(false);
    const re2::RE2 re(re2::StringPiece(pattern.data(), pattern.size()), options);
    std::string smallest;
    std::string largest;
    if (!re.ok() || !re.PossibleMatchRange(&smallest, &largest, 256)) {
        return {};
    }
    auto length =
            static_cast<size_t>(std::ranges::mismatch(smallest, largest).in1 - smallest.begin());
    // Keep whole characters: the shared bytes may end inside one, which a format that converts
    // the prefix to wide characters cannot seek to.
    size_t lead = length;
    while (lead > 0 && (static_cast<uint8_t>(smallest[lead - 1]) & 0xC0) == 0x80) {
        --lead;
    }
    if (lead > 0 && lead - 1 + utf8_sequence_length(smallest[lead - 1]) > length) {
        length = lead - 1;
    }
    smallest.resize(length);
    return smallest;
}

// The longest literal every match of a regular expression holds: the consecutive literal code
// points of a concatenation, through anchors (which take no text) and into a group repeated at
// least once; a class, `.`, an alternation and a quantifier that may take nothing end the run.
// Under `(?i)` the parser turns letters into classes, so they name no literal.
void collect_regexp_literal(const segment_v2::gram::RegexNode& node, std::string* run,
                            std::string* longest) {
    using Type = segment_v2::gram::RegexNode::Type;
    const auto end_run = [&] {
        if (run->size() > longest->size()) {
            *longest = *run;
        }
        run->clear();
    };
    switch (node.type) {
    case Type::EMPTY:
        return;
    case Type::LIT:
        run->append(node.lit);
        return;
    case Type::CAT:
        for (const auto& kid : node.kids) {
            collect_regexp_literal(*kid, run, longest);
        }
        return;
    case Type::PLUS:
    case Type::REPEAT:
        end_run();
        if (node.type == Type::PLUS || node.rmin >= 1) {
            std::string inner;
            for (const auto& kid : node.kids) {
                collect_regexp_literal(*kid, &inner, longest);
            }
            if (inner.size() > longest->size()) {
                *longest = inner;
            }
        }
        return;
    case Type::CLASS:
    case Type::ANY:
    case Type::ALT:
    case Type::STAR:
    case Type::QUEST:
        end_run();
        return;
    }
}

// Empty for a pattern the parser does not accept: the expansion then tests every term.
std::string regexp_required_literal(std::string_view pattern) {
    std::unique_ptr<segment_v2::gram::RegexNode> root;
    bool case_insensitive = false;
    if (!segment_v2::gram::parse_regex(pattern, &root, &case_insensitive).ok()) {
        return {};
    }
    std::string run;
    std::string longest;
    collect_regexp_literal(*root, &run, &longest);
    return run.size() > longest.size() ? run : longest;
}

int on_match(unsigned int /*id*/, unsigned long long /*from*/, unsigned long long /*to*/,
             unsigned int /*flags*/, void* context) {
    *static_cast<bool*>(context) = true;
    return 0;
}

} // namespace

void TermPattern::HyperscanDeleter::operator()(hs_database* database) const {
    hs_free_database(database);
}

void TermPattern::HyperscanDeleter::operator()(hs_scratch* scratch) const {
    hs_free_scratch(scratch);
}

Status TermPattern::create(TermPatternKind kind, std::string_view pattern, TermPattern* out) {
    out->_kind = kind;
    if (kind == TermPatternKind::kPrefix) {
        out->_enumeration_prefix = pattern;
        return Status::OK();
    }
    if (kind == TermPatternKind::kSuffix || kind == TermPatternKind::kContains) {
        // Such terms can start with anything, so the whole dictionary is enumerated.
        out->_text = pattern;
        return Status::OK();
    }
    if (kind == TermPatternKind::kWildcard) {
        out->_wildcard.emplace(pattern);
        if (!out->_wildcard->pattern_valid()) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "wildcard pattern is not valid UTF-8");
        }
        out->_enumeration_prefix = pattern.substr(0, pattern.find_first_of("*?"));
        return Status::OK();
    }
    if (is_hyperscan_regexp_expensive(pattern)) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(HYPERSCAN_BOUNDED_REPEAT_ERROR);
    }
    const std::string terminated(pattern);
    hs_database_t* database = nullptr;
    hs_compile_error_t* error = nullptr;
    if (hs_compile(terminated.c_str(), HS_FLAG_DOTALL | HS_FLAG_ALLOWEMPTY | HS_FLAG_UTF8,
                   HS_MODE_BLOCK, nullptr, &database, &error) != HS_SUCCESS) {
        // An invalid regular expression matches no term.
        hs_free_compile_error(error);
        out->_can_match = false;
        return Status::OK();
    }
    out->_database.reset(database);
    hs_scratch_t* scratch = nullptr;
    if (hs_alloc_scratch(database, &scratch) != HS_SUCCESS) {
        return Status::MemoryAllocFailed("cannot allocate Hyperscan scratch");
    }
    out->_scratch.reset(scratch);
    // Hyperscan reads the expression as a C string, so a raw NUL ends it. The prefix and the
    // literal come from that same text, or they would skip terms the expression matches.
    const std::string_view compiled(terminated.c_str());
    out->_enumeration_prefix = regexp_enumeration_prefix(compiled);
    // As for a glob: an expression with a prefix already stops where the prefix ends, and CLucene
    // checks the prefix only after this text, so '^cach.' would walk on past "cach" through every
    // later term that lacks "cach". Only an expression that enumerates the whole dictionary keeps it.
    if (out->_enumeration_prefix.empty()) {
        out->_text = regexp_required_literal(compiled);
    }
    return Status::OK();
}

bool TermPattern::matches(std::string_view term) {
    if (_kind == TermPatternKind::kSuffix) {
        return term.ends_with(_text);
    }
    if (_kind == TermPatternKind::kContains) {
        return term.find(_text) != std::string_view::npos;
    }
    if (_kind == TermPatternKind::kWildcard) {
        return (*_wildcard)(term);
    }
    if (_kind == TermPatternKind::kRegexp) {
        bool matched = false;
        const hs_error_t status =
                hs_scan(_database.get(), term.data(), static_cast<unsigned int>(term.size()), 0,
                        _scratch.get(), on_match, &matched);
        DCHECK_EQ(status, HS_SUCCESS);
        return matched;
    }
    // Enumeration visits only terms that start with the prefix.
    return true;
}

int32_t max_expansions(const RuntimeState* runtime_state) {
    return runtime_state == nullptr ? 50
                                    : runtime_state->query_options().inverted_index_max_expansions;
}

int32_t max_expansions(const segment_v2::IndexQueryContext& context) {
    return max_expansions(context.runtime_state);
}

int32_t expansion_limit(TermPatternKind kind, int32_t max_expansions) {
    return kind == TermPatternKind::kContains ? 0 : max_expansions;
}

} // namespace doris::index_query
