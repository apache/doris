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
#include <cstddef>
#include <string_view>

#include "common/status.h"
#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/gram_query.h"
#include "storage/index/inverted/gram/gram_scheme.h"

namespace doris::segment_v2::gram {

// Compiles a regex / LIKE pattern into a boolean query over grams (Cox's "five-tuple"
// derivation: can_empty / exact / prefix / suffix / match).
//
// The one hard invariant is "false positives are fine, false negatives are not": for any row r,
// if the pattern matches r then the compiled GramQuery is necessarily true over "the gram set
// extracted from r with the same GramScheme". So anywhere the derivation cannot be made reliably
// (a parse failure, an oversized character class, a set explosion, a literal shorter than one
// gram, ...) it degrades to ALL (filtering no row at all) and never emits a stronger condition
// that could filter a matching row away.
//
// The compiler only folds literals into grams and makes no regex-semantic decisions: the real
// row-level matching is still done by the regexp / like expression above it, and this query only
// serves to skip rows that cannot possibly match.
//
// An instance holds GramExtractor's internal buffers, so both compile_* methods are non-const
// and not thread-safe: every thread that uses one constructs its own (construction only computes
// an 8 KB boundary bitmap).
class RegexGramCompiler {
public:
    explicit RegexGramCompiler(const GramScheme& scheme);

    // Compile a regex. Any parse or derivation failure returns OK with *out = ALL (a
    // conservative fall back to a full scan); only an internal assertion failure returns non-OK.
    Status compile_regexp(std::string_view pattern, GramQuery* out);

    // Compile a LIKE pattern: cut it into literal segments at % and _, AND the grams of each
    // segment together and then AND the segments; an all-wildcard pattern (no literal segment)
    // yields ALL.
    //
    // The escape character is fixed to `\`, and only `\%`, `\_` and `\\` count as escapes (which
    // matches Doris LIKE's actual semantics); the escaped character joins the current segment as
    // a literal. For any other `\x` (x not one of those three) it is undecidable whether the
    // engine keeps the backslash itself, so under either reading the segment is conservatively
    // cut at the backslash and no gram spans that point, while x still starts the next segment
    // and takes part in the following literal (this only loses a little pruning power and can
    // never filter a match away); a lone trailing `\` likewise cuts the segment and is ignored.
    // The `ESCAPE` clause is not supported: the literal/wildcard split here always assumes `\`
    // is the escape character, so a caller facing a custom escape character other than `\` must
    // skip the index -- otherwise the derived boundaries would disagree with the engine's actual
    // semantics and could filter genuinely matching rows away.
    //
    // ILIKE semantics are decided by the caller through scheme.lower_case (index and query fold
    // together).
    Status compile_like(std::string_view like_pattern, GramQuery* out);

    // Size cap of the exact/prefix/suffix sets: a Cartesian product or union larger than this is
    // demoted, which keeps the enumeration from exploding.
    static constexpr size_t kMaxSet = 20;
    // An exact set larger than this is demoted to prefix/suffix (after folding the grams it
    // already yields into match).
    static constexpr size_t kMaxExact = 7;

private:
    // The extractor also owns the scheme (_extractor.scheme()), so the two configurations cannot
    // disagree.
    GramExtractor _extractor;
};

} // namespace doris::segment_v2::gram
