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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"

namespace doris::segment_v2::gram {

// A node of the regex syntax tree parsed out of the RE2 syntax subset. The tree keeps only what
// the gram compiler (Task 5: RegexGramCompiler) needs; it is not a full regex-engine AST:
//   - LIT: the UTF-8 encoding of one code point; a multi-byte literal (CJK, say) stays one LIT
//     for the whole code point and is never split down to bytes;
//   - CLASS: a small class of <= 4 code points is expanded into cls (sorted, deduplicated);
//     big_class=true marks a "non-enumerable" large class, negated class,
//     `\d \w \s \D \W \S \pL \p{..}`, POSIX class and the like, and cls is then empty;
//   - EMPTY: a node that adds no literal constraint (`^ $ \b \B \A \z`, a flags-only group, ...);
//   - CAT/ALT/STAR/PLUS/QUEST: operator nodes whose semantics match the regex syntax, with
//     children hanging off kids;
//   - REPEAT: the bounded quantifier `{m}` / `{m,}` / `{m,n}`; rmax=-1 means no upper bound
//     (`{m,}`).
// Once the `(?i)` flag is on, a literal or class item that is an ASCII letter expands into both
// cases as two code points ('a' produces CLASS{'A','a'}), so that a caller matching
// case-insensitively can still fold literals into a gram query safely.
struct RegexNode {
    enum class Type : uint8_t { EMPTY, LIT, CLASS, ANY, CAT, ALT, STAR, PLUS, QUEST, REPEAT };
    Type type = Type::EMPTY;
    std::string lit;              // LIT: the UTF-8 of one code point
    std::vector<std::string> cls; // CLASS: <= 4 code points; empty + big_class = large/negated
    bool big_class = false;
    std::vector<std::unique_ptr<RegexNode>> kids;
    int rmin = 0, rmax = -1; // REPEAT
};

// Parse a pattern in the RE2 syntax subset into a RegexNode AST. Supported: literals; escapes
// (`\. \n \t \r \xHH \x{...} \Q..\E`); classes (`[...]`, negation, ranges, POSIX classes,
// `\d \w \s \D \W \S \pL \p{..}`); `.`; groups (capturing, `(?:`, `(?P<name>`, `(?<name>`);
// the flags `(?i) (?s) (?m) (?U)` and `(?i:...)`; the quantifiers `* + ? {m} {m,} {m,n}` and
// their lazy suffixes; the anchors `^ $ \b \B \A \z`; `|`.
// On success *root owns the whole tree and *case_insensitive says whether `(?i)` appeared in
// the pattern; on failure (syntax error, unclosed group/class, dangling escape, group nesting
// too deep, ...) Status::InvalidArgument is returned and neither *root nor *case_insensitive is
// guaranteed to have been written, so the caller must fall back conservatively (treat it as
// matching every row).
Status parse_regex(std::string_view pattern, std::unique_ptr<RegexNode>* root,
                   bool* case_insensitive);

} // namespace doris::segment_v2::gram
