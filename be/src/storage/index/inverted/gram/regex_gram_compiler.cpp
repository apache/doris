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

#include "storage/index/inverted/gram/regex_gram_compiler.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "storage/index/inverted/gram/regex_ast.h"

namespace doris::segment_v2::gram {

// Storage is a unity build: other .cpp files in this directory also define file-level helpers
// named utf8_len / codepoint_len, so this file's helpers live in a file-specific namespace
// wrapped in an anonymous one.
namespace regex_gram_compiler_detail {
namespace {

// An exact set whose strings are all already >= n yields useful grams more often, so it is
// demoted past 4 entries; together with kMaxExact (the cap that demotes regardless of length)
// these are the prototype simplify's two thresholds.
constexpr size_t kMaxLongExact = 4;
// Recursion depth cap for analyze. parse_regex only bounds group nesting (64 levels), while
// stacked quantifiers such as `a++++...` build an equally deep PLUS/REPEAT chain; past the cap
// we degrade to info_any_match (no constraint at all, conservative and safe) so a user-supplied
// pattern cannot blow the stack.
constexpr int kMaxAnalyzeDepth = 200;
// Maximum number of copies a REPEAT is unrolled into: x{m,..} is expanded exactly min(m, 4)
// times and then handled as a plus.
constexpr int kMaxRepeatUnroll = 4;

// Byte length (1/2/3/4) of the UTF-8 sequence starting with lead byte c; an illegal lead byte
// counts as 1.
inline int utf8_len(unsigned char c) {
    if (c < 0x80) {
        return 1;
    }
    if ((c >> 5) == 0x6) {
        return 2;
    }
    if ((c >> 4) == 0xE) {
        return 3;
    }
    if ((c >> 3) == 0x1E) {
        return 4;
    }
    return 1;
}

// Byte length of one valid UTF-8 code point starting at p; returns 1 for a truncated sequence or
// a stray continuation byte. Exactly the rule GramExtractor uses: on the index side a non-ASCII
// code point is a whole 1-gram.
inline size_t codepoint_len(const char* p, size_t remain) {
    int l = utf8_len((unsigned char)p[0]);
    if (l == 1 || (size_t)l > remain) {
        return 1;
    }
    for (int k = 1; k < l; k++) {
        if (((unsigned char)p[k] & 0xC0) != 0x80) {
            return 1;
        }
    }
    return l;
}

// The leading <= k bytes of s, always stopping on a code point boundary. Cutting a multi-byte
// code point in half would forge "fake grams" that do not exist in the index and cause false
// negatives, so we would rather keep a few bytes fewer.
std::string head_units(const std::string& s, size_t k) {
    size_t i = 0;
    while (i < s.size()) {
        size_t l = codepoint_len(s.data() + i, s.size() - i);
        if (i + l > k) {
            break;
        }
        i += l;
    }
    return s.substr(0, i);
}

// The trailing <= k bytes of s, always starting on a code point boundary (same reason as
// head_units).
std::string tail_units(const std::string& s, size_t k) {
    if (s.size() <= k) {
        return s;
    }
    size_t i = 0;
    while (i < s.size() && s.size() - i > k) {
        i += codepoint_len(s.data() + i, s.size() - i);
    }
    return s.substr(i);
}

// Cartesian-product concatenation; sets *too_big and returns an empty set once the result would
// exceed kMaxSet (the caller then takes the demotion path).
std::set<std::string> cross(const std::set<std::string>& a, const std::set<std::string>& b,
                            bool* too_big) {
    std::set<std::string> r;
    *too_big = a.size() * b.size() > RegexGramCompiler::kMaxSet;
    if (*too_big) {
        return r;
    }
    for (const auto& x : a) {
        for (const auto& y : b) {
            r.insert(x + y);
        }
    }
    return r;
}

std::set<std::string> uni(const std::set<std::string>& a, const std::set<std::string>& b) {
    std::set<std::string> r = a;
    r.insert(b.begin(), b.end());
    return r;
}

// Byte length of the shortest string in the set; an empty set counts as 0.
size_t min_str_len(const std::set<std::string>& s) {
    size_t m = SIZE_MAX;
    for (const auto& x : s) {
        m = std::min(m, x.size());
    }
    return m == SIZE_MAX ? 0 : m;
}

// Cox's five-tuple: a finite approximation of the string set a regex subtree can match.
//   can_empty  the subtree can match the empty string
//   has_exact  the match set is fully enumerated by exact (prefix/suffix are then meaningless)
//   exact      the fully enumerated set of matching strings
//   prefix     the possible beginnings of every matching string (nothing outside the set can
//              start a match)
//   suffix     the possible endings of every matching string
//   match      the gram condition already known to hold inside every matching string
// Invariant: exact is non-empty when has_exact; otherwise prefix and suffix are both non-empty
// ("" meaning no constraint).
struct Info {
    bool can_empty = false;
    bool has_exact = false;
    std::set<std::string> exact;
    std::set<std::string> prefix;
    std::set<std::string> suffix;
    GramQuery match; // defaults to ALL
};

// Matches the empty string only.
Info info_empty() {
    Info i;
    i.can_empty = true;
    i.has_exact = true;
    i.exact = {""};
    return i;
}

// Matches exactly one unknown character (`.`, a big class, a non-indexable literal with NUL).
Info info_any_char() {
    Info i;
    i.prefix = {""};
    i.suffix = {""};
    return i;
}

// Matches any string (`x*`, or a subtree we cannot reason about): imposes no constraint.
Info info_any_match() {
    Info i;
    i.can_empty = true;
    i.prefix = {""};
    i.suffix = {""};
    return i;
}

const std::set<std::string>& pre(const Info& x) {
    return x.has_exact ? x.exact : x.prefix;
}

const std::set<std::string>& suf(const Info& x) {
    return x.has_exact ? x.exact : x.suffix;
}

// The body of the Cox derivation. Every place that can produce grams goes through q_of_string /
// q_of_set, both of which return ALL when no gram is available, which guarantees "rather not
// filter at all than filter a match away".
class CoxAnalyzer {
public:
    explicit CoxAnalyzer(GramExtractor& extractor)
            : _extractor(extractor), _scheme(extractor.scheme()) {}

    // Whole tree -> gram query.
    GramQuery compile(const RegexNode* root) {
        Info info = _analyze(root, 0);
        GramQuery m = std::move(info.match);
        if (info.has_exact) {
            m = GramQuery::and_(std::move(m), q_of_set(info.exact));
        } else {
            m = GramQuery::and_(std::move(m), q_of_set(info.prefix));
            m = GramQuery::and_(std::move(m), q_of_set(info.suffix));
        }
        return m;
    }

    // AND of every gram of the literal s. Returns ALL when s yields no gram (too short, or no
    // CDC boundary).
    GramQuery q_of_string(const std::string& s) {
        // The index side never produces a gram containing NUL, and neither may we (Ruling R9).
        // analyze already treats a literal node holding NUL as anyChar; this is the fallback for
        // other entry points such as compile_like.
        if (s.find('\0') != std::string::npos) {
            return GramQuery::all();
        }
        std::vector<std::string> g;
        _extractor.grams_of_literal(s, &g);
        if (g.empty()) {
            return GramQuery::all();
        }
        GramQuery q;
        q.op = GramQuery::Op::AND;
        q.grams = std::move(g);
        std::sort(q.grams.begin(), q.grams.end());
        q.grams.erase(std::unique(q.grams.begin(), q.grams.end()), q.grams.end());
        return q;
    }

    // Any one string of the set suffices -> OR. An empty set means "nothing can match", hence
    // NONE.
    GramQuery q_of_set(const std::set<std::string>& ss) {
        if (ss.empty()) {
            return GramQuery::none();
        }
        GramQuery r = GramQuery::none();
        for (const auto& s : ss) {
            r = GramQuery::or_(std::move(r), q_of_string(s));
        }
        return r;
    }

private:
    // How much of prefix/suffix to keep: a SPARSE gram is at most max_len long, so a whole gram
    // has to fit; DENSE has fixed length n, so n-1 is enough (anything longer only repeats grams
    // already folded into match).
    size_t _keep() const {
        if (_scheme.mode == GramMode::SPARSE) {
            return _scheme.max_len;
        }
        return _scheme.min_len >= 1 ? _scheme.min_len - 1 : 0;
    }

    // Whether every string in the set is long enough (>= n). Folding the whole set into match is
    // only worth it when they all are: if a single string yields no gram, the OR collapses to
    // ALL and the fold constrains nothing.
    bool _all_long(const std::set<std::string>& s) const {
        return std::ranges::all_of(s,
                                   [this](const auto& x) { return x.size() >= _scheme.min_len; });
    }

    // With scheme.lower_case the index side already folds ASCII letters to lower case, so
    // literals must fold the same way. The whole pattern string must not be lowercased -- that
    // would break the case-sensitive escapes `\E \B \W \D \S \P \A` -- so folding happens only
    // on AST leaves (LIT / CLASS elements).
    std::string _fold(const std::string& s) const {
        if (!_scheme.lower_case) {
            return s;
        }
        std::string r = s;
        for (auto& ch : r) {
            if (ch >= 'A' && ch <= 'Z') {
                ch = static_cast<char>(ch - 'A' + 'a');
            }
        }
        return r;
    }

    // Fold the set's grams into match (only meaningful when every string is >= n, otherwise it
    // carries no information), then trim the strings to keep bytes; if the set still exceeds
    // kMaxSet after trimming, keep shrinking keep until it is small enough or trimmed to empty.
    void _trim_set(std::set<std::string>* s, GramQuery* match, bool is_suffix) {
        if (s->empty()) {
            return;
        }
        if (_all_long(*s)) {
            *match = GramQuery::and_(std::move(*match), q_of_set(*s));
        }
        size_t keep = _keep();
        for (;;) {
            std::set<std::string> t;
            for (const auto& x : *s) {
                t.insert(is_suffix ? tail_units(x, keep) : head_units(x, keep));
            }
            *s = std::move(t);
            if (s->size() <= RegexGramCompiler::kMaxSet || keep == 0) {
                break;
            }
            keep--;
        }
    }

    // Demote exact to prefix/suffix: the full enumeration can no longer be maintained, but
    // "every matching string starts with one of the exact strings and ends with one of them"
    // still holds; fold the grams into match first so that information is not lost.
    void _demote(Info* x) {
        if (!x->has_exact) {
            return;
        }
        if (_all_long(x->exact)) {
            x->match = GramQuery::and_(std::move(x->match), q_of_set(x->exact));
        }
        x->prefix = x->exact;
        x->suffix = x->exact;
        x->exact.clear();
        x->has_exact = false;
    }

    // Demote exact once the set size / string length crosses a threshold, and trim
    // prefix/suffix. The prototype's simplify also takes a `force` parameter, but every call
    // site passes false, so it is omitted here.
    Info _simplify(Info x) {
        if (x.has_exact) {
            const size_t ml = min_str_len(x.exact);
            const bool all_long = _all_long(x.exact);
            // Three demotion conditions: too many enumerated strings (kMaxExact); not too many,
            // but each one already yields a gram, so enumerating further only multiplies OR
            // branches (kMaxLongExact); the strings have grown to >= 2n, so more concatenation
            // only makes exact longer without adding grams. In all three cases demoting to
            // prefix/suffix and landing the grams we have in match pays off more.
            if (x.exact.size() > RegexGramCompiler::kMaxExact ||
                (all_long && x.exact.size() > kMaxLongExact) ||
                ml >= static_cast<size_t>(2) * _scheme.min_len) {
                _demote(&x);
            }
        }
        if (!x.has_exact) {
            _trim_set(&x.prefix, &x.match, false);
            _trim_set(&x.suffix, &x.match, true);
        }
        return x;
    }

    // Concatenation xy.
    Info _concat_info(Info x, Info y) {
        Info r;
        if (x.has_exact && y.has_exact) {
            bool big = false;
            std::set<std::string> c = cross(x.exact, y.exact, &big);
            if (!big) {
                r.has_exact = true;
                r.exact = std::move(c);
            } else {
                _demote(&x);
                _demote(&y);
            }
        }
        if (!r.has_exact) {
            if (x.has_exact) {
                bool big = false;
                std::set<std::string> c = cross(x.exact, y.prefix, &big);
                if (big) {
                    _demote(&x);
                    r.prefix = x.prefix;
                    if (x.can_empty) {
                        r.prefix = uni(r.prefix, y.prefix);
                    }
                } else {
                    r.prefix = std::move(c);
                }
            } else {
                r.prefix = x.prefix;
                if (x.can_empty) {
                    r.prefix = uni(r.prefix, pre(y));
                }
            }
            if (y.has_exact) {
                bool big = false;
                std::set<std::string> c = cross(x.suffix, y.exact, &big);
                if (big) {
                    _demote(&y);
                    r.suffix = y.suffix;
                    if (y.can_empty) {
                        r.suffix = uni(r.suffix, x.suffix);
                    }
                } else {
                    r.suffix = std::move(c);
                }
            } else {
                r.suffix = y.suffix;
                if (y.can_empty) {
                    r.suffix = uni(r.suffix, suf(x));
                }
            }
            // Boundary grams: some suffix of x and some prefix of y are necessarily adjacent in
            // a matching string, so their concatenation is necessarily a substring of it and can
            // be folded straight into match.
            if (!x.has_exact && !y.has_exact) {
                bool big = false;
                std::set<std::string> c = cross(x.suffix, y.prefix, &big);
                if (!big && !c.empty() && _all_long(c)) {
                    r.match = GramQuery::and_(std::move(r.match), q_of_set(c));
                }
            }
        }
        r.match = GramQuery::and_(std::move(r.match),
                                  GramQuery::and_(std::move(x.match), std::move(y.match)));
        r.can_empty = x.can_empty && y.can_empty;
        return _simplify(std::move(r));
    }

    // Alternation x|y.
    Info _alt_info(Info x, Info y) {
        Info r;
        if (x.has_exact && y.has_exact) {
            std::set<std::string> u = uni(x.exact, y.exact);
            if (u.size() <= RegexGramCompiler::kMaxSet) {
                r.has_exact = true;
                r.exact = std::move(u);
            } else {
                _demote(&x);
                _demote(&y);
            }
        }
        if (!r.has_exact) {
            _demote(&x);
            _demote(&y);
            r.prefix = uni(x.prefix, y.prefix);
            r.suffix = uni(x.suffix, y.suffix);
        }
        r.can_empty = x.can_empty || y.can_empty;
        r.match = GramQuery::or_(std::move(x.match), std::move(y.match));
        return _simplify(std::move(r));
    }

    // x+: the repetition count is unknown, so all we can keep is "starts with one of x's
    // strings and ends with one of x's strings".
    Info _plus_info(Info x) {
        _demote(&x);
        return _simplify(std::move(x));
    }

    // Bounded quantifier REPEAT `{m}` / `{m,}` / `{m,n}`: unroll exactly
    // min(m, kMaxRepeatUnroll) times (which captures the boundary grams across copies); when
    // there may be more repetitions (rmax has no upper bound, or exceeds the unrolled count)
    // fall back to plus and keep only the two ends. Split out of _analyze to reduce its
    // complexity/length; the semantics are identical to the original switch case.
    Info _repeat_info(const RegexNode* n, int depth) {
        // rmin < 0 can only come from a counter overflow during parsing; the range is then
        // untrustworthy, so be conservative.
        if (n->kids.empty() || n->rmin < 0) {
            return info_any_match();
        }
        if (n->rmin == 0 && n->rmax == 0) {
            return info_empty();
        }
        if (n->rmin == 0) {
            return info_any_match();
        }
        Info acc = info_empty();
        const int reps = std::min(n->rmin, kMaxRepeatUnroll);
        for (int k = 0; k < reps; k++) {
            acc = _concat_info(std::move(acc), _analyze(n->kids[0].get(), depth + 1));
        }
        if (n->rmax == n->rmin && n->rmin <= kMaxRepeatUnroll) {
            return acc;
        }
        return _plus_info(std::move(acc));
    }

    Info _analyze(const RegexNode* n, int depth) {
        if (n == nullptr || depth > kMaxAnalyzeDepth) {
            return info_any_match();
        }
        switch (n->type) {
        case RegexNode::Type::EMPTY:
            return info_empty();
        case RegexNode::Type::LIT: {
            std::string lit = _fold(n->lit);
            // Ruling R9: a literal containing NUL is not indexable; treat the whole node as one
            // unknown character.
            if (lit.find('\0') != std::string::npos) {
                return info_any_char();
            }
            Info i;
            i.has_exact = true;
            i.exact.insert(std::move(lit));
            return _simplify(std::move(i));
        }
        case RegexNode::Type::CLASS: {
            DCHECK(!n->big_class || n->cls.empty());
            if (n->big_class || n->cls.empty()) {
                return info_any_char();
            }
            Info i;
            i.has_exact = true;
            for (const auto& s : n->cls) {
                std::string f = _fold(s);
                // A single non-indexable element degrades the whole class to an unknown
                // character: dropping just that element would stop exact from covering every
                // possibility, and that is exactly where false negatives come from.
                if (f.find('\0') != std::string::npos) {
                    return info_any_char();
                }
                i.exact.insert(std::move(f)); // folding may create duplicates; the set dedups
            }
            return _simplify(std::move(i));
        }
        case RegexNode::Type::ANY:
            return info_any_char();
        case RegexNode::Type::CAT: {
            Info acc = info_empty();
            for (const auto& k : n->kids) {
                acc = _concat_info(std::move(acc), _analyze(k.get(), depth + 1));
            }
            return acc;
        }
        case RegexNode::Type::ALT: {
            if (n->kids.empty()) {
                return info_any_match();
            }
            Info acc = _analyze(n->kids[0].get(), depth + 1);
            for (size_t k = 1; k < n->kids.size(); k++) {
                acc = _alt_info(std::move(acc), _analyze(n->kids[k].get(), depth + 1));
            }
            return acc;
        }
        case RegexNode::Type::STAR:
            return info_any_match();
        case RegexNode::Type::PLUS:
            if (n->kids.empty()) {
                return info_any_match();
            }
            return _plus_info(_analyze(n->kids[0].get(), depth + 1));
        case RegexNode::Type::QUEST:
            if (n->kids.empty()) {
                return info_any_match();
            }
            return _alt_info(_analyze(n->kids[0].get(), depth + 1), info_empty());
        case RegexNode::Type::REPEAT:
            return _repeat_info(n, depth);
        }
        return info_any_match();
    }

    GramExtractor& _extractor;
    const GramScheme& _scheme;
};

} // namespace
} // namespace regex_gram_compiler_detail

RegexGramCompiler::RegexGramCompiler(const GramScheme& scheme) : _extractor(scheme) {}

Status RegexGramCompiler::compile_regexp(std::string_view pattern, GramQuery* out) {
    std::unique_ptr<RegexNode> root;
    bool case_insensitive = false;
    // A parse failure always falls back conservatively to ALL; never return an error and fail
    // the caller's query.
    if (!parse_regex(pattern, &root, &case_insensitive).ok() || root == nullptr) {
        *out = GramQuery::all();
        return Status::OK();
    }
    // case_insensitive itself needs no extra handling: with lower_case=false parse_regex has
    // already expanded ASCII letter literals under `(?i)` into CLASS{c,C} (Cox's approach); with
    // lower_case=true both index and query fold to lower case, so it is enough for CoxAnalyzer
    // to fold on the AST leaves.
    regex_gram_compiler_detail::CoxAnalyzer analyzer(_extractor);
    *out = analyzer.compile(root.get());
    return Status::OK();
}

Status RegexGramCompiler::compile_like(std::string_view like_pattern, GramQuery* out) {
    regex_gram_compiler_detail::CoxAnalyzer analyzer(_extractor);
    GramQuery q = GramQuery::all();
    std::string seg;
    // Wildcards cut the pattern into literal segments: the bytes inside one segment necessarily
    // appear consecutively in a matching row, so grams can be taken from it directly.
    auto flush = [&] {
        if (!seg.empty()) {
            q = GramQuery::and_(std::move(q), analyzer.q_of_string(seg));
            seg.clear();
        }
    };
    for (size_t i = 0; i < like_pattern.size(); i++) {
        const char c = like_pattern[i];
        if (c == '\\') {
            if (i + 1 < like_pattern.size()) {
                const char next = like_pattern[i + 1];
                if (next == '%' || next == '_' || next == '\\') {
                    // Doris LIKE really escapes only these three: the backslash is consumed and
                    // next joins the current segment as a literal (next and its neighbours
                    // necessarily appear consecutively in a matching row).
                    seg.push_back(next);
                    i++;
                    continue;
                }
                // `\x` (x not one of % _ \): it is unclear whether the engine keeps the
                // backslash itself (two bytes "\x" in the row) or drops it (just "x" in the
                // row, the old implementation's assumption). All that holds under both
                // readings is "x is adjacent to whatever follows it"; whether a backslash sits
                // between x and what precedes it is unknown, so cut the segment at the
                // backslash -- the backslash yields no gram, and x starts a new segment on the
                // next iteration instead of joining the segment already cut (this loses a
                // little pruning power but can never filter a match away).
                flush();
                continue;
            }
            // A lone trailing backslash with nothing to escape: cut the segment and ignore it.
            flush();
            continue;
        }
        if (c == '%' || c == '_') {
            flush();
            continue;
        }
        seg.push_back(c);
    }
    flush();
    *out = std::move(q);
    return Status::OK();
}

} // namespace doris::segment_v2::gram
