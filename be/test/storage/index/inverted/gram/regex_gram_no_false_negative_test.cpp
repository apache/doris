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

// Property test for the one hard contract of the gram compiler:
//
//   for every row R and every pattern P, if the real engine says "P matches R" then
//   evaluating compile(P, scheme) over extract(R, scheme) must yield true.
//
// A false positive (a candidate row the expression later rejects) is fine; a false negative
// silently drops a row from the result set, which is a correctness bug. The existing
// differential fuzz test (regex_gram_fuzz_test.cpp) only ever draws rows from a handful of
// pure-ASCII / valid-UTF-8 templates, so whole families of inputs -- illegal UTF-8 bytes,
// literals that start in the middle of a code point, NUL bytes, rows around the max_len
// boundary, very long rows -- were never exercised. This test walks a fixed corpus that
// deliberately contains all of them, across a scheme matrix that also varies min_len, max_len
// and density instead of only mode x lower_case.

#include <gtest/gtest.h>
#include <re2/re2.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/gram/regex_gram_compiler.h"

namespace doris::segment_v2::gram {

namespace {

// ---------------------------------------------------------------------------------------------
// Independent interpreters (they never call into the code under test)
// ---------------------------------------------------------------------------------------------

// Evaluate a query tree over the gram set of one row, re-implemented from the Op semantics so
// that this test does not verify the compiler with the compiler's own simplification logic.
bool eval_query(const GramQuery& q, const std::set<std::string>& grams) {
    switch (q.op) {
    case GramQuery::Op::ALL:
        return true;
    case GramQuery::Op::NONE:
        return false;
    case GramQuery::Op::AND:
        return std::ranges::all_of(q.grams, [&](const auto& g) { return grams.contains(g); }) &&
               std::ranges::all_of(q.subs, [&](const auto& s) { return eval_query(s, grams); });
    case GramQuery::Op::OR:
        return std::ranges::any_of(q.grams, [&](const auto& g) { return grams.contains(g); }) ||
               std::ranges::any_of(q.subs, [&](const auto& s) { return eval_query(s, grams); });
    }
    return true;
}

// Doris LIKE is byte oriented and anchored (the whole value has to match). `%` stands for any
// byte sequence, `_` for exactly one byte, and only `\%`, `\_` and `\\` are escapes. Modelling
// `_` as one byte rather than one code point makes the ground truth strictly more permissive,
// which can only make the "truth => candidate" assertion harder to satisfy -- and it stays sound,
// because the compiler treats `_` purely as a segment separator and assumes nothing about what it
// matches.
enum class LikeTok : uint8_t { kByte, kAnyOne, kAnySeq };

struct LikeItem {
    LikeTok kind = LikeTok::kByte;
    char byte = 0;
};

// `\x` where x is none of % _ \ has two possible readings (the engine may keep or drop the
// backslash); keep_unknown_backslash selects one of them. The caller checks both, because the
// compiler is conservative enough to have to be correct under either.
std::vector<LikeItem> parse_like(std::string_view pat, bool keep_unknown_backslash) {
    std::vector<LikeItem> out;
    for (size_t i = 0; i < pat.size(); i++) {
        const char c = pat[i];
        if (c == '\\' && i + 1 < pat.size()) {
            const char n = pat[i + 1];
            if (n == '%' || n == '_' || n == '\\') {
                out.push_back({.kind = LikeTok::kByte, .byte = n});
                i++;
                continue;
            }
            if (keep_unknown_backslash) {
                out.push_back({.kind = LikeTok::kByte, .byte = '\\'});
            }
            continue; // n is consumed as an ordinary character by the next iteration
        }
        if (c == '%') {
            out.push_back({.kind = LikeTok::kAnySeq, .byte = 0});
            continue;
        }
        if (c == '_') {
            out.push_back({.kind = LikeTok::kAnyOne, .byte = 0});
            continue;
        }
        out.push_back({.kind = LikeTok::kByte, .byte = c});
    }
    return out;
}

// Classic O(n*m) wildcard match, anchored at both ends.
bool like_match_tokens(const std::vector<LikeItem>& t, std::string_view s) {
    const size_t n = t.size();
    const size_t m = s.size();
    std::vector<uint8_t> prev(m + 1, 0);
    std::vector<uint8_t> cur(m + 1, 0);
    prev[0] = 1;
    for (size_t i = 1; i <= n; i++) {
        const LikeItem& it = t[i - 1];
        if (it.kind == LikeTok::kAnySeq) {
            cur[0] = prev[0];
            for (size_t j = 1; j <= m; j++) {
                cur[j] = static_cast<uint8_t>(prev[j] != 0 || cur[j - 1] != 0);
            }
        } else {
            cur[0] = 0;
            for (size_t j = 1; j <= m; j++) {
                const bool hit =
                        it.kind == LikeTok::kAnyOne ||
                        static_cast<unsigned char>(it.byte) == static_cast<unsigned char>(s[j - 1]);
                cur[j] = static_cast<uint8_t>(hit && prev[j - 1] != 0);
            }
        }
        prev.swap(cur);
    }
    return prev[m] != 0;
}

bool like_matches(std::string_view pattern, std::string_view row) {
    return like_match_tokens(parse_like(pattern, false), row) ||
           like_match_tokens(parse_like(pattern, true), row);
}

// ---------------------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------------------

std::string hex_escape(std::string_view s) {
    static const char* kHex = "0123456789ABCDEF";
    std::string r;
    for (unsigned char c : s) {
        if (c >= 0x20 && c < 0x7F && c != '\\') {
            r.push_back(static_cast<char>(c));
        } else {
            r += "\\x";
            r.push_back(kHex[c >> 4]);
            r.push_back(kHex[c & 0x0F]);
        }
    }
    return r;
}

// A byte sequence no conforming UTF-8 encoder can ever produce, so no index can ever hold a gram
// containing it: a lead byte above 0xF4, a 0xF4 sequence encoding more than U+10FFFF, or the
// WTF-8 encoding of a surrogate. Requiring such a gram prunes every row unconditionally.
bool is_unencodable(std::string_view g) {
    for (size_t i = 0; i < g.size(); i++) {
        const auto c = static_cast<unsigned char>(g[i]);
        if (c >= 0xF5) {
            return true;
        }
        if (c == 0xF4 && i + 1 < g.size() && static_cast<unsigned char>(g[i + 1]) >= 0x90) {
            return true;
        }
        if (c == 0xED && i + 1 < g.size() && static_cast<unsigned char>(g[i + 1]) >= 0xA0) {
            return true;
        }
    }
    return false;
}

bool query_has_unencodable_gram(const GramQuery& q) {
    return std::ranges::any_of(q.grams, [](const auto& g) { return is_unencodable(g); }) ||
           std::ranges::any_of(q.subs, [](const auto& s) { return query_has_unencodable_gram(s); });
}

std::set<std::string> grams_of_row(GramExtractor& ex, const std::string& row) {
    std::vector<std::string_view> g;
    ex.extract(row, &g);
    return {g.begin(), g.end()};
}

std::string describe(const GramScheme& s) {
    return std::string(s.mode == GramMode::DENSE ? "dense" : "sparse") + "/lc" +
           std::to_string(static_cast<int>(s.lower_case)) + "/min" + std::to_string(s.min_len) +
           "/max" + std::to_string(s.max_len) + "/d" + std::to_string(s.density_permille);
}

// ---------------------------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------------------------

// mode x lower_case x min_len x max_len x density. DENSE ignores max_len/density, so those
// combinations are collapsed. min_len=1 is what drives RegexGramCompiler's `keep == 0` branch,
// which no other test reaches.
std::vector<GramScheme> build_schemes() {
    std::vector<GramScheme> out;
    for (GramMode mode : {GramMode::DENSE, GramMode::SPARSE}) {
        for (bool lc : {false, true}) {
            for (uint32_t min_len : {1U, 3U, 4U}) {
                if (mode == GramMode::DENSE) {
                    GramScheme s;
                    s.mode = mode;
                    s.lower_case = lc;
                    s.min_len = min_len;
                    out.push_back(s);
                    continue;
                }
                for (uint32_t max_len : {4U, 16U}) {
                    for (uint32_t density : {50U, 250U, 1000U}) {
                        GramScheme s;
                        s.mode = mode;
                        s.lower_case = lc;
                        s.min_len = min_len;
                        s.max_len = max_len;
                        s.density_permille = density;
                        out.push_back(s);
                    }
                }
            }
        }
    }
    return out;
}

// Rows deliberately covering what the existing fuzz corpus never produced: illegal lead bytes,
// stray continuation bytes, truncated sequences, surrogate and overlong encodings, embedded NUL,
// lengths right at min_len/max_len, and rows far longer than any gram window.
std::vector<std::string> build_rows() {
    return {
            "",
            "a",
            "ab",
            "abc",
            "abcd",
            "abcde",
            "abcdefghijklmno",   // max_len - 1
            "abcdefghijklmnop",  // max_len
            "abcdefghijklmnopq", // max_len + 1
            "AbCdEf",
            "ABC",
            "abc abc abc",
            "aaaaaaa",
            "a\tb\nc",
            " ",
            "100%",
            "a_b",
            "a\\b",
            "back\\slash",
            "rpc error: code = Unavailable desc = timeout",
            "user_id=abc GET /images/x.gif",
            "code=Unavailable",
            "timeout after error error error",
            "GET /a/b/c?d=1&e=2",
            "\xE6\x89\x8B\xE6\x9C\xBA\xE5\xBE\xAE\xE5\x8D\x9A POST 10.68.3.18:8080 error",
            "\xC3\xA9"
            "abc", // e-acute followed by "abc": the P0-2 row
            "x\xC3\xA9"
            "abcy",
            "\xC3\xA9"
            "lan vital",
            "\xF0\x9F\x98\x80 emoji",
            "\xFF",
            "\xFF"
            "abc",
            "\xC3",
            "z\xC3z",
            "\xC3"
            "abc",
            "z\xC3"
            "abcz",
            "\xE4\xBD",
            "z\xE4\xBDz",
            "\xE4\xBD"
            "abc",
            "z\xE4\xBD"
            "abcz",
            "abc\xFF"
            "def",
            "xxabc\xFF"
            "defyy",
            "\xA9"
            "abc",
            "abc\xA9"
            "def",
            "abc\xC3",
            "abc\xE4\xBD",
            "\xED\xA0\x80"
            "abc",      // surrogate (WTF-8) encoding
            "\xC0\xAF", // overlong '/'
            std::string("ab\0cd", 5),
            std::string("\0abc", 4),
            std::string("abc\0", 4),
            std::string(1, '\0'),
            std::string("\xFF\0\xFE", 3),
            std::string(4096, 'a'),
            std::string(2000, 'x') +
                    "abc\xFF"
                    "def" +
                    std::string(2000, 'y'),
            std::string(2000, 'x') +
                    "\xC3\xA9"
                    "abc" +
                    std::string(2000, 'y'),
    };
}

// Regex patterns. The last block holds patterns carrying raw illegal bytes; they are written so
// that a byte-oriented and a code-point-oriented reading agree (no quantifier, class or `.` ever
// applies to a multi-byte run), which is what makes the Latin-1 ground truth used for them below
// legitimate.
std::vector<std::string> build_regex_patterns() {
    return {
            "abc",
            "abcdef",
            "error",
            "GET|POST",
            "abc.*def",
            "(abc|def)",
            "^abc",
            "abc$",
            "a{3}b",
            "(abc){2}",
            "abc+",
            "[abc]def",
            "[^a]bcd",
            "\\d+error",
            "error.*timeout",
            "user_id=[a-z]+",
            "10\\.68\\.",
            "(?i)ABC",
            "(?i)\xE6\x89\x8B\xE6\x9C\xBA",
            "\xE6\x89\x8B\xE6\x9C\xBA",
            "\xE5\xBE\xAE\xE5\x8D\x9A",
            "\xF0\x9F\x98\x80",
            "\xC3\xA9"
            "lan",
            "aaaaaaaaaaaaaaaaaaaaaa",
            "a",
            "ab",
            // Demotion paths: a Cartesian product over kMaxSet, an exact set over kMaxExact, and
            // a long alternation. None of these had any coverage before.
            "(a|b|c|d|e|f|g|h)(1|2|3)",
            "(aa|bb|cc|dd|ee|ff|gg|hh)",
            "(aaa|bbb|ccc|ddd|eee|fff|ggg|hhh|iii|jjj|kkk|lll|mmm|nnn|ooo|ppp|qqq|rrr|sss|ttt|"
            "uuu|vvv|www|xxx|yyy)",
            "(x|y)(1|2)(3|4)(5|6)(7|8)",
            "(abc|abcd|abcde|abcdef|abcdefg|abcdefgh)",
            // Raw illegal bytes in the pattern.
            "abc\xFF"
            "def",
            "\xFF"
            "abc",
            "abc\xC3",
            "a\xA9"
            "b",
            "\xC3"
            "abc",
            "\xE4\xBD"
            "abc",
            "\xC3"
            "|abc",
            "\xE4\xBD"
            "|abc",
            "\xFF"
            "|abc",
    };
}

std::vector<std::string> build_like_patterns() {
    return {
            "abc",
            "%abc%",
            "%abc",
            "abc%",
            "a%c",
            "%abc%def%",
            "_abc_",
            "%a_c%",
            "abcdefghijklmnop",
            "%abcdefghijklmnop%",
            "%error%timeout%",
            "%GET%",
            "%code=Unavailable%",
            "%\xE6\x89\x8B\xE6\x9C\xBA%",
            "%\xE5\xBE\xAE\xE5\x8D\x9A%",
            "%\xC3\xA9"
            "abc%",
            "%100\\%%",
            "%a\\_b%",
            "%a\\\\b%",
            "",
            "%",
            "%%",
            "%aaaaaaa%",
            // Literal segments that are not aligned to a code point boundary, or that carry
            // illegal bytes.
            "%\xA9"
            "abc%",
            "%abc\xFF"
            "def%",
            "%\xC3%",
            "%\xFF%",
            "%\xED\xA0\x80%",
            "%\xC0\xAF%",
            "%\xE4\xBD%",
            "%abc\xC3%",
            "%\xC3"
            "abc%",
            "%\xE4\xBD"
            "abc%",
    };
}

// RE2 in UTF-8 mode is the ground truth for REGEXP (Doris compiles with HS_FLAG_UTF8 and falls
// back to RE2). A pattern holding raw illegal bytes cannot be compiled in UTF-8 mode at all, so
// for those the byte-oriented Latin-1 mode is used instead -- see the note on
// build_regex_patterns for why that stays faithful for this corpus.
std::unique_ptr<RE2> build_ground_truth_regex(const std::string& pattern) {
    auto utf8 = std::make_unique<RE2>(pattern, RE2::Quiet);
    if (utf8->ok()) {
        return utf8;
    }
    RE2::Options opt;
    opt.set_log_errors(false);
    opt.set_encoding(RE2::Options::EncodingLatin1);
    auto latin1 = std::make_unique<RE2>(pattern, opt);
    if (latin1->ok()) {
        return latin1;
    }
    return nullptr;
}

constexpr size_t kMaxRecordedFailures = 200;
constexpr size_t kReportedFailures = 25;

void record(std::vector<std::string>* failures, const std::string& msg) {
    if (failures->size() < kMaxRecordedFailures) {
        failures->push_back(msg);
    }
}

std::string join_failures(const std::vector<std::string>& failures) {
    std::string s = "false negatives: " + std::to_string(failures.size()) +
                    (failures.size() >= kMaxRecordedFailures ? "+ (capped)" : "") + "\n";
    for (size_t i = 0; i < failures.size() && i < kReportedFailures; i++) {
        s += "  " + failures[i] + "\n";
    }
    return s;
}

// The corpus plus its per-(pattern, row) ground truth, which does not depend on the scheme and is
// therefore computed once.
struct Corpus {
    std::vector<std::string> rows;
    std::vector<std::string> regex_patterns;
    std::vector<std::string> like_patterns;
    std::vector<std::unique_ptr<RE2>> res; // null when neither RE2 mode can compile the pattern
    std::vector<std::vector<uint8_t>> regex_truth;
    std::vector<std::vector<uint8_t>> like_truth;
    size_t compilable = 0;
};

Corpus build_corpus() {
    Corpus c;
    c.rows = build_rows();
    c.regex_patterns = build_regex_patterns();
    c.like_patterns = build_like_patterns();

    c.res.resize(c.regex_patterns.size());
    c.regex_truth.resize(c.regex_patterns.size());
    for (size_t p = 0; p < c.regex_patterns.size(); p++) {
        c.res[p] = build_ground_truth_regex(c.regex_patterns[p]);
        if (c.res[p] == nullptr) {
            continue;
        }
        c.compilable++;
        c.regex_truth[p].resize(c.rows.size());
        for (size_t r = 0; r < c.rows.size(); r++) {
            c.regex_truth[p][r] = static_cast<uint8_t>(RE2::PartialMatch(c.rows[r], *c.res[p]));
        }
    }

    c.like_truth.resize(c.like_patterns.size());
    for (size_t p = 0; p < c.like_patterns.size(); p++) {
        c.like_truth[p].resize(c.rows.size());
        for (size_t r = 0; r < c.rows.size(); r++) {
            c.like_truth[p][r] = static_cast<uint8_t>(like_matches(c.like_patterns[p], c.rows[r]));
        }
    }
    return c;
}

using GramSets = std::vector<std::set<std::string>>;

void check_one_pattern(const Corpus& c, const GramScheme& s, const GramSets& row_grams,
                       const std::string& kind, const std::string& pattern,
                       const std::vector<uint8_t>& truth, const GramQuery& q,
                       std::vector<std::string>* failures) {
    for (size_t r = 0; r < c.rows.size(); r++) {
        if (truth[r] == 0 || eval_query(q, row_grams[r])) {
            continue;
        }
        record(failures, describe(s) + " " + kind + " '" + hex_escape(pattern) + "' row='" +
                                 hex_escape(c.rows[r]) + "' q=" + hex_escape(q.to_debug_string()));
    }
}

void check_scheme(const Corpus& c, const GramScheme& s, std::vector<std::string>* failures) {
    GramExtractor ex(s);
    RegexGramCompiler comp(s);
    GramSets row_grams(c.rows.size());
    for (size_t r = 0; r < c.rows.size(); r++) {
        row_grams[r] = grams_of_row(ex, c.rows[r]);
    }
    for (size_t p = 0; p < c.regex_patterns.size(); p++) {
        if (c.res[p] == nullptr) {
            continue;
        }
        GramQuery q;
        EXPECT_TRUE(comp.compile_regexp(c.regex_patterns[p], &q).ok());
        check_one_pattern(c, s, row_grams, "REGEXP", c.regex_patterns[p], c.regex_truth[p], q,
                          failures);
    }
    for (size_t p = 0; p < c.like_patterns.size(); p++) {
        GramQuery q;
        EXPECT_TRUE(comp.compile_like(c.like_patterns[p], &q).ok());
        check_one_pattern(c, s, row_grams, "LIKE", c.like_patterns[p], c.like_truth[p], q,
                          failures);
    }
}

} // namespace

// The core property: over the whole scheme matrix, no compiled query may ever be false on a row
// the engine actually matches.
TEST(RegexGramNoFalseNegativeTest, CompiledQueryNeverPrunesAMatchingRow) {
    const Corpus c = build_corpus();
    // Guard against the corpus silently degenerating into "nothing has a ground truth".
    ASSERT_GT(c.compilable, c.regex_patterns.size() * 3 / 4);

    std::vector<std::string> failures;
    for (const GramScheme& s : build_schemes()) {
        check_scheme(c, s, &failures);
    }
    EXPECT_TRUE(failures.empty()) << join_failures(failures);
}

// P0-1: a raw illegal byte in a regex is decoded into the fake code point 0x110000+byte and then
// re-encoded by encode_cp into a four-byte sequence above U+10FFFF. The write side slices raw
// bytes and never re-encodes anything, so that gram cannot exist in any index and the AND prunes
// every row of the query.
TEST(RegexGramNoFalseNegativeTest, IllegalUtf8ByteInRegexIsNotCompiledIntoAnImpossibleGram) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    s.min_len = 3;
    GramExtractor ex(s);
    RegexGramCompiler comp(s);

    GramQuery q;
    ASSERT_TRUE(comp.compile_regexp("abc\xFF"
                                    "def",
                                    &q)
                        .ok());
    EXPECT_FALSE(query_has_unencodable_gram(q)) << hex_escape(q.to_debug_string());

    const std::string row =
            "xxabc\xFF"
            "defyy";
    EXPECT_TRUE(eval_query(q, grams_of_row(ex, row)))
            << "row '" << hex_escape(row) << "' contains the pattern byte for byte but the "
            << "compiled query prunes it: " << hex_escape(q.to_debug_string());
}

// P0-2: GramExtractor splits position-dependently -- a byte run starting in the middle of a code
// point is split differently from the same bytes inside the whole row. A LIKE literal segment
// that is not aligned to a code point boundary therefore demands grams the row does not have,
// even though Doris LIKE is byte oriented and does match the row.
TEST(RegexGramNoFalseNegativeTest, LikeLiteralStartingInsideACodePointDoesNotPruneAMatchingRow) {
    const std::string row =
            "\xC3\xA9"
            "abc"; // 'e-acute' + "abc"
    const std::string pattern =
            "%\xA9"
            "abc%";
    ASSERT_TRUE(like_matches(pattern, row));

    for (GramMode mode : {GramMode::DENSE, GramMode::SPARSE}) {
        GramScheme s;
        s.mode = mode;
        s.min_len = 3;
        GramExtractor ex(s);
        RegexGramCompiler comp(s);
        GramQuery q;
        ASSERT_TRUE(comp.compile_like(pattern, &q).ok());
        EXPECT_TRUE(eval_query(q, grams_of_row(ex, row)))
                << describe(s) << " LIKE '" << hex_escape(pattern) << "' prunes matching row '"
                << hex_escape(row) << "'; q=" << hex_escape(q.to_debug_string());
    }
}

// P0-3: Parser::next_cp advances by the length guessed from the lead byte, while decode_cps
// consumes exactly one byte for an ill-formed sequence. The bytes in between are swallowed --
// regex metacharacters included -- so a different pattern gets compiled than the engine sees.
// `\xC3|abc` is "illegal byte OR abc"; swallowing the `|` turns it into the concatenation
// "illegal byte followed by abc" and forces the gram "abc" onto rows that only hold `\xC3`.
TEST(RegexGramNoFalseNegativeTest, TruncatedUtf8SequenceDoesNotSwallowTheNextPatternByte) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    s.min_len = 3;
    GramExtractor ex(s);
    RegexGramCompiler comp(s);

    struct Case {
        std::string pattern;
        std::string row;
    };
    const std::vector<Case> cases = {
            {.pattern = "\xC3"
                        "|abc",
             .row = "z\xC3z"},
            {.pattern = "\xE4\xBD"
                        "|abc",
             .row = "z\xE4\xBDz"},
            {.pattern = "\xC3"
                        "abc",
             .row = "z\xC3"
                    "abcz"},
            {.pattern = "\xE4\xBD"
                        "abc",
             .row = "z\xE4\xBD"
                    "abcz"},
            {.pattern = "\xFF"
                        "abc",
             .row = "z\xFF"
                    "abcz"},
    };
    for (const Case& c : cases) {
        GramQuery q;
        ASSERT_TRUE(comp.compile_regexp(c.pattern, &q).ok());
        EXPECT_FALSE(query_has_unencodable_gram(q))
                << hex_escape(c.pattern) << " -> " << hex_escape(q.to_debug_string());
        EXPECT_TRUE(eval_query(q, grams_of_row(ex, c.row)))
                << "REGEXP '" << hex_escape(c.pattern) << "' prunes matching row '"
                << hex_escape(c.row) << "'; q=" << hex_escape(q.to_debug_string());
    }
}

// An out-of-range `\x{...}` escape is plain ASCII in the pattern, yet it also produces a fake
// code point; the same degradation has to cover it.
TEST(RegexGramNoFalseNegativeTest, OutOfRangeHexEscapeDoesNotBecomeAnImpossibleGram) {
    GramScheme s;
    s.mode = GramMode::DENSE;
    s.min_len = 3;
    RegexGramCompiler comp(s);
    for (const char* pattern : {"abc\\x{110000}def", "abc[\\x{110000}]def", "abc\\x{D800}def"}) {
        GramQuery q;
        ASSERT_TRUE(comp.compile_regexp(pattern, &q).ok());
        EXPECT_FALSE(query_has_unencodable_gram(q))
                << pattern << " -> " << hex_escape(q.to_debug_string());
    }
}

} // namespace doris::segment_v2::gram
