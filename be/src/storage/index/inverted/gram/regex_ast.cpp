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

#include "storage/index/inverted/gram/regex_ast.h"

#include <algorithm>
#include <cctype>
#include <cstdint>

namespace doris::segment_v2::gram {

// The BE storage target enables CMake unity builds (several .cpp files are compiled together,
// see UNITY_BUILD_BATCH_SIZE in be/src/storage/CMakeLists.txt), so every anonymous namespace in
// a batch is merged into one translation unit. A bare anonymous namespace then redefines any
// symbol whose name another file of the same batch happens to reuse (even in a different .cpp),
// and the batching changes as files are added to or removed from the directory, so "this batch
// only holds these files" cannot be assumed for long. Hence the extra named namespace private
// to this file, which isolates this file's anonymous namespace; the symbols inside it still
// have internal linkage (anonymous-namespace semantics are unaffected by a named enclosing
// namespace).
namespace regex_ast_detail {

namespace {

// Maximum recursion nesting depth of `(...)` groups: every extra group level adds one more
// recursion through the parse_alt/parse_cat/parse_atom call chain. A malformed (or maliciously
// crafted) regex can drive that chain very deep with a pile of nested parentheses and blow the
// stack; this repository has already seen a stack overflow from deep recursion (CIR-21633), so
// there is a hard cap here that errors out instead of recursing further.
constexpr int kMaxNestingDepth = 64;

// The parser derives conservative literal constraints for the scalar regex engines.
// Unsupported syntax fails parsing so the caller can skip gram filtering.

// Infer the byte length of a UTF-8 sequence from its lead byte; an illegal lead byte counts as a
// single byte.
int utf8_len(unsigned char c) {
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
    return 1; // illegal lead byte: treat it as a single byte
}

// The largest legal Unicode code point. Anything above it can only be a fake code point minted
// by decode_one_cp for an ill-formed byte, and must never reach encode_cp: the four-byte sequence
// encode_cp would produce encodes a value above U+10FFFF, so it is a byte string no encoder can
// emit and no index can hold, and demanding it as a gram would filter every row away.
constexpr uint32_t kMaxCodePoint = 0x10FFFF;

// Decode the code point starting at s[0]; s must not be empty. A well-formed UTF-8 sequence
// yields its code point and its byte length; any ill-formed byte (an illegal lead byte, a
// truncated sequence or a bad continuation byte) yields the fake code point 0x110000+byte (still
// < 2^21, so it cannot collide with a legal one) and consumes exactly one byte.
//
// *consumed is what keeps a caller's cursor in sync with the decoder. Advancing by the length
// guessed from the lead byte instead would swallow the bytes following an ill-formed sequence --
// regex metacharacters among them -- and silently compile a different pattern than the engine
// sees.
uint32_t decode_one_cp(std::string_view s, size_t* consumed) {
    const auto c = static_cast<unsigned char>(s[0]);
    const int l = utf8_len(c);
    *consumed = 1;
    if (l == 1) {
        return c < 0x80 ? c : 0x110000U + c;
    }
    if (static_cast<size_t>(l) > s.size()) {
        return 0x110000U + c;
    }
    uint32_t v = 0;
    if (l == 2) {
        v = c & 0x1FU;
    } else if (l == 3) {
        v = c & 0x0FU;
    } else {
        v = c & 0x07U;
    }
    for (int k = 1; k < l; k++) {
        const auto cc = static_cast<unsigned char>(s[k]);
        if ((cc & 0xC0) != 0x80) {
            return 0x110000U + c;
        }
        v = (v << 6) | (cc & 0x3FU);
    }
    *consumed = static_cast<size_t>(l);
    return v;
}

// Encode one code point as UTF-8 and append it to out.
void encode_cp(uint32_t cp, std::string* out) {
    if (cp < 0x80) {
        out->push_back((char)cp);
    } else if (cp < 0x800) {
        out->push_back((char)(0xC0 | (cp >> 6)));
        out->push_back((char)(0x80 | (cp & 0x3F)));
    } else if (cp < 0x10000) {
        out->push_back((char)(0xE0 | (cp >> 12)));
        out->push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out->push_back((char)(0x80 | (cp & 0x3F)));
    } else {
        out->push_back((char)(0xF0 | (cp >> 18)));
        out->push_back((char)(0x80 | ((cp >> 12) & 0x3F)));
        out->push_back((char)(0x80 | ((cp >> 6) & 0x3F)));
        out->push_back((char)(0x80 | (cp & 0x3F)));
    }
}

using NP = std::unique_ptr<RegexNode>;

NP mk(RegexNode::Type t) {
    auto p = std::make_unique<RegexNode>();
    p->type = t;
    return p;
}

// ASCII K and S also match the Kelvin sign and long s under the scalar engines' Unicode
// case-insensitive matching. Keep the same expansion for literals and small character classes.
void append_ascii_case_variants(uint32_t cp, std::vector<std::string>* items) {
    items->emplace_back(1, static_cast<char>(cp));
    const uint32_t lower = cp >= 'A' && cp <= 'Z' ? cp + ('a' - 'A') : cp;
    if (lower < 'a' || lower > 'z') {
        return;
    }
    items->emplace_back(1, static_cast<char>(cp == lower ? cp - ('a' - 'A') : lower));
    if (lower == 'k') {
        items->emplace_back("K");
    } else if (lower == 's') {
        items->emplace_back("ſ");
    }
}

// Recursive-descent parser for the supported regex subset.
struct Parser {
    std::string_view p;
    size_t i = 0;
    bool icase = false;
    bool ok = true;
    std::string err;
    int depth = 0; // current group nesting depth, see kMaxNestingDepth

    explicit Parser(std::string_view s) : p(s) {}

    bool eof() const { return i >= p.size(); }
    char peek() const { return eof() ? 0 : p[i]; }

    uint32_t next_cp(std::string* utf8) {
        if (eof()) {
            // Defensive fallback: every normal call site checks that a character is still
            // available before entering next_cp; this merely distrusts the caller and avoids an
            // out-of-bounds p[i] read at i==size() on a string_view, which -- unlike the
            // std::string the prototype used -- is not guaranteed to be NUL-terminated.
            utf8->clear();
            return 0;
        }
        // Advance by however many bytes the decoder actually consumed, never by the length
        // guessed from the lead byte: an ill-formed sequence consumes exactly one byte, and
        // advancing further would swallow the bytes that follow it -- including a regex
        // metacharacter that may sit there -- and compile a pattern the engine never saw.
        size_t consumed = 0;
        const uint32_t cp = decode_one_cp(p.substr(i), &consumed);
        *utf8 = std::string(p.substr(i, consumed));
        i += consumed;
        return cp;
    }

    NP parse() {
        NP r = parse_alt();
        if (!eof()) {
            ok = false;
            err = "trailing input at " + std::to_string(i);
        }
        return r;
    }

    NP parse_alt() {
        std::vector<NP> branches;
        branches.push_back(parse_cat());
        while (peek() == '|') {
            i++;
            branches.push_back(parse_cat());
        }
        if (branches.size() == 1) {
            return std::move(branches[0]);
        }
        NP a = mk(RegexNode::Type::ALT);
        a->kids = std::move(branches);
        return a;
    }

    NP parse_cat() {
        NP c = mk(RegexNode::Type::CAT);
        while (!eof() && peek() != '|' && peek() != ')') {
            if (peek() == '\\' && i + 1 < p.size() && p[i + 1] == 'Q') {
                append_quoted_literals(&c->kids);
                if (!c->kids.empty()) {
                    // A quote adds individual literal atoms. If it is empty, a following
                    // quantifier still applies to the preceding atom, including a group or
                    // repeat. Keep this token boundary: '+\\Q\\E?' must not become lazy '+?'.
                    c->kids.back() = parse_quant(std::move(c->kids.back()));
                }
                continue;
            }
            NP atom = parse_atom();
            if (!ok) {
                return c;
            }
            if (!atom) {
                continue; // e.g. a flags-only empty atom such as (?i)
            }
            atom = parse_quant(std::move(atom));
            c->kids.push_back(std::move(atom));
        }
        return c;
    }

    void append_quoted_literals(std::vector<NP>* atoms) {
        i += 2; // '\\Q'
        while (!eof() && !(peek() == '\\' && i + 1 < p.size() && p[i + 1] == 'E')) {
            std::string utf8;
            atoms->push_back(make_lit(next_cp(&utf8)));
        }
        if (!eof()) {
            i += 2; // '\\E'
        }
    }

    NP parse_quant(NP a) {
        while (!eof()) {
            char c = peek();
            if (c == '*') {
                i++;
                NP s = mk(RegexNode::Type::STAR);
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else if (c == '+') {
                i++;
                NP s = mk(RegexNode::Type::PLUS);
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else if (c == '?') {
                i++;
                NP s = mk(RegexNode::Type::QUEST);
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else if (c == '{') {
                size_t save = i;
                i++;
                int mn = 0;
                int mx = -1;
                bool has = false;
                while (!eof() && std::isdigit(static_cast<unsigned char>(peek()))) {
                    mn = mn * 10 + (peek() - '0');
                    i++;
                    has = true;
                }
                if (!has) {
                    i = save;
                    break;
                }
                if (peek() == ',') {
                    i++;
                    if (std::isdigit(static_cast<unsigned char>(peek()))) {
                        mx = 0;
                        while (!eof() && std::isdigit(static_cast<unsigned char>(peek()))) {
                            mx = mx * 10 + (peek() - '0');
                            i++;
                        }
                    }
                } else {
                    mx = mn;
                }
                if (peek() != '}') {
                    i = save;
                    break;
                }
                i++;
                NP s = mk(RegexNode::Type::REPEAT);
                s->rmin = mn;
                s->rmax = mx;
                s->kids.push_back(std::move(a));
                a = std::move(s);
            } else {
                break;
            }
            if (peek() == '?') {
                i++; // a lazy quantifier does not change the match set
            }
        }
        return a;
    }

    // Hex value of a `\x` escape in class_escape: on entry "\x" has already been consumed (i
    // points at the brace or at the first hex digit). Ruling R12: the `\x{...}` form requires at
    // least one hex digit inside the braces and the braces must be closed; the bare `\xHH` form
    // requires exactly two hex digits, and anything shorter (end of string, or a non-hex
    // character) is an error, matching RE2's rejection of `\x4`. On success the value is written
    // to *v and true is returned; on failure ok=false and err are set and false is returned (the
    // caller then returns immediately). Split out of class_escape to reduce its
    // complexity/length; the semantics are identical to the original inline code.
    bool parse_hex_escape_value(uint32_t* v) {
        *v = 0;
        if (peek() == '{') {
            i++;
            int cnt = 0;
            while (!eof() && peek() != '}') {
                if (!std::isxdigit(static_cast<unsigned char>(peek()))) {
                    ok = false;
                    err = "bad \\x escape";
                    return false;
                }
                *v = *v * 16 +
                     (std::isdigit(static_cast<unsigned char>(peek()))
                              ? peek() - '0'
                              : (std::tolower(static_cast<unsigned char>(peek())) - 'a' + 10));
                i++;
                cnt++;
            }
            if (eof() || cnt == 0) {
                ok = false;
                err = "bad \\x escape";
                return false;
            }
            i++; // consume '}'
            return true;
        }
        for (int cnt = 0; cnt < 2; cnt++) {
            if (eof() || !std::isxdigit(static_cast<unsigned char>(peek()))) {
                ok = false;
                err = "bad \\x escape";
                return false;
            }
            *v = *v * 16 +
                 (std::isdigit(static_cast<unsigned char>(peek()))
                          ? peek() - '0'
                          : (std::tolower(static_cast<unsigned char>(peek())) - 'a' + 10));
            i++;
        }
        return true;
    }

    // Decode only character escapes whose meaning is shared by the scalar engines. Other
    // letter/digit escapes may denote assertions, classes or backreferences; treating them as
    // literals could exclude matching rows. Three-digit octal avoids short numeric escapes'
    // ambiguity with backreferences.
    bool parse_character_escape(uint32_t* cp) {
        const char c = p[i++];
        switch (c) {
        case 'a':
            *cp = '\a';
            return true;
        case 'f':
            *cp = '\f';
            return true;
        case 'n':
            *cp = '\n';
            return true;
        case 'r':
            *cp = '\r';
            return true;
        case 't':
            *cp = '\t';
            return true;
        case 'x':
            return parse_hex_escape_value(cp);
        default:
            if (c >= '0' && c <= '7' && i + 1 < p.size() && p[i] >= '0' && p[i] <= '7' &&
                p[i + 1] >= '0' && p[i + 1] <= '7') {
                *cp = (c - '0') * 64 + (p[i] - '0') * 8 + (p[i + 1] - '0');
                i += 2;
                return true;
            }
            if (c >= ' ' && c <= '~' && !std::isalnum(static_cast<unsigned char>(c))) {
                *cp = c;
                return true;
            }
            ok = false;
            err = "unsupported character escape";
            return false;
        }
    }

    // Add one code point or escape from inside a class to out (a large class sets big).
    void class_escape(std::vector<uint32_t>* out, bool* big) {
        // The caller always consumes the '\\' that triggered the escape before calling this
        // function; being at eof here means that '\\' was the last character of the whole
        // pattern with nothing left to escape -- the same error as "trailing backslash" in the
        // top-level parse_atom, and handled the same way: report it instead of falling through
        // to the default branch, where next_cp would read p[i] once more at i==p.size().
        if (eof()) {
            ok = false;
            err = "trailing backslash";
            return;
        }
        char c = peek();
        i++;
        switch (c) {
        case 'd':
        case 'w':
        case 's':
        case 'D':
        case 'W':
        case 'S':
        case 'v': // Hyperscan: vertical whitespace class; RE2: vertical tab.
            *big = true;
            break;
        case 'p':
        case 'P':
            *big = true;
            if (peek() == '{') {
                while (!eof() && peek() != '}') {
                    i++;
                }
                i++;
            } else {
                i++;
            }
            break;
        default: {
            i--;
            uint32_t cp;
            if (!parse_character_escape(&cp)) {
                return;
            }
            out->push_back(cp);
            break;
        }
        }
    }

    // Handling of a `-hi` range after a single code point lo inside [...]: a real range expands
    // to [lo,hi] (beyond 4 items it degrades to a large class together with the items already
    // collected); when it is not a range (the next character is not '-', or the '-' sits right
    // before ']' and is therefore a literal '-'), lo itself becomes a standalone item. Split out
    // of parse_class to reduce its complexity/length; the semantics are identical to the
    // original inline code.
    void parse_class_range_or_single(uint32_t lo, std::vector<uint32_t>* items, bool* big) {
        if (!(peek() == '-' && i + 1 < p.size() && p[i + 1] != ']')) {
            items->push_back(lo);
            return;
        }
        i++;
        uint32_t hi;
        if (peek() == '\\') {
            i++;
            std::vector<uint32_t> tmp;
            bool b2 = false;
            class_escape(&tmp, &b2);
            hi = tmp.empty() ? lo : tmp.back();
            if (b2) {
                *big = true;
            }
        } else {
            std::string u;
            hi = next_cp(&u);
        }
        if (hi < lo) {
            std::swap(lo, hi);
        }
        if (hi - lo + 1 + items->size() > 4) {
            *big = true;
        } else {
            for (uint32_t x = lo; x <= hi; x++) {
                items->push_back(x);
            }
        }
    }

    // Encode the code points collected inside `[...]` into n->cls. Returns false when the class
    // cannot be enumerated and the caller has to degrade it to big_class (with cls cleared, per
    // the invariant documented on RegexNode): an ill-formed byte of the pattern, or a non-ASCII
    // item under `(?i)`. Split out of parse_class to keep that function under the size
    // threshold; the semantics are identical to the original inline loop.
    bool encode_class_items(const std::vector<uint32_t>& items, RegexNode* n) const {
        for (auto cp : items) {
            if (cp > kMaxCodePoint) {
                // Same reason as in make_lit: an ill-formed byte of the pattern decodes to a
                // fake code point that encode_cp would turn into a byte sequence no index can
                // ever hold, so it cannot be enumerated as a class element.
                return false;
            }
            if (icase && cp >= 128) {
                // The scalar engines fold Unicode, while the gram index only folds ASCII.
                // An unenumerated alternative makes this class unknown, but leaves surrounding
                // literal constraints available to the compiler.
                return false;
            }
            if (icase) {
                append_ascii_case_variants(cp, &n->cls);
                continue;
            }
            std::string u;
            encode_cp(cp, &u);
            n->cls.push_back(u);
        }
        return true;
    }

    NP parse_class() {
        // '[' has already been consumed
        NP n = mk(RegexNode::Type::CLASS);
        bool neg = false;
        bool big = false;
        if (peek() == '^') {
            neg = true;
            i++;
        }
        std::vector<uint32_t> items;
        bool first = true;
        while (!eof() && (peek() != ']' || first)) {
            first = false;
            if (peek() == '[' && i + 1 < p.size() && p[i + 1] == ':') { // POSIX class, [:alpha:]
                size_t e = p.find(":]", i);
                if (e == std::string_view::npos) {
                    ok = false;
                    err = "bad posix class";
                    return n;
                }
                i = e + 2;
                big = true;
                continue;
            }
            uint32_t lo;
            if (peek() == '\\') {
                i++;
                size_t before = items.size();
                class_escape(&items, &big);
                if (items.size() == before) {
                    continue;
                }
                lo = items.back();
                items.pop_back();
            } else {
                std::string u;
                lo = next_cp(&u);
            }
            parse_class_range_or_single(lo, &items, &big);
        }
        if (peek() != ']') {
            ok = false;
            err = "unterminated class";
            return n;
        }
        i++;
        if (neg || big || items.size() > 4) {
            n->big_class = true;
            return n;
        }
        if (!encode_class_items(items, n.get())) {
            n->big_class = true;
            n->cls.clear();
            return n;
        }
        std::sort(n->cls.begin(), n->cls.end());
        n->cls.erase(std::unique(n->cls.begin(), n->cls.end()), n->cls.end());
        if (n->cls.size() > 4) {
            // Under `(?i)` the case expansion can grow <= 4 original code points to more than
            // 4 items (e.g. (?i)[abc] expands to 6), which degrades to a large class; cls must
            // be cleared whenever big_class=true (see the invariant in the RegexNode comment in
            // the header), otherwise a caller cannot tell "is this enumerable?" from big_class
            // alone.
            n->big_class = true;
            n->cls.clear();
        }
        return n;
    }

    NP make_lit(uint32_t cp) const {
        if (cp > kMaxCodePoint) {
            // A fake code point minted for an ill-formed byte of the pattern. encode_cp would
            // turn it into a four-byte sequence above U+10FFFF, while the index side only ever
            // stores raw slices of the row, so that gram cannot exist anywhere and the AND would
            // prune every row of the query. Degrade to the same "unknown character" the icase
            // branch below uses.
            return mk(RegexNode::Type::ANY);
        }
        if (icase && cp >= 128) {
            // Unknown Unicode case variants must not become mandatory literal grams.
            return mk(RegexNode::Type::ANY);
        }
        if (icase && ((cp >= 'A' && cp <= 'Z') || (cp >= 'a' && cp <= 'z'))) {
            NP n = mk(RegexNode::Type::CLASS);
            append_ascii_case_variants(cp, &n->cls);
            std::sort(n->cls.begin(), n->cls.end());
            return n;
        }
        NP n = mk(RegexNode::Type::LIT);
        encode_cp(cp, &n->lit);
        return n;
    }

    // Decoding of the `\` escape branch in parse_atom: on entry '\\' has been consumed and eof
    // has been ruled out (the caller handles a trailing backslash before switching here), and
    // the escape is dispatched on the peek()ed character. Quotes are handled by parse_cat.
    NP parse_backslash_escape() {
        char e = peek();
        switch (e) {
        case 'd':
        case 'w':
        case 's':
        case 'D':
        case 'W':
        case 'S':
        case 'v': { // Cover both Hyperscan's whitespace class and RE2's vertical tab.
            i++;
            NP n = mk(RegexNode::Type::CLASS);
            n->big_class = true;
            return n;
        }
        case 'b':
        case 'B':
        case 'A':
        case 'Z':
        case 'z':
            i++;
            return mk(RegexNode::Type::EMPTY);
        case 'p':
        case 'P': {
            i++;
            if (peek() == '{') {
                while (!eof() && peek() != '}') {
                    i++;
                }
                i++;
            } else {
                i++;
            }
            NP n = mk(RegexNode::Type::CLASS);
            n->big_class = true;
            return n;
        }
        default: {
            uint32_t cp;
            if (!parse_character_escape(&cp)) {
                return nullptr;
            }
            return make_lit(cp);
        }
        }
    }

    // The '(' branch of parse_atom: parses a capturing group / a non-capturing group `(?:...)` /
    // a named group `(?P<name>...)` or `(?<name>...)` and the inline flags `(?i) (?is) (?i:...)`,
    // then recursively parses the group body and checks the closing parenthesis, all while
    // maintaining the kMaxNestingDepth recursion cap. On entry the leading '(' has already been
    // consumed. Groups restore the enclosing flags after parsing their bodies.
    NP parse_group() {
        const bool enclosing_icase = icase;
        if (peek() == '?') {
            i++;
            if (peek() == ':') {
                i++;
            } else if (peek() == 'P' || peek() == '<') { // named group
                if (peek() == 'P') {
                    i++;
                }
                if (peek() != '<') {
                    ok = false;
                    err = "bad group";
                    return nullptr;
                }
                while (!eof() && peek() != '>') {
                    i++;
                }
                i++;
            } else { // flags (?i) (?is) (?i:...)
                bool neg = false;
                while (!eof() && peek() != ')' && peek() != ':') {
                    char f = peek();
                    i++;
                    if (f == '-') {
                        neg = true;
                    } else if (f == 'i') {
                        icase = !neg;
                    } else if (f == 's' || f == 'm' || f == 'U') {
                        // Recognized but irrelevant to the AST shape: (?s)(?m)(?U) do not
                        // change the match set of a literal or a class, so gram compilation
                        // need not distinguish them.
                    } else {
                        ok = false;
                        err = "unknown flag";
                        return nullptr;
                    }
                }
                if (peek() == ')') {
                    i++;
                    // Flags-only groups affect the remainder of their enclosing scope.
                    return nullptr;
                }
                i++; // ':'
            }
        }
        depth++;
        if (depth > kMaxNestingDepth) {
            ok = false;
            err = "regex nesting too deep";
            depth--;
            return nullptr;
        }
        NP inner = parse_alt();
        depth--;
        icase = enclosing_icase;
        if (peek() != ')') {
            ok = false;
            err = "missing )";
            return inner;
        }
        i++;
        return inner;
    }

    NP parse_atom() {
        char c = peek();
        if (c == '(') {
            i++;
            return parse_group();
        }
        if (c == '[') {
            i++;
            return parse_class();
        }
        if (c == '.') {
            i++;
            return mk(RegexNode::Type::ANY);
        }
        if (c == '^' || c == '$') {
            i++;
            return mk(RegexNode::Type::EMPTY);
        }
        if (c == '\\') {
            i++;
            if (eof()) {
                ok = false;
                err = "trailing backslash";
                return nullptr;
            }
            return parse_backslash_escape();
        }
        if (c == '*' || c == '+' || c == '?') {
            ok = false;
            err = "bad quantifier";
            return nullptr;
        }
        std::string u;
        uint32_t cp = next_cp(&u);
        return make_lit(cp);
    }
};

} // namespace

} // namespace regex_ast_detail

Status parse_regex(std::string_view pattern, std::unique_ptr<RegexNode>* root,
                   bool* case_insensitive) {
    regex_ast_detail::Parser p(pattern);
    std::unique_ptr<RegexNode> r = p.parse();
    if (!p.ok) {
        return Status::InvalidArgument("regex parse error: {}", p.err);
    }
    *case_insensitive = p.icase;
    *root = std::move(r);
    return Status::OK();
}

} // namespace doris::segment_v2::gram
