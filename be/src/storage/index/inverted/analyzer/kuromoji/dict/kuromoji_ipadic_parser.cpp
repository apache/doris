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

#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_ipadic_parser.h"

#include <charconv>
#include <limits>
#include <string>
#include <vector>

namespace doris::segment_v2::kuromoji {

namespace {

constexpr std::string_view WS = " \t\r\n\f\v";

std::string_view trim(std::string_view s) {
    const auto b = s.find_first_not_of(WS);
    if (b == std::string_view::npos) {
        return {};
    }
    const auto e = s.find_last_not_of(WS);
    return s.substr(b, e - b + 1);
}

bool parse_dec(std::string_view s, int* v) {
    s = trim(s);
    if (s.empty()) {
        return false;
    }
    const auto* end = s.data() + s.size();
    auto r = std::from_chars(s.data(), end, *v);
    return r.ec == std::errc() && r.ptr == end;
}

int16_t to_int16(int v) {
    if (v > std::numeric_limits<int16_t>::max()) {
        return std::numeric_limits<int16_t>::max();
    }
    if (v < std::numeric_limits<int16_t>::min()) {
        return std::numeric_limits<int16_t>::min();
    }
    return static_cast<int16_t>(v);
}

bool parse_hex_cp(std::string_view s, uint32_t* v) {
    s = trim(s);
    if (s.size() > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        s = s.substr(2);
    }
    if (s.empty()) {
        return false;
    }
    const auto* end = s.data() + s.size();
    auto r = std::from_chars(s.data(), end, *v, 16);
    return r.ec == std::errc() && r.ptr == end;
}

// Strip an inline '#' comment and trim. Returns the content view (may be empty).
std::string_view strip_comment(std::string_view line) {
    const auto h = line.find('#');
    if (h != std::string_view::npos) {
        line = line.substr(0, h);
    }
    return trim(line);
}

// Whitespace-separated tokens.
std::vector<std::string_view> ws_tokens(std::string_view s) {
    std::vector<std::string_view> out;
    std::size_t i = 0;
    while (i < s.size()) {
        const auto b = s.find_first_not_of(WS, i);
        if (b == std::string_view::npos) {
            break;
        }
        auto e = s.find_first_of(WS, b);
        if (e == std::string_view::npos) {
            e = s.size();
        }
        out.push_back(s.substr(b, e - b));
        i = e;
    }
    return out;
}

// Invoke `fn(line)` for each line in `content`.
template <typename F>
void for_each_line(std::string_view content, F&& fn) {
    std::size_t i = 0;
    while (i <= content.size()) {
        auto nl = content.find('\n', i);
        if (nl == std::string_view::npos) {
            nl = content.size();
        }
        fn(content.substr(i, nl - i));
        if (nl == content.size()) {
            break;
        }
        i = nl + 1;
    }
}

} // namespace

uint8_t ipadic_category_ordinal(std::string_view name) {
    name = trim(name);
    if (name == "DEFAULT") return CAT_DEFAULT;
    if (name == "SPACE") return CAT_SPACE;
    if (name == "KANJI") return CAT_KANJI;
    if (name == "SYMBOL") return CAT_SYMBOL;
    if (name == "NUMERIC") return CAT_NUMERIC;
    if (name == "ALPHA") return CAT_ALPHA;
    if (name == "HIRAGANA") return CAT_HIRAGANA;
    if (name == "KATAKANA") return CAT_KATAKANA;
    if (name == "KANJINUMERIC") return CAT_KANJINUMERIC;
    if (name == "GREEK") return CAT_GREEK;
    if (name == "CYRILLIC") return CAT_CYRILLIC;
    return CAT_CLASS_COUNT; // unknown
}

Status parse_lexicon_line(std::string_view line, std::string* surface, BuilderWord* out) {
    const auto c1 = line.find(',');
    const auto c2 = c1 == std::string_view::npos ? c1 : line.find(',', c1 + 1);
    const auto c3 = c2 == std::string_view::npos ? c2 : line.find(',', c2 + 1);
    const auto c4 = c3 == std::string_view::npos ? c3 : line.find(',', c3 + 1);
    if (c4 == std::string_view::npos) {
        return Status::InvalidArgument("kuromoji ipadic: malformed lexicon line: {}",
                                       std::string(line));
    }
    int left = 0;
    int right = 0;
    int cost = 0;
    if (!parse_dec(line.substr(c1 + 1, c2 - c1 - 1), &left) ||
        !parse_dec(line.substr(c2 + 1, c3 - c2 - 1), &right) ||
        !parse_dec(line.substr(c3 + 1, c4 - c3 - 1), &cost)) {
        return Status::InvalidArgument("kuromoji ipadic: bad ids/cost in lexicon line: {}",
                                       std::string(line));
    }
    *surface = std::string(line.substr(0, c1));
    out->left_id = to_int16(left);
    out->right_id = to_int16(right);
    out->word_cost = to_int16(cost);
    out->feature = std::string(line.substr(c4 + 1));
    return Status::OK();
}

Status parse_matrix_def(std::string_view content, MatrixInput* out) {
    bool have_header = false;
    Status st = Status::OK();
    for_each_line(content, [&](std::string_view raw) {
        if (!st.ok()) {
            return;
        }
        const std::string_view line = strip_comment(raw);
        if (line.empty()) {
            return;
        }
        const auto tok = ws_tokens(line);
        if (!have_header) {
            int fwd = 0;
            int bwd = 0;
            if (tok.size() < 2 || !parse_dec(tok[0], &fwd) || !parse_dec(tok[1], &bwd) ||
                fwd <= 0 || bwd <= 0) {
                st = Status::InvalidArgument("kuromoji ipadic: bad matrix.def header");
                return;
            }
            out->forward_size = static_cast<uint32_t>(fwd);
            out->backward_size = static_cast<uint32_t>(bwd);
            out->cells.assign(static_cast<std::size_t>(fwd) * static_cast<std::size_t>(bwd), 0);
            have_header = true;
            return;
        }
        int a = 0;
        int b = 0;
        int c = 0;
        if (tok.size() < 3 || !parse_dec(tok[0], &a) || !parse_dec(tok[1], &b) ||
            !parse_dec(tok[2], &c)) {
            st = Status::InvalidArgument("kuromoji ipadic: bad matrix.def row");
            return;
        }
        if (a < 0 || b < 0 || static_cast<uint32_t>(a) >= out->forward_size ||
            static_cast<uint32_t>(b) >= out->backward_size) {
            st = Status::InvalidArgument("kuromoji ipadic: matrix.def id out of range");
            return;
        }
        out->cells[static_cast<std::size_t>(b) * out->forward_size + static_cast<std::size_t>(a)] =
                to_int16(c);
    });
    if (st.ok() && !have_header) {
        return Status::InvalidArgument("kuromoji ipadic: empty matrix.def");
    }
    return st;
}

Status parse_char_def(std::string_view content, CharDefInput* out) {
    out->catmap.fill(static_cast<uint8_t>(CAT_DEFAULT));
    out->defs.assign(CAT_CLASS_COUNT, CategoryDef {0, 0, 0});
    Status st = Status::OK();
    for_each_line(content, [&](std::string_view raw) {
        if (!st.ok()) {
            return;
        }
        const std::string_view line = strip_comment(raw);
        if (line.empty()) {
            return;
        }
        const auto tok = ws_tokens(line);
        if (tok.empty()) {
            return;
        }
        if (tok[0].size() >= 2 && tok[0][0] == '0' && (tok[0][1] == 'x' || tok[0][1] == 'X')) {
            // code-point mapping: 0xXXXX[..0xYYYY] CATEGORY [extra...]
            if (tok.size() < 2) {
                return;
            }
            uint32_t lo = 0;
            uint32_t hi = 0;
            const auto dots = tok[0].find("..");
            if (dots != std::string_view::npos) {
                if (!parse_hex_cp(tok[0].substr(0, dots), &lo) ||
                    !parse_hex_cp(tok[0].substr(dots + 2), &hi)) {
                    return;
                }
            } else {
                if (!parse_hex_cp(tok[0], &lo)) {
                    return;
                }
                hi = lo;
            }
            const uint8_t ord = ipadic_category_ordinal(tok[1]); // first = primary
            if (ord >= CAT_CLASS_COUNT) {
                return;
            }
            if (hi > 0xFFFF) {
                hi = 0xFFFF; // catmap covers the BMP only
            }
            for (uint32_t cp = lo; cp <= hi && cp <= 0xFFFF; ++cp) {
                out->catmap[cp] = ord;
            }
        } else {
            // category definition: NAME INVOKE GROUP LENGTH
            const uint8_t ord = ipadic_category_ordinal(tok[0]);
            if (ord >= CAT_CLASS_COUNT || tok.size() < 4) {
                return;
            }
            int invoke = 0;
            int group = 0;
            int length = 0;
            if (!parse_dec(tok[1], &invoke) || !parse_dec(tok[2], &group) ||
                !parse_dec(tok[3], &length)) {
                return;
            }
            out->defs[ord] = CategoryDef {static_cast<uint8_t>(invoke != 0),
                                          static_cast<uint8_t>(group != 0),
                                          static_cast<uint16_t>(length < 0 ? 0 : length)};
        }
    });
    return st;
}

Status parse_unk_def(std::string_view content, UnkDictInput* out) {
    out->per_category.assign(CAT_CLASS_COUNT, {});
    Status st = Status::OK();
    for_each_line(content, [&](std::string_view raw) {
        if (!st.ok()) {
            return;
        }
        const std::string_view line = strip_comment(raw);
        if (line.empty()) {
            return;
        }
        std::string surface;
        BuilderWord w;
        if (!parse_lexicon_line(line, &surface, &w).ok()) {
            return; // skip malformed unk row
        }
        const uint8_t ord = ipadic_category_ordinal(surface); // col 0 is the category name
        if (ord >= CAT_CLASS_COUNT) {
            return;
        }
        out->per_category[ord].push_back(std::move(w));
    });
    return st;
}

} // namespace doris::segment_v2::kuromoji
