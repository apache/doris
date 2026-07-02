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
#include <string>
#include <string_view>

namespace doris::snii::format {

inline constexpr std::string_view kPhraseBigramTermMarker =
        "\x1F"
        "SNII_PHRASE_BIGRAM"
        "\x1F";

// Encodes `value` as LEB128 into `buf` (>= 5 bytes), returning the byte count.
// Shared by the string-appending encoder below and by piecewise (no-string)
// consumers such as the SPIMI zero-alloc bigram probe.
inline size_t encode_phrase_bigram_varint32(uint32_t value, char* buf) {
    size_t n = 0;
    while (value >= 0x80) {
        buf[n++] = static_cast<char>((value & 0x7F) | 0x80);
        value >>= 7;
    }
    buf[n++] = static_cast<char>(value);
    return n;
}

inline void append_phrase_bigram_varint32(uint32_t value, std::string* out) {
    char buf[5];
    out->append(buf, encode_phrase_bigram_varint32(value, buf));
}

inline std::string make_phrase_bigram_term(std::string_view left, std::string_view right) {
    std::string out;
    out.reserve(kPhraseBigramTermMarker.size() + 5 + left.size() + right.size());
    out.append(kPhraseBigramTermMarker);
    append_phrase_bigram_varint32(static_cast<uint32_t>(left.size()), &out);
    out.append(left);
    out.append(right);
    return out;
}

inline std::string make_phrase_bigram_sentinel_term() {
    std::string out(kPhraseBigramTermMarker);
    out.push_back('\0');
    return out;
}

inline bool is_phrase_bigram_term(std::string_view term) {
    return term.starts_with(kPhraseBigramTermMarker);
}

// The sentinel is marker + '\0'. A REAL bigram term is marker + varint(len(left))
// + left + right with len(left) >= 1, so its first post-marker byte is never 0 --
// the sentinel is unambiguous. The sentinel marks "this index was built with the
// hidden-bigram feature" and must NEVER be df-pruned (it gates reader semantics).
inline bool is_phrase_bigram_sentinel_term(std::string_view term) {
    return term.size() == kPhraseBigramTermMarker.size() + 1 &&
           term.starts_with(kPhraseBigramTermMarker) && term.back() == '\0';
}

// Piecewise equality: does `term` equal make_phrase_bigram_term(left, right)?
// Compares marker / varint(len(left)) / left / right fragment by fragment so the
// caller never composes the synthetic string (the SPIMI zero-alloc intern probe).
inline bool phrase_bigram_term_equals(std::string_view term, std::string_view left,
                                      std::string_view right) {
    if (!term.starts_with(kPhraseBigramTermMarker)) {
        return false;
    }
    char varint_buf[5];
    const size_t varint_len =
            encode_phrase_bigram_varint32(static_cast<uint32_t>(left.size()), varint_buf);
    const size_t expected_size =
            kPhraseBigramTermMarker.size() + varint_len + left.size() + right.size();
    if (term.size() != expected_size) {
        return false;
    }
    size_t off = kPhraseBigramTermMarker.size();
    if (term.substr(off, varint_len) != std::string_view(varint_buf, varint_len)) {
        return false;
    }
    off += varint_len;
    if (term.substr(off, left.size()) != left) {
        return false;
    }
    off += left.size();
    return term.substr(off) == right;
}

// Default phrase-bigram df-prune threshold for a segment of `segment_doc_count`
// docs: max(64, doc_count / 10000) (0.01%). Pure policy (not format semantics);
// the EFFECTIVE threshold a writer applied is recorded in the per-index meta
// (SectionType::kBigramPruneInfo), so readers never re-derive it from this
// formula.
inline uint32_t default_phrase_bigram_prune_min_df(uint64_t segment_doc_count) {
    return static_cast<uint32_t>(
            std::max<uint64_t>(64, std::min<uint64_t>(segment_doc_count / 10000, UINT32_MAX)));
}

inline bool is_ascii_alpha_phrase_bigram_char(char c) {
    return ('a' <= c && c <= 'z') || ('A' <= c && c <= 'Z');
}

inline bool is_phrase_bigram_indexable_term(std::string_view term) {
    constexpr size_t kMinPhraseBigramTermLength = 2;
    constexpr size_t kMaxPhraseBigramTermLength = 32;
    if (term.size() < kMinPhraseBigramTermLength || term.size() > kMaxPhraseBigramTermLength) {
        return false;
    }
    for (const char c : term) {
        if (!is_ascii_alpha_phrase_bigram_char(c)) {
            return false;
        }
    }
    return true;
}

} // namespace doris::snii::format
