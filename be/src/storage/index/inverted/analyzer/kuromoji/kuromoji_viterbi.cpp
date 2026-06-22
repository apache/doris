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

#include "storage/index/inverted/analyzer/kuromoji/kuromoji_viterbi.h"

#include <algorithm>
#include <cstddef>
#include <limits>

namespace doris::segment_v2::kuromoji {

namespace {

constexpr int64_t KMJ_INF = std::numeric_limits<int64_t>::max() / 4;
constexpr uint32_t MAX_UNKNOWN_GROUP_CHARS = 1024;

// Search/Extended-mode compound-decomposition penalties, matching Lucene's
// JapaneseTokenizer. Lengths are counted in code points. A token longer than the
// length threshold is penalized so the minimum-cost path prefers its shorter
// parts: all-kanji runs over KANJI_LENGTH chars, other runs over OTHER_LENGTH.
constexpr uint32_t SEARCH_MODE_KANJI_LENGTH = 2;
constexpr int64_t SEARCH_MODE_KANJI_PENALTY = 3000;
constexpr uint32_t SEARCH_MODE_OTHER_LENGTH = 7;
constexpr int64_t SEARCH_MODE_OTHER_PENALTY = 1700;

struct DecodedCp {
    char32_t cp;
    uint32_t len;
};

// Decode one UTF-8 code point at text[pos]. Invalid/truncated -> single byte.
DecodedCp decode_utf8(std::string_view text, std::size_t pos) {
    auto b0 = static_cast<unsigned char>(text[pos]);
    const std::size_t avail = text.size() - pos;
    if (b0 < 0x80) {
        return {b0, 1};
    }
    if ((b0 >> 5) == 0x6 && avail >= 2) {
        auto b1 = static_cast<unsigned char>(text[pos + 1]);
        return {static_cast<char32_t>(((b0 & 0x1FU) << 6) | (b1 & 0x3FU)), 2};
    }
    if ((b0 >> 4) == 0xE && avail >= 3) {
        auto b1 = static_cast<unsigned char>(text[pos + 1]);
        auto b2 = static_cast<unsigned char>(text[pos + 2]);
        return {static_cast<char32_t>(((b0 & 0x0FU) << 12) | ((b1 & 0x3FU) << 6) | (b2 & 0x3FU)),
                3};
    }
    if ((b0 >> 3) == 0x1E && avail >= 4) {
        auto b1 = static_cast<unsigned char>(text[pos + 1]);
        auto b2 = static_cast<unsigned char>(text[pos + 2]);
        auto b3 = static_cast<unsigned char>(text[pos + 3]);
        return {static_cast<char32_t>(((b0 & 0x07U) << 18) | ((b1 & 0x3FU) << 12) |
                                      ((b2 & 0x3FU) << 6) | (b3 & 0x3FU)),
                4};
    }
    return {b0, 1};
}

// Lucene JapaneseTokenizer's search-mode penalty for the token covering
// [start, end) bytes: penalize long compounds so the Viterbi prefers their
// shorter parts. Returns 0 for tokens at or under the length thresholds.
int64_t compute_penalty(const KuromojiDictionary& dict, std::string_view text, uint32_t start,
                        uint32_t end) {
    uint32_t length = 0;
    bool all_kanji = true;
    for (uint32_t p = start; p < end;) {
        const DecodedCp d = decode_utf8(text, p);
        if (dict.char_category(d.cp) != CAT_KANJI) {
            all_kanji = false;
        }
        p += d.len;
        ++length;
    }
    if (length > SEARCH_MODE_KANJI_LENGTH) {
        if (all_kanji) {
            return static_cast<int64_t>(length - SEARCH_MODE_KANJI_LENGTH) *
                   SEARCH_MODE_KANJI_PENALTY;
        }
        if (length > SEARCH_MODE_OTHER_LENGTH) {
            return static_cast<int64_t>(length - SEARCH_MODE_OTHER_LENGTH) *
                   SEARCH_MODE_OTHER_PENALTY;
        }
    }
    return 0;
}

// A lattice node spanning [start, end) bytes of the input.
struct VNode {
    uint32_t start;
    uint32_t end;
    int16_t left_id;
    int16_t right_id;
    int16_t word_cost;
    bool known;
    uint32_t word_id;
    int64_t total_cost;
    int back; // previous node index, -1 if none
};

} // namespace

void KuromojiViterbi::segment(std::string_view text, std::vector<KuromojiMorpheme>* out) const {
    out->clear();
    const auto n = static_cast<uint32_t>(text.size());
    if (n == 0) {
        return;
    }

    std::vector<VNode> nodes;
    std::vector<std::vector<int>> ending_at(n + 1); // node indices ending at each byte position

    // BOS (index 0): ends at position 0, context id 0, zero cost.
    nodes.push_back(VNode {0, 0, 0, 0, 0, false, 0, 0, -1});
    ending_at[0].push_back(0);

    // Add a node and relax it against all nodes ending at its start position.
    auto add_node = [&](uint32_t s, uint32_t e, int16_t lid, int16_t rid, int16_t wcost, bool known,
                        uint32_t wid) {
        int64_t best = KMJ_INF;
        int best_prev = -1;
        for (int pe : ending_at[s]) {
            const VNode& pv = nodes[static_cast<std::size_t>(pe)];
            if (pv.total_cost >= KMJ_INF) {
                continue;
            }
            const int64_t c =
                    pv.total_cost + _dict.connection_cost(static_cast<uint32_t>(pv.right_id),
                                                          static_cast<uint32_t>(lid));
            if (c < best) {
                best = c;
                best_prev = pe;
            }
        }
        if (best_prev < 0) {
            return;
        }
        // Search/Extended mode penalizes long compounds so shorter parts win.
        const int64_t penalty =
                _mode == KuromojiMode::Normal ? 0 : compute_penalty(_dict, text, s, e);
        const auto idx = static_cast<int>(nodes.size());
        nodes.push_back(
                VNode {s, e, lid, rid, wcost, known, wid, best + wcost + penalty, best_prev});
        ending_at[e].push_back(idx);
    };

    uint32_t pos = 0;
    while (pos < n) {
        if (ending_at[pos].empty()) {
            pos += decode_utf8(text, pos).len; // unreachable boundary; skip
            continue;
        }
        const DecodedCp d0 = decode_utf8(text, pos);
        const auto before = nodes.size();

        // System-dictionary words (common-prefix search).
        std::vector<KuromojiDictionary::PrefixMatch> matches;
        _dict.common_prefix_search(text.data() + pos, n - pos, &matches);
        bool any_known = false;
        for (const auto& mt : matches) {
            const WordIdRun run = _dict.run_for_value(mt.trie_value);
            for (uint32_t k = 0; k < run.count; ++k) {
                const uint32_t wid = run.entry_start + k;
                const WordEntry& e = _dict.word(wid);
                add_node(pos, pos + mt.length, e.left_id, e.right_id, e.word_cost, true, wid);
                any_known = true;
            }
        }

        // Unknown words: when no known word starts here, or the category forces it.
        if (!any_known || _dict.is_invoke(d0.cp)) {
            const uint8_t cat = _dict.char_category(d0.cp);
            uint32_t group_len = d0.len;
            if (_dict.is_group(d0.cp)) {
                uint32_t p = pos + d0.len;
                uint32_t chars = 1;
                while (p < n && chars < MAX_UNKNOWN_GROUP_CHARS) {
                    const DecodedCp dn = decode_utf8(text, p);
                    if (_dict.char_category(dn.cp) != cat) {
                        break;
                    }
                    group_len += dn.len;
                    p += dn.len;
                    ++chars;
                }
            }
            const WordIdRun urun = _dict.unknown_run(cat);
            for (uint32_t k = 0; k < urun.count; ++k) {
                const uint32_t wid = urun.entry_start + k;
                const WordEntry& e = _dict.unknown_word(wid);
                add_node(pos, pos + d0.len, e.left_id, e.right_id, e.word_cost, false, wid);
                if (group_len > d0.len) {
                    add_node(pos, pos + group_len, e.left_id, e.right_id, e.word_cost, false, wid);
                }
            }
        }

        // Connectivity safety net: if nothing covers this reachable position, force a
        // single-character node so the lattice never dead-ends.
        if (nodes.size() == before) {
            add_node(pos, pos + d0.len, 0, 0, std::numeric_limits<int16_t>::max(), false, 0);
        }
        pos += d0.len;
    }

    // EOS: best node ending at n connected to the EOS context (id 0).
    int64_t best = KMJ_INF;
    int best_prev = -1;
    for (int pe : ending_at[n]) {
        const VNode& pv = nodes[static_cast<std::size_t>(pe)];
        if (pv.total_cost >= KMJ_INF) {
            continue;
        }
        const int64_t c =
                pv.total_cost + _dict.connection_cost(static_cast<uint32_t>(pv.right_id), 0);
        if (c < best) {
            best = c;
            best_prev = pe;
        }
    }
    if (best_prev < 0) {
        return; // no path (should not happen given the connectivity net)
    }

    std::vector<int> path;
    for (int cur = best_prev; cur > 0; cur = nodes[static_cast<std::size_t>(cur)].back) {
        path.push_back(cur);
    }
    std::reverse(path.begin(), path.end());
    out->reserve(path.size());
    for (int idx : path) {
        const VNode& nd = nodes[static_cast<std::size_t>(idx)];
        out->push_back(KuromojiMorpheme {nd.start, nd.end - nd.start, nd.known, nd.word_id});
    }
}

} // namespace doris::segment_v2::kuromoji
