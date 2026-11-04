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
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_model.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// What the two cuts read, built once from the loaded dictionaries, model and stop words, which
// can be dropped then, and laid out so that every lookup of a cut is an array probe or a short
// scan instead of a hash lookup behind a pointer.
//
// The dictionary is a trie of flat arrays. Node 0 is the root and the other nodes are numbered
// breadth first, so the children of node n are the consecutive nodes child_begin[n] to
// child_begin[n + 1] - 1, in increasing rune, and key[c] is the rune that leads to child c. A
// node at which a word ends has has_value set and the word's weight. Say the words are 中, 中国,
// 中国人 and 人: the root's children are 1 (中) and 2 (人), node 1's child is 3 (国) and node 3's
// is 4 (人); child_begin is {1, 3, 4, 4, 5, 5}, so node 2 has 4 - 4 = no child, and has_value is
// set for nodes 1 to 4.
//
// The trie keeps the words of the units with a word, the last unit of a word where there are
// several, which is the trie of pointers upstream built by inserting the units in order. Nothing
// is cached per value: the cuts keep what a value needs in a JiebaScratch.
class FlatDict {
public:
    // The root, which is nobody's child, also stands for no node.
    static constexpr uint32_t kNoNode = 0;

    FlatDict(const DictTrie& dict_trie, const HMMModel& model, const std::string& stop_word_path)
            : _min_weight(dict_trie.get_min_weight()),
              _start_prob(model.start_prob),
              _trans_prob(model.trans_prob) {
        build_trie(dict_trie.units());
        build_user_single_words(dict_trie.user_dict_single_chinese_words());
        build_emit_prob(model);
        load_stop_words(stop_word_path);
    }
    FlatDict(const FlatDict&) = delete;
    FlatDict& operator=(const FlatDict&) = delete;

    // The child of the root for `rune`, or kNoNode: a table for the first plane, a search among
    // the root's children above it.
    uint32_t root_child(Rune rune) const {
        return rune < kDirectRunes ? _root_children[rune] : child(kNoNode, rune);
    }

    // The child of `node` for `rune`, or kNoNode. A node of up to kLinearChildren children is
    // scanned, one of more is searched by bisection.
    uint32_t child(uint32_t node, Rune rune) const {
        const uint32_t begin = _child_begin[node];
        const uint32_t end = _child_begin[node + 1];
        if (end - begin <= kLinearChildren) {
            for (uint32_t c = begin; c < end; ++c) {
                if (_key[c] == rune) {
                    return c;
                }
            }
            return kNoNode;
        }
        const Rune* const first = _key.data() + begin;
        const Rune* const last = _key.data() + end;
        const Rune* const it = std::lower_bound(first, last, rune);
        return it != last && *it == rune ? static_cast<uint32_t>(it - _key.data()) : kNoNode;
    }

    // Whether a word ends at `node`, and its weight.
    bool has_value(uint32_t node) const { return _has_value[node] != 0; }
    double weight(uint32_t node) const { return _weight[node]; }

    // The weight of a rune that is no word: the smallest weight of the main dictionary.
    double min_weight() const { return _min_weight; }

    // Whether a user dictionary has `rune` alone as a word, upstream's
    // IsUserDictSingleChineseWord.
    bool is_user_single_word(Rune rune) const {
        if (rune < kDirectRunes) {
            return ((_user_single_words[rune / 64] >> (rune % 64)) & 1U) != 0;
        }
        return _user_single_supplementary.contains(rune);
    }

    const std::array<double, HMMModel::STATUS_SUM>& start_prob() const { return _start_prob; }
    const std::array<std::array<double, HMMModel::STATUS_SUM>, HMMModel::STATUS_SUM>& trans_prob()
            const {
        return _trans_prob;
    }

    // The emission probabilities of `rune` for B, E, M and S, in this order, with MIN_DOUBLE for a
    // status the model gives no probability for `rune`, as upstream's GetEmitProb.
    const double* emit_prob(Rune rune) const {
        if (rune < kDirectRunes) {
            return _emit_prob.data() + size_t {rune} * HMMModel::STATUS_SUM;
        }
        const auto it = _supplementary_emit_prob.find(rune);
        return it == _supplementary_emit_prob.end() ? kNoEmitProb.data() : it->second.data();
    }

    // Whether `word` is a line of the stop word file. A word of up to kPackedStopWordBytes bytes
    // is probed for in an open-addressing table keyed by its bytes, a longer one searched for among
    // the longer stop words.
    bool is_stop_word(std::string_view word) const {
        if (word.size() > _longest_stop_word) {
            return false;
        }
        if (word.size() > kPackedStopWordBytes) {
            return std::ranges::binary_search(_long_stop_words, word);
        }
        const uint64_t bytes = pack_bytes(word);
        const size_t mask = _stop_word_slots.size() - 1;
        for (size_t slot = stop_word_slot(bytes, word.size());; slot = (slot + 1) & mask) {
            const StopWordSlot& entry = _stop_word_slots[slot];
            if (entry.size == 0) {
                return false;
            }
            if (entry.bytes == bytes && entry.size == word.size()) {
                return true;
            }
        }
    }

private:
    // The runes below this have a table of their own: the first rune of a word, whether a user
    // word is that rune alone, and the emission probabilities.
    static constexpr Rune kDirectRunes = 0x10000;
    static constexpr uint32_t kLinearChildren = 8;
    static constexpr size_t kPackedStopWordBytes = 8;
    // 30 times the 542 distinct stop words of up to 8 bytes in the shipped file, so that a probe
    // nearly always ends at its first slot; a file with more of them gets a table of at least
    // twice as many slots as words, so that a probe always meets an empty slot.
    static constexpr size_t kMinStopWordSlots = size_t {1} << 14;
    static constexpr std::array<double, HMMModel::STATUS_SUM> kNoEmitProb = {
            MIN_DOUBLE, MIN_DOUBLE, MIN_DOUBLE, MIN_DOUBLE};

    // A stop word of up to 8 bytes; an empty slot has size 0, which no stop word in the table has.
    struct StopWordSlot {
        uint64_t bytes = 0;
        uint64_t size = 0;
    };

    // The units the trie keeps, by word, breadth first. For each node the words below it are a
    // run of the sorted words, and those below a child are the run that has the child's rune
    // after the node's runes; a word that ends at the node sorts first in its run.
    void build_trie(const std::vector<DictUnit>& units) {
        std::vector<uint32_t> words;
        words.reserve(units.size());
        for (uint32_t i = 0; i < units.size(); ++i) {
            if (!units[i].word.empty()) {
                words.push_back(i);
            }
        }
        // Stable, so that the units of one word stay in the order of the files; the last of them
        // is the one upstream's trie kept.
        std::ranges::stable_sort(words, [&units](uint32_t lhs, uint32_t rhs) {
            return units[lhs].word < units[rhs].word;
        });
        size_t kept = 0;
        for (size_t i = 0; i < words.size(); ++i) {
            if (i + 1 == words.size() || units[words[i]].word != units[words[i + 1]].word) {
                words[kept++] = words[i];
            }
        }
        words.resize(kept);

        // The words below each node: words[begin, end), which all start with the node's `depth`
        // runes. Nodes are appended in the order they are numbered, so that a node is handled
        // after every node of a smaller number has appended its children.
        struct Below {
            uint32_t begin;
            uint32_t end;
            uint32_t depth;
        };
        std::vector<Below> below;
        below.push_back({.begin = 0, .end = static_cast<uint32_t>(words.size()), .depth = 0});
        _key.push_back(0);
        _has_value.push_back(0);
        _weight.push_back(0.0);
        for (size_t node = 0; node < below.size(); ++node) {
            auto [begin, end, depth] = below[node];
            _child_begin.push_back(static_cast<uint32_t>(below.size()));
            if (begin < end && units[words[begin]].word.size() == depth) {
                ++begin;
            }
            while (begin < end) {
                const DictUnit& first = units[words[begin]];
                const Rune rune = first.word[depth];
                uint32_t run_end = begin + 1;
                while (run_end < end && units[words[run_end]].word[depth] == rune) {
                    ++run_end;
                }
                const bool ends_here = first.word.size() == depth + 1;
                below.push_back({.begin = begin, .end = run_end, .depth = depth + 1});
                _key.push_back(rune);
                _has_value.push_back(ends_here ? 1 : 0);
                _weight.push_back(ends_here ? first.weight : 0.0);
                begin = run_end;
            }
        }
        _child_begin.push_back(static_cast<uint32_t>(below.size()));
        _child_begin.shrink_to_fit();
        _key.shrink_to_fit();
        _has_value.shrink_to_fit();
        _weight.shrink_to_fit();

        _root_children.assign(kDirectRunes, kNoNode);
        for (uint32_t c = _child_begin[kNoNode]; c < _child_begin[kNoNode + 1]; ++c) {
            if (_key[c] < kDirectRunes) {
                _root_children[_key[c]] = c;
            }
        }
    }

    void build_user_single_words(const std::unordered_set<Rune>& runes) {
        _user_single_words.assign(kDirectRunes / 64, 0);
        for (const Rune rune : runes) {
            if (rune < kDirectRunes) {
                _user_single_words[rune / 64] |= uint64_t {1} << (rune % 64);
            } else {
                _user_single_supplementary.insert(rune);
            }
        }
    }

    // Rune by rune instead of status by status, so that the four probabilities Viterbi reads for
    // a rune are next to each other.
    void build_emit_prob(const HMMModel& model) {
        _emit_prob.assign(size_t {kDirectRunes} * HMMModel::STATUS_SUM, MIN_DOUBLE);
        for (size_t status = 0; status < HMMModel::STATUS_SUM; ++status) {
            for (const auto& [rune, prob] : *model.emit_prob_vec[status]) {
                if (rune < kDirectRunes) {
                    _emit_prob[size_t {rune} * HMMModel::STATUS_SUM + status] = prob;
                } else {
                    // A status the model has no pair of the rune for keeps MIN_DOUBLE.
                    auto& probs =
                            _supplementary_emit_prob.try_emplace(rune, kNoEmitProb).first->second;
                    probs[status] = prob;
                }
            }
        }
    }

    // A stop word file that cannot be opened leaves no stop word, as upstream. Every line is a
    // stop word as it is, untrimmed; an empty line is one too, but no word is empty, so it is not
    // kept.
    void load_stop_words(const std::string& file_path) {
        std::vector<std::string> short_words;
        std::ifstream ifs(file_path);
        if (ifs.is_open()) {
            std::string line;
            while (std::getline(ifs, line)) {
                _longest_stop_word = std::max(_longest_stop_word, line.size());
                if (line.size() > kPackedStopWordBytes) {
                    _long_stop_words.push_back(line);
                } else if (!line.empty()) {
                    short_words.push_back(line);
                }
            }
        }
        std::ranges::sort(_long_stop_words);
        const auto [last, end] = std::ranges::unique(_long_stop_words);
        _long_stop_words.erase(last, end);

        size_t slots = kMinStopWordSlots;
        while (slots < 2 * short_words.size()) {
            slots *= 2;
        }
        _stop_word_slots.assign(slots, StopWordSlot {});
        _stop_word_shift = 64 - std::countr_zero(slots);
        for (const std::string& word : short_words) {
            const uint64_t bytes = pack_bytes(word);
            size_t slot = stop_word_slot(bytes, word.size());
            while (_stop_word_slots[slot].size != 0 &&
                   (_stop_word_slots[slot].bytes != bytes ||
                    _stop_word_slots[slot].size != word.size())) {
                slot = (slot + 1) & (slots - 1);
            }
            _stop_word_slots[slot].bytes = bytes;
            _stop_word_slots[slot].size = word.size();
        }
    }

    // The bytes of a word of at most 8 bytes as one integer, the first byte lowest.
    static uint64_t pack_bytes(std::string_view word) {
        uint64_t bytes = 0;
        for (size_t i = 0; i < word.size(); ++i) {
            bytes |= uint64_t {static_cast<uint8_t>(word[i])} << (8 * i);
        }
        return bytes;
    }

    // Fibonacci hashing: the top bits of the product, as many as the table has slots.
    size_t stop_word_slot(uint64_t bytes, size_t size) const {
        return static_cast<size_t>(((bytes ^ size) * 0x9E3779B97F4A7C15ULL) >> _stop_word_shift);
    }

    // The trie.
    std::vector<uint32_t> _child_begin;
    std::vector<Rune> _key;
    std::vector<uint8_t> _has_value;
    std::vector<double> _weight;
    std::vector<uint32_t> _root_children;
    double _min_weight = 0.0;

    // The user words of one rune: a bit for each rune of the first plane, a set above it.
    std::vector<uint64_t> _user_single_words;
    std::unordered_set<Rune> _user_single_supplementary;

    // The HMM.
    std::array<double, HMMModel::STATUS_SUM> _start_prob;
    std::array<std::array<double, HMMModel::STATUS_SUM>, HMMModel::STATUS_SUM> _trans_prob;
    std::vector<double> _emit_prob;
    std::unordered_map<Rune, std::array<double, HMMModel::STATUS_SUM>> _supplementary_emit_prob;

    // The stop words.
    std::vector<StopWordSlot> _stop_word_slots;
    int _stop_word_shift = 0;
    std::vector<std::string> _long_stop_words;
    size_t _longest_stop_word = 0;
};

} // namespace doris::segment_v2::inverted_index::jieba
