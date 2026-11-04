// Derived from cppjieba's MPSegment.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <cstddef>
#include <vector>

#include "common/check.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// Maximum-probability segmentation of a range over the dictionary: of all the ways to cut the range
// into dictionary words and single runes, the one with the largest sum of weights, a single rune
// that is no word weighing the smallest weight of the dictionary. Only the range cut MixSegment
// calls is kept; it does not use the separators, so this is no SegmentBase any more.
class MPSegment {
public:
    explicit MPSegment(const DictTrie* dict_trie) : _dict_trie(dict_trie) {
        DORIS_CHECK(_dict_trie != nullptr);
    }

    void cut(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
             std::vector<WordRange>& words, size_t max_word_len = MAX_WORD_LENGTH) const {
        std::vector<Dag> dags;
        _dict_trie->find(begin, end, dags, max_word_len);
        calc_dp(dags);
        cut_by_dag(begin, end, dags, words);
    }

    bool is_user_dict_single_chinese_word(const Rune& value) const {
        return _dict_trie->is_user_dict_single_chinese_word(value);
    }

private:
    // From the last rune to the first, the best weight of the rest of the range from each rune and
    // the word that starts it. The weight of a word is added to the best weight after it, in that
    // order, and the strict > keeps the first of equal sums; the words come in increasing end, so
    // a tie goes to the shorter word.
    void calc_dp(std::vector<Dag>& dags) const {
        for (auto rit = dags.rbegin(); rit != dags.rend(); rit++) {
            rit->p_info = nullptr;
            rit->weight = MIN_DOUBLE;
            DCHECK(!rit->nexts.empty());
            for (const auto& [next_pos, p] : rit->nexts) {
                double val = 0.0;
                if (next_pos + 1 < dags.size()) {
                    val += dags[next_pos + 1].weight;
                }

                if (p != nullptr) {
                    val += p->weight;
                } else {
                    val += _dict_trie->get_min_weight();
                }
                if (val > rit->weight) {
                    rit->p_info = p;
                    rit->weight = val;
                }
            }
        }
    }

    void cut_by_dag(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
                    const std::vector<Dag>& dags, std::vector<WordRange>& words) const {
        size_t i = 0;
        while (i < dags.size()) {
            const DictUnit* p = dags[i].p_info;
            if (p != nullptr) {
                DCHECK(!p->word.empty());
                words.emplace_back(begin + i, begin + i + p->word.size() - 1);
                i += p->word.size();
            } else { //single chinese word
                words.emplace_back(begin + i, begin + i);
                i++;
            }
        }
    }

    const DictTrie* _dict_trie;
};

} // namespace doris::segment_v2::inverted_index::jieba
