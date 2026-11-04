// Derived from cppjieba's MixSegment.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/logging.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_model.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_segment.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/mp_segment.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/pre_filter.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/segment_base.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/unicode.h"

namespace doris::segment_v2::inverted_index::jieba {

// Jieba's default cut: MPSegment over each range between separators, with every run of single
// runes that are not single-rune user words cut again by the HMM. The separators themselves are
// words. Words that are stop words are dropped, as they are, before anything lowercases them.
class MixSegment : public SegmentBase {
public:
    MixSegment(const DictTrie* dict_trie, const HMMModel* model,
               const std::string& stop_word_path = "")
            : _mp_seg(dict_trie), _hmm_seg(model) {
        load_stop_word_dict(stop_word_path);
    }
    // _stop_words views the strings of _stop_word_list.
    MixSegment(const MixSegment&) = delete;
    MixSegment& operator=(const MixSegment&) = delete;

    // The words of `sentence` are views of it.
    void cut(const std::string& sentence, std::vector<std::string_view>& words,
             bool hmm = true) const {
        PreFilter pre_filter(_symbols, sentence);
        std::vector<WordRange> wrs;
        wrs.reserve(sentence.size() / 2);
        while (pre_filter.has_next()) {
            const PreFilter::Range range = pre_filter.next();
            cut(range.begin, range.end, wrs, hmm);
        }
        words.clear();
        words.reserve(wrs.size());
        for (const WordRange& wr : wrs) {
            uint32_t len = wr.right->offset - wr.left->offset + wr.right->len;
            std::string_view word(sentence.data() + wr.left->offset, len);
            if (_stop_words.contains(word)) {
                continue;
            }
            words.emplace_back(word);
        }
    }

    void cut(RuneStrArray::const_iterator begin, RuneStrArray::const_iterator end,
             std::vector<WordRange>& res, bool hmm) const {
        if (!hmm) {
            _mp_seg.cut(begin, end, res);
            return;
        }
        std::vector<WordRange> words;
        DCHECK(end >= begin);
        words.reserve(static_cast<size_t>(end - begin));
        _mp_seg.cut(begin, end, words);

        std::vector<WordRange> hmm_res;
        hmm_res.reserve(static_cast<size_t>(end - begin));
        for (size_t i = 0; i < words.size(); i++) {
            //if mp Get a word, it's ok, put it into result
            if (words[i].left != words[i].right ||
                (words[i].left == words[i].right &&
                 _mp_seg.is_user_dict_single_chinese_word(words[i].left->rune))) {
                res.push_back(words[i]);
                continue;
            }

            // if mp Get a single one and it is not in userdict, collect it in sequence
            size_t j = i;
            while (j < words.size() && words[j].left == words[j].right &&
                   !_mp_seg.is_user_dict_single_chinese_word(words[j].left->rune)) {
                j++;
            }

            // Cut the sequence with hmm
            DCHECK(j - 1 >= i);
            _hmm_seg.cut(words[i].left, words[j - 1].left + 1, hmm_res);
            //put hmm result to result
            for (const WordRange& wr : hmm_res) {
                res.push_back(wr);
            }

            //clear tmp vars
            hmm_res.clear();

            //let i jump over this piece
            i = j - 1;
        }
    }

private:
    // A stop word file that cannot be opened leaves no stop word. Every line is a stop word as it
    // is, untrimmed; an empty line is one too, but no word is empty.
    void load_stop_word_dict(const std::string& file_path) {
        std::ifstream ifs(file_path);
        if (ifs.is_open()) {
            std::string line;
            while (std::getline(ifs, line)) {
                _stop_word_list.push_back(line);
            }
            for (const std::string& word : _stop_word_list) {
                _stop_words.insert(std::string_view(word.data(), word.size()));
            }
        }
    }

    MPSegment _mp_seg;
    HMMSegment _hmm_seg;

    std::vector<std::string> _stop_word_list;
    std::unordered_set<std::string_view> _stop_words;
};

} // namespace doris::segment_v2::inverted_index::jieba
