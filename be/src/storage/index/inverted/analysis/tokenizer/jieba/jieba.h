// Derived from cppjieba's Jieba.hpp (https://github.com/yanyiwu/cppjieba), MIT License,
// Copyright (c) 2013. See dist/licenses/LICENSE-cppjieba.txt.

#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/full_segment.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_model.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/mix_segment.h"

namespace doris::segment_v2::inverted_index::jieba {

// The dictionary trie, the HMM model and the stop words, loaded once, and the two segmenters of the
// chinese parser, which share the trie and the model. The members are built in upstream's order:
// the main dictionary and the user dictionaries, the model, then MixSegment and FullSegment, each
// with its own copy of the stop words. Loading throws INVERTED_INDEX_ANALYZER_ERROR when the
// dictionary, a user dictionary or the model cannot be opened or has an illegal line; a stop word
// file that cannot be opened leaves no stop word. Unlike upstream it reads no idf file and has no
// keyword extractor, whose loading also failed on a stop word file that could not be opened.
//
// Cutting is const and keeps nothing between calls, so one Jieba serves every thread.
class Jieba {
public:
    Jieba(const std::string& dict_path, const std::string& model_path,
          const std::string& user_dict_path, const std::string& stop_word_path)
            : _dict_trie(dict_path, user_dict_path),
              _model(model_path),
              _mix_seg(&_dict_trie, &_model, stop_word_path),
              _full_seg(&_dict_trie, stop_word_path) {}
    // The segments point at _dict_trie and _model.
    Jieba(const Jieba&) = delete;
    Jieba& operator=(const Jieba&) = delete;

    // The coarse mode, upstream's Cut with the HMM. The words are views of `sentence`.
    void cut(const std::string& sentence, std::vector<std::string_view>& words) const {
        _mix_seg.cut(sentence, words, /*hmm=*/true);
    }

    // The fine mode, upstream's CutAll. The words are views of `sentence`.
    void cut_all(const std::string& sentence, std::vector<std::string_view>& words) const {
        _full_seg.cut(sentence, words);
    }

private:
    DictTrie _dict_trie;
    HMMModel _model;

    // They share the same dict trie and model
    MixSegment _mix_seg;
    FullSegment _full_seg;
};

} // namespace doris::segment_v2::inverted_index::jieba
