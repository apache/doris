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

#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/tokenizer/jieba/dict_trie.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/flat_dict.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/hmm_model.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_scratch.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/segmenter.h"

namespace doris::segment_v2::inverted_index::jieba {

// The dictionaries of the chinese parser, loaded once, and its two cuts. Loading reads the main
// dictionary and the user dictionaries, then the model, then the stop words, and throws
// INVERTED_INDEX_ANALYZER_ERROR when the dictionary, a user dictionary or the model cannot be
// opened or has an illegal line; a stop word file that cannot be opened leaves no stop word. The
// loaders' results are laid out in a FlatDict and dropped once it is built. Unlike CLucene's
// Jieba it reads no idf file and has no keyword extractor, whose loading also failed on a stop
// word file that could not be opened.
//
// Cutting is const and keeps what it needs in the caller's scratch, so one Jieba serves every
// thread.
class Jieba {
public:
    Jieba(const std::string& dict_path, const std::string& model_path,
          const std::string& user_dict_path, const std::string& stop_word_path)
            : _dict(load(dict_path, model_path, user_dict_path, stop_word_path)) {}
    Jieba(const Jieba&) = delete;
    Jieba& operator=(const Jieba&) = delete;

    // The coarse mode, CLucene's Cut with the HMM. The words are views of `sentence`.
    void cut(std::string_view sentence, JiebaScratch& scratch,
             std::vector<std::string_view>& words) const {
        Segmenter(_dict, sentence, scratch, words).mix_cut();
    }

    // The fine mode, CLucene's CutAll. The words are views of `sentence`.
    void cut_all(std::string_view sentence, JiebaScratch& scratch,
                 std::vector<std::string_view>& words) const {
        Segmenter(_dict, sentence, scratch, words).full_cut();
    }

    // The same with a scratch of their own, for a caller that cuts once.
    void cut(std::string_view sentence, std::vector<std::string_view>& words) const {
        JiebaScratch scratch;
        cut(sentence, scratch, words);
    }
    void cut_all(std::string_view sentence, std::vector<std::string_view>& words) const {
        JiebaScratch scratch;
        cut_all(sentence, scratch, words);
    }

private:
    // In this order, as CLucene's Jieba built its members, so that a bad dictionary fails the
    // load before a bad model does.
    static FlatDict load(const std::string& dict_path, const std::string& model_path,
                         const std::string& user_dict_path, const std::string& stop_word_path) {
        const DictTrie dict_trie(dict_path, user_dict_path);
        const HMMModel model(model_path);
        return {dict_trie, model, stop_word_path};
    }

    const FlatDict _dict;
};

} // namespace doris::segment_v2::inverted_index::jieba
