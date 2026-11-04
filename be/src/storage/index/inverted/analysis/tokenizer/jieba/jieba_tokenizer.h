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

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_scratch.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

namespace jieba {
class Jieba;
} // namespace jieba

// The dictionaries of the chinese parser in `dict_dir`: jieba.dict.utf8, hmm_model.utf8,
// user.dict.utf8 and stop_words.utf8. They are loaded at the first call for a directory and shared
// by every stream after it; a load that fails throws, and the next call tries again.
std::shared_ptr<const jieba::Jieba> load_jieba(const std::string& dict_dir);

// The tokenizer of the builtin chinese parser, which makes the tokens CLucene's ChineseTokenizer
// made: jieba's words of the value, cut by MixSegment with the HMM in the coarse mode and by
// FullSegment, which hands out every dictionary word it finds, in the fine mode, with jieba's stop
// words dropped before anything else happens to them. A value jieba cannot decode has no token. A
// word is cut to MAX_WORD_LEN bytes, which may split a character, and every word is one position
// after the last.
//
// With `lowercase`, a word whose first byte is ASCII is lowercased whole, and a word that starts with
// another byte is left as it is, capitals included: with "卡拉OK" in the user dictionary, the coarse
// mode makes "卡拉OK", and "Q版本" makes "q版本". The words are lowercased where they lie, in the copy
// of the value, when they are handed out, as the old tokenizer did to its buffer, so a word sees the
// lowercasing of the words handed out before it. That matters where the fine mode's words overlap:
// it hands out a run of ASCII letters and digits whole and then the runs of letters and of digits
// in it, and jieba reads a stray continuation byte and the byte after it as one character, so
// "" "7abcB" is the run "7abcB", which starts with a byte that is not ASCII and keeps its
// capital, followed by "abcB", which lowers it. A batch holds many words at once, so a word that
// still has a capital after its turn is copied into the batch, where a later word cannot lower it.
class JiebaTokenizer final : public TextTokenizer {
public:
    JiebaTokenizer(std::shared_ptr<const jieba::Jieba> jieba, bool fine, bool lowercase);
    ~JiebaTokenizer() override;

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The copy of the value, the list of its words and what jieba cut them with.
    size_t reserved_bytes() const override {
        return _value.capacity() + _words.capacity() * sizeof(std::string_view) +
               _scratch.reserved_bytes();
    }

private:
    const std::shared_ptr<const jieba::Jieba> _jieba;
    const bool _fine;
    const bool _lowercase;
    // The value, which jieba cuts and lowercasing rewrites; the words are views of it.
    std::string _value;
    std::vector<std::string_view> _words;
    // The runes of the value and the arrays of the cut, kept for the next value.
    jieba::JiebaScratch _scratch;
    size_t _next = 0;
};

// The tokenizer of the builtin chinese parser, which is no custom tokenizer. It loads the
// dictionaries when it is made, so that a missing file fails the analyzer, not its first stream.
class JiebaTokenizerFactory final : public TokenizerFactory {
public:
    JiebaTokenizerFactory(bool fine, bool lowercase, const std::string& dict_dir);

    void initialize(const Settings& settings) override {}

    std::unique_ptr<TextTokenizer> create() const override {
        return std::make_unique<JiebaTokenizer>(_jieba, _fine, _lowercase);
    }

private:
    const bool _fine;
    const bool _lowercase;
    const std::shared_ptr<const jieba::Jieba> _jieba;
};

} // namespace doris::segment_v2::inverted_index
