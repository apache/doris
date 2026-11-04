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

#include "storage/index/inverted/analysis/tokenizer/jieba/jieba_tokenizer.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"
#include "storage/index/inverted/analysis/tokenizer/jieba/jieba.h"

namespace doris::segment_v2::inverted_index {

namespace {

bool has_ascii_upper(std::string_view term) {
    return std::ranges::any_of(term,
                               [](char c) { return is_ascii_upper(static_cast<uint8_t>(c)); });
}

} // namespace

std::shared_ptr<const jieba::Jieba> load_jieba(const std::string& dict_dir) {
    static std::mutex mutex;
    static std::unordered_map<std::string, std::shared_ptr<const jieba::Jieba>> loaded;
    std::lock_guard lock(mutex);
    if (const auto it = loaded.find(dict_dir); it != loaded.end()) {
        return it->second;
    }
    // idf.utf8, which CLucene's keyword extractor read, is no longer needed. The segmenters skip a
    // stop word file they cannot open, which the extractor refused, so an unreadable file is
    // refused here.
    for (const char* name :
         {"jieba.dict.utf8", "hmm_model.utf8", "user.dict.utf8", "stop_words.utf8"}) {
        const std::string path = dict_dir + "/" + name;
        if (!std::ifstream(path).is_open()) {
            throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                            "chinese tokenizer dict file not found: " + path);
        }
    }
    auto jieba = std::make_shared<const jieba::Jieba>(
            dict_dir + "/jieba.dict.utf8", dict_dir + "/hmm_model.utf8",
            dict_dir + "/user.dict.utf8", dict_dir + "/stop_words.utf8");
    loaded.emplace(dict_dir, jieba);
    return jieba;
}

JiebaTokenizer::JiebaTokenizer(std::shared_ptr<const jieba::Jieba> jieba, bool fine, bool lowercase)
        : _jieba(std::move(jieba)), _fine(fine), _lowercase(lowercase) {}

JiebaTokenizer::~JiebaTokenizer() = default;

void JiebaTokenizer::reset(std::string_view text) {
    _next = 0;
    release_oversized_scratch(_value);
    release_oversized_scratch(_words);
    _scratch.release_oversized();
    _value.assign(text);
    if (_fine) {
        _jieba->cut_all(_value, _scratch, _words);
    } else {
        _jieba->cut(_value, _scratch, _words);
    }
}

bool JiebaTokenizer::next_batch(TokenBatch& batch) {
    batch.clear();
    while (_next < _words.size() && batch.size() < kBatchTokens) {
        const std::string_view word = _words[_next++];
        if (_lowercase && !word.empty() && static_cast<uint8_t>(word[0]) < 0x80) {
            // The whole word, even past what the term keeps: an overlapping word reads those bytes.
            char* const begin = _value.data() + (word.data() - _value.data());
            for (size_t i = 0; i < word.size(); ++i) {
                begin[i] = ascii_lower(begin[i]);
            }
        }
        const std::string_view term = word.substr(0, MAX_WORD_LEN);
        if (_lowercase && has_ascii_upper(term)) {
            batch.push_copy(term, 1);
        } else {
            batch.push(term, 1);
        }
    }
    return batch.size() != 0;
}

JiebaTokenizerFactory::JiebaTokenizerFactory(bool fine, bool lowercase, const std::string& dict_dir)
        : _fine(fine), _lowercase(lowercase), _jieba(load_jieba(dict_dir)) {}

} // namespace doris::segment_v2::inverted_index
