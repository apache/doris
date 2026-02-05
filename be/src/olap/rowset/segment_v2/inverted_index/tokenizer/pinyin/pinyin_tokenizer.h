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

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "olap/rowset/segment_v2/inverted_index/tokenizer/tokenizer.h"
#include "pinyin_config.h"
#include "rune.h"
#include "term_item.h"
#include "unicode/uchar.h"

namespace doris::segment_v2::inverted_index {

class PinyinTokenizer : public DorisTokenizer {
public:
    PinyinTokenizer();
    PinyinTokenizer(std::shared_ptr<doris::segment_v2::PinyinConfig> config);
    ~PinyinTokenizer() override = default;

    Token* next(Token* token) override;
    void reset() override;

private:
    bool done_;
    bool processed_candidate_;
    bool processed_sort_candidate_;
    bool processed_first_letter_;
    bool processed_full_pinyin_letter_;
    bool processed_original_;

    // Position tracking (ported from Java)
    int position_;
    int last_offset_;
    int candidate_offset_; // Indicate candidates process offset
    int last_increment_position_;

    // Configuration
    std::shared_ptr<doris::segment_v2::PinyinConfig> config_;

    std::vector<TermItem> candidate_;
    std::unordered_set<std::string> terms_filter_;
    std::string first_letters_;
    std::string full_pinyin_letters_;
    std::vector<UChar32>
            source_codepoints_; // Unicode code point sequence, replacing original UTF-8 string

    const char* _char_buffer {nullptr};
    int32_t _char_length {0};

    std::vector<Rune> runes_;

    bool hasMoreTokens() const;
    void addCandidate(const TermItem& item);

    void setTerm(std::string term, int start_offset, int end_offset, int position);
    void addCandidate(const std::string& term, int start_offset, int end_offset, int position);

    void decode_to_runes();
    void processInput();
    void parseBuff(std::string& ascii_buff, int& ascii_buff_start_byte);

    std::string codepointsToUtf8(const std::vector<UChar32>& codepoints) const;
};

} // namespace doris::segment_v2::inverted_index