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

#include "olap/rowset/segment_v2/inverted_index/token_filter/token_filter.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_config.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/term_item.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

class PinyinFilter : public DorisTokenFilter {
public:
    PinyinFilter(const TokenStreamPtr& in, std::shared_ptr<PinyinConfig> config);

    ~PinyinFilter() override = default;

    void initialize();

    Token* next(Token* token) override;

    void reset() override;

private:
    struct RuneInfo {
        UChar32 cp;
        int32_t byte_start;
        int32_t byte_end;
    };

    bool processCurrentToken();

    bool readTerm(Token* token);

    void resetVariables();

    void addCandidate(const TermItem& item);

    void processAsciiBuffer(const std::string& ascii_buffer, int start_pos, int end_pos);

    void setTokenAttributes(Token* token, const std::string& term, int startOffset, int endOffset,
                            int position);

    std::string trim(const std::string& str);

    std::vector<RuneInfo> convertToRunes(const std::string& text, std::vector<UChar32>& codepoints);

private:
    // Configuration
    std::shared_ptr<PinyinConfig> config_;

    // Processing state flags
    bool done_;
    bool processed_candidate_;
    bool processed_first_letter_;
    bool processed_full_pinyin_letter_;
    bool processed_original_;
    bool processed_sort_candidate_;

    // Position and offset tracking
    int position_;
    int last_offset_;
    int last_increment_position_;

    // Token candidates and filtering
    std::vector<TermItem> candidate_;
    std::unordered_set<std::string> terms_filter_;
    size_t candidate_offset_;

    // Pinyin processing buffers
    std::string first_letters_;
    std::string full_pinyin_letters_;
    std::string current_source_;

    // Current token being processed
    bool has_current_token_;
    std::string current_token_text_;
    int current_start_offset_;
    int current_end_offset_;
};

using PinyinFilterPtr = std::shared_ptr<PinyinFilter>;

} // namespace doris::segment_v2::inverted_index
