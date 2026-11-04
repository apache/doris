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

#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/tokenizer/pinyin/pinyin_config.h"
#include "storage/index/inverted/analysis/tokenizer/pinyin/term_item.h"
#include "unicode/uchar.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

// Turns every token into pinyin candidates: per-character pinyin, first letters, joined pinyin
// and the original, as configured.
class PinyinFilter final : public PerTokenFilter {
public:
    PinyinFilter(std::unique_ptr<TextTokenStream> in, std::shared_ptr<PinyinConfig> config);
    ~PinyinFilter() override = default;

    void initialize();

    // The candidates of the current token with their buffers, and the input.
    size_t reserved_bytes() const override;

protected:
    void on_reset() override;
    bool next_token(TokenRef& token) override;

private:
    bool processCurrentToken();
    bool prepareCurrentSource(std::vector<UChar32>& source_codepoints);
    bool readTerm(TokenRef& token);
    void resetVariables();
    void addCandidate(const TermItem& item);
    void processAsciiBuffer(const std::string& ascii_buffer);
    void setTokenAttributes(TokenRef& token, const std::string& term, int position);
    std::string trim(const std::string& str);
    void convertToCodepoints(const std::string& text, std::vector<UChar32>& codepoints);

    // Configuration
    std::shared_ptr<PinyinConfig> config_;

    // Processing state flags
    bool done_;
    bool processed_candidate_;
    bool processed_first_letter_;
    bool processed_full_pinyin_letter_;
    bool processed_original_;
    bool processed_sort_candidate_;

    // Position tracking
    int position_;
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
};
using PinyinFilterPtr = std::shared_ptr<PinyinFilter>;

} // namespace doris::segment_v2::inverted_index
