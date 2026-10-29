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

#include "storage/index/inverted/token_filter/token_filter.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_config.h"
#include "storage/index/inverted/tokenizer/pinyin/term_item.h"
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

    // Provenance of the most recently emitted candidate, never the input token's map.
    std::span<const int32_t> get_source_byte_offsets() const override {
        return published_source_byte_offsets_;
    }
    std::span<const int32_t> get_source_byte_end_offsets() const override {
        return published_source_byte_end_offsets_;
    }
    bool get_conservative_source_byte_span(int32_t& start, int32_t& end) const override;

#ifdef BE_TEST
    size_t last_ascii_rune_index_capacity_for_test() const {
        return last_ascii_rune_index_capacity_;
    }
    size_t current_runes_capacity_for_test() const { return current_runes_.capacity(); }
    size_t current_source_offsets_capacity_for_test() const {
        return current_source_byte_offsets_.capacity();
    }
    size_t current_token_capacity_for_test() const { return current_token_text_.capacity(); }
    size_t current_source_capacity_for_test() const { return current_source_.capacity(); }
#endif

private:
    struct RuneInfo {
        UChar32 cp;
        int32_t byte_start;
        int32_t byte_end;
    };

    bool processCurrentToken();

    bool prepareCurrentSource(std::vector<UChar32>& source_codepoints);

    bool readTerm(Token* token);

    void resetVariables();

    void addCandidate(const TermItem& item);

    void processAsciiBuffer(const std::string& ascii_buffer,
                            const std::vector<int>& source_rune_indices);

    void setTokenAttributes(Token* token, const std::string& term, int startOffset, int endOffset,
                            int position);

    void publishCandidateProvenance(const std::string& term, bool is_whole_token,
                                    int32_t source_length);

    std::string trim(const std::string& str);

    std::vector<RuneInfo> convertToRunes(const std::string& text, std::vector<UChar32>& codepoints);

    void convertToCodepoints(const std::string& text, std::vector<UChar32>& codepoints);

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
    std::vector<RuneInfo> current_runes_;
    std::vector<int32_t> current_source_byte_offsets_;
    std::vector<int32_t> current_source_byte_end_offsets_;
    int32_t current_conservative_source_start_ = 0;
    int32_t current_conservative_source_end_ = 0;
    bool has_current_conservative_source_span_ = false;

    // Provenance published for the most recently emitted candidate
    std::vector<int32_t> published_source_byte_offsets_;
    std::vector<int32_t> published_source_byte_end_offsets_;
    int32_t published_source_length_ = 0;
    bool has_published_token_ = false;
#ifdef BE_TEST
    size_t last_ascii_rune_index_capacity_ = 0;
#endif
};

using PinyinFilterPtr = std::shared_ptr<PinyinFilter>;

} // namespace doris::segment_v2::inverted_index
