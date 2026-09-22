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

#include "storage/index/inverted/token_filter/pinyin_filter.h"

#include <algorithm>
#include <iostream>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/tokenizer/pinyin/chinese_util.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_format.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_util.h"

namespace doris::segment_v2::inverted_index {

PinyinFilter::PinyinFilter(const TokenStreamPtr& in, std::shared_ptr<PinyinConfig> config)
        : DorisTokenFilter(in),
          config_(std::move(config)),
          done_(true),
          processed_candidate_(false),
          processed_first_letter_(false),
          processed_full_pinyin_letter_(false),
          processed_original_(false),
          processed_sort_candidate_(false),
          position_(0),
          last_offset_(0),
          last_increment_position_(0),
          candidate_offset_(0),
          has_current_token_(false),
          current_start_offset_(0),
          current_end_offset_(0) {
    if (!config_) {
        config_ = std::make_shared<PinyinConfig>();
    }
}

void PinyinFilter::initialize() {
    // Initialize internal data structures
    candidate_.clear();
    terms_filter_.clear();
    first_letters_.clear();
    full_pinyin_letters_.clear();
}

Token* PinyinFilter::next(Token* token) {
    // Check if input stream is valid
    if (!_in) {
        return nullptr;
    }

    // If current token processing is done, try to get next input token
    if (done_) {
        // Reset state for new token
        resetVariables();

        // Try to get next token from input stream
        if (_in->next(token) == nullptr) {
            return nullptr;
        }

        // Store current token information
        has_current_token_ = true;
        current_token_text_ = std::string(token->termBuffer<char>(), token->termLength<char>());
        current_start_offset_ = token->startOffset();
        current_end_offset_ = token->endOffset();
        if (!config_->ignorePinyinOffset) {
            // Read the upstream token's provenance; the overrides publish this filter's own.
            auto source_byte_offsets = DorisTokenFilter::get_source_byte_offsets();
            current_source_byte_offsets_.assign(source_byte_offsets.begin(),
                                                source_byte_offsets.end());
            auto source_byte_end_offsets = DorisTokenFilter::get_source_byte_end_offsets();
            current_source_byte_end_offsets_.assign(source_byte_end_offsets.begin(),
                                                    source_byte_end_offsets.end());
            has_current_conservative_source_span_ =
                    DorisTokenFilter::get_conservative_source_byte_span(
                            current_conservative_source_start_, current_conservative_source_end_);
        }

        done_ = false;
    }

    // Process current token if needed
    if (!done_) {
        if (readTerm(token)) {
            return token;
        }
        // If readTerm returns false, it means current token processing is complete
        // Mark as done to try next input token on next call
        done_ = true;
        return next(token); // Recursively try next input token
    }

    return nullptr;
}

void PinyinFilter::reset() {
    // Check if input stream is valid before calling parent reset
    if (_in) {
        DorisTokenFilter::reset();
    }

    // Reset all state variables
    done_ = true;
    resetVariables();
    has_current_token_ = false;
    current_token_text_.clear();
    has_published_token_ = false;
    published_source_length_ = 0;
    published_source_byte_offsets_.clear();
    published_source_byte_end_offsets_.clear();
    release_oversized_scratch(published_source_byte_offsets_);
    release_oversized_scratch(published_source_byte_end_offsets_);
    release_oversized_scratch(current_runes_);
    release_oversized_scratch(current_source_byte_offsets_);
    release_oversized_scratch(current_source_byte_end_offsets_);
    release_oversized_scratch(current_token_text_);
    release_oversized_scratch(current_source_);
}

void PinyinFilter::resetVariables() {
    position_ = 0;
    last_offset_ = 0;
    candidate_.clear();
    processed_candidate_ = false;
    processed_first_letter_ = false;
    processed_full_pinyin_letter_ = false;
    processed_original_ = false;
    processed_sort_candidate_ = false;
    first_letters_.clear();
    full_pinyin_letters_.clear();
    current_source_.clear();
    current_runes_.clear();
    current_source_byte_offsets_.clear();
    current_source_byte_end_offsets_.clear();
    current_conservative_source_start_ = 0;
    current_conservative_source_end_ = 0;
    has_current_conservative_source_span_ = false;
    candidate_offset_ = 0;
    terms_filter_.clear();
    last_increment_position_ = 0;
}

bool PinyinFilter::readTerm(Token* token) {
    // Process the current input token to generate pinyin candidates
    if (!processed_candidate_) {
        if (!processCurrentToken()) {
            done_ = true;
            return false;
        }
    }

    // Preserve original text if configured or if no candidates were generated
    // This ensures Unicode symbols (emoji, etc.) are preserved even without keep_original setting
    // matching Elasticsearch behavior
    // NOTE: Must be AFTER processCurrentToken() but BEFORE first_letters to maintain correct order
    if (!processed_original_ && has_current_token_) {
        bool should_add_original = config_->keepOriginal;

        // For emoji/symbol fallback: check if ANY content WILL BE ACTUALLY OUTPUT
        // Not just whether buffers have content, but whether they will be processed
        // This handles cases like: keep_first_letter=false but first_letters_ has content
        bool will_output_first_letter = config_->keepFirstLetter && !first_letters_.empty();
        bool will_output_full_pinyin =
                config_->keepJoinedFullPinyin && !full_pinyin_letters_.empty();
        bool has_candidates = !candidate_.empty();

        if (!should_add_original && !has_candidates && !will_output_first_letter &&
            !will_output_full_pinyin) {
            // No content will be output, trigger fallback to preserve original token
            should_add_original = true;
        }

        processed_original_ = true;
        if (should_add_original) {
            addCandidate(
                    TermItem(current_source_, 0, static_cast<int>(current_source_.length()), 1));
        }
    }

    // Process joined full pinyin if needed
    if (config_->keepJoinedFullPinyin && !processed_full_pinyin_letter_ &&
        !full_pinyin_letters_.empty()) {
        processed_full_pinyin_letter_ = true;
        addCandidate(
                TermItem(full_pinyin_letters_, 0, static_cast<int>(current_source_.length()), 1));
        full_pinyin_letters_.clear();
    }

    // Process first letters if needed - should be processed AFTER all individual character processing
    if (config_->keepFirstLetter && !first_letters_.empty() && !processed_first_letter_) {
        processed_first_letter_ = true;
        std::string fl = first_letters_;

        // Apply length limit
        if (config_->limitFirstLetterLength > 0 &&
            fl.length() > static_cast<size_t>(config_->limitFirstLetterLength)) {
            fl = fl.substr(0, config_->limitFirstLetterLength);
        }

        // Apply lowercase
        if (config_->lowercase) {
            std::transform(fl.begin(), fl.end(), fl.begin(), ::tolower);
        }

        // Add candidate if not a single character when separate first letter is enabled
        if (!(config_->keepSeparateFirstLetter && fl.length() <= 1)) {
            addCandidate(TermItem(fl, 0, static_cast<int>(current_source_.length()), 1));
        }
    }

    // Sort candidates if not done yet
    if (!processed_sort_candidate_) {
        processed_sort_candidate_ = true;
        std::stable_sort(
                candidate_.begin(), candidate_.end(),
                [](const TermItem& a, const TermItem& b) { return a.position < b.position; });
    }

    // Return next candidate if available
    if (candidate_offset_ < candidate_.size()) {
        const TermItem& item = candidate_[candidate_offset_];
        candidate_offset_++;
        setTokenAttributes(token, item.term, item.start_offset, item.end_offset, item.position);
        return true;
    }

    done_ = true;
    return false;
}

bool PinyinFilter::prepareCurrentSource(std::vector<UChar32>& source_codepoints) {
    size_t source_start = 0;
    size_t source_end = current_token_text_.size();
    if (config_->trimWhitespace) {
        source_start = current_token_text_.find_first_not_of(" \t\n\r");
        if (source_start == std::string::npos) {
            return false;
        }
        source_end = current_token_text_.find_last_not_of(" \t\n\r") + 1;
    }
    current_source_ = current_token_text_.substr(source_start, source_end - source_start);

    if (current_source_.empty()) {
        return false;
    }

    if (config_->ignorePinyinOffset) {
        convertToCodepoints(current_source_, source_codepoints);
        return !source_codepoints.empty();
    }

    current_runes_ = convertToRunes(current_source_, source_codepoints);

    std::vector<RuneInfo> original_runes;
    if (!current_source_byte_offsets_.empty()) {
        std::vector<UChar32> original_codepoints;
        original_runes = convertToRunes(current_token_text_, original_codepoints);
        if (current_source_byte_offsets_.size() != original_runes.size() + 1) {
            current_source_byte_offsets_.clear();
            current_source_byte_end_offsets_.clear();
        }
    }
    if (!current_source_byte_offsets_.empty()) {
        DORIS_CHECK(current_source_byte_end_offsets_.empty() ||
                    current_source_byte_end_offsets_.size() == original_runes.size());
        const auto start_rune = std::ranges::lower_bound(
                original_runes, static_cast<int32_t>(source_start), {}, &RuneInfo::byte_start);
        const auto end_rune = std::ranges::lower_bound(
                original_runes, static_cast<int32_t>(source_end), {}, &RuneInfo::byte_start);
        const auto start_index = static_cast<size_t>(start_rune - original_runes.begin());
        const auto end_index = static_cast<size_t>(end_rune - original_runes.begin());
        DORIS_CHECK_EQ(end_index - start_index, current_runes_.size());
        const int32_t token_start_offset = current_start_offset_;
        current_start_offset_ += current_source_byte_offsets_[start_index];
        current_end_offset_ =
                token_start_offset + (current_source_byte_end_offsets_.empty()
                                              ? current_source_byte_offsets_[end_index]
                                              : current_source_byte_end_offsets_[end_index - 1]);
        for (size_t i = 0; i < current_runes_.size(); ++i) {
            current_runes_[i].byte_start = current_source_byte_offsets_[start_index + i] -
                                           current_source_byte_offsets_[start_index];
            current_runes_[i].byte_end =
                    (current_source_byte_end_offsets_.empty()
                             ? current_source_byte_offsets_[start_index + i + 1]
                             : current_source_byte_end_offsets_[start_index + i]) -
                    current_source_byte_offsets_[start_index];
        }
    } else if (has_current_conservative_source_span_) {
        DORIS_CHECK_GE(current_conservative_source_start_, 0);
        DORIS_CHECK_GE(current_conservative_source_end_, current_conservative_source_start_);
        const int32_t token_start_offset = current_start_offset_;
        current_start_offset_ = token_start_offset + current_conservative_source_start_;
        current_end_offset_ = token_start_offset + current_conservative_source_end_;
        const int32_t source_length =
                current_conservative_source_end_ - current_conservative_source_start_;
        for (auto& rune : current_runes_) {
            rune.byte_start = 0;
            rune.byte_end = source_length;
        }
    } else {
        current_start_offset_ += static_cast<int32_t>(source_start);
        current_end_offset_ =
                current_start_offset_ + static_cast<int32_t>(source_end - source_start);
    }

    return !source_codepoints.empty();
}

bool PinyinFilter::processCurrentToken() {
    processed_candidate_ = true;

    if (!has_current_token_) {
        return false;
    }

    // Convert to Unicode codepoints for processing.
    std::vector<UChar32> source_codepoints;
    if (!prepareCurrentSource(source_codepoints)) {
        return false;
    }

    // Use PinyinUtil to convert Chinese characters to pinyin
    auto pinyin_list =
            PinyinUtil::instance().convert(source_codepoints, PinyinFormat::TONELESS_PINYIN_FORMAT);
    auto chinese_list = ChineseUtil::segmentChinese(source_codepoints);

    // Process each character and generate candidates
    position_ = 0;
    std::string first_letters_buffer;
    std::string full_pinyin_buffer;

    // Buffer for accumulating ASCII characters
    std::string ascii_buffer;
    std::vector<int> ascii_source_rune_indices;

    for (size_t i = 0; i < source_codepoints.size(); ++i) {
        UChar32 codepoint = source_codepoints[i];
        std::string pinyin = (i < pinyin_list.size()) ? pinyin_list[i] : "";
        std::string chinese = (i < chinese_list.size()) ? chinese_list[i] : "";

        // Check if it's ASCII character
        bool is_ascii = codepoint >= 0 && codepoint < 128;
        bool is_alnum = (codepoint >= 'a' && codepoint <= 'z') ||
                        (codepoint >= 'A' && codepoint <= 'Z') ||
                        (codepoint >= '0' && codepoint <= '9');

        if (is_ascii && is_alnum) {
            // Check if we should process ASCII characters individually
            if (!config_->keepNoneChineseTogether && config_->keepNoneChinese) {
                // Process accumulated ASCII buffer before processing individual character
                if (!ascii_buffer.empty()) {
                    processAsciiBuffer(ascii_buffer, ascii_source_rune_indices);
                    ascii_buffer.clear();
                    ascii_source_rune_indices.clear();
                }
                // Process individual ASCII character immediately
                position_++;
                std::string single_char(1, static_cast<char>(codepoint));
                addCandidate(TermItem(single_char, static_cast<int>(i), static_cast<int>(i + 1),
                                      position_));
            } else {
                // Accumulate ASCII characters for later processing
                ascii_buffer += static_cast<char>(codepoint);
                ascii_source_rune_indices.push_back(static_cast<int>(i));
            }

            // Handle ASCII alphanumeric characters for first letters
            if (config_->keepNoneChineseInFirstLetter) {
                first_letters_buffer += static_cast<char>(codepoint);
            }
            if (config_->keepNoneChineseInJoinedFullPinyin) {
                full_pinyin_buffer += static_cast<char>(codepoint);
            }
        } else if (is_ascii) {
            // For non-alphanumeric ASCII characters (like spaces, punctuation),
            // do nothing and continue to keep the buffer intact.
            continue;
        } else {
            // Process accumulated ASCII buffer when we hit non-ASCII (Chinese) characters
            if (!ascii_buffer.empty()) {
                processAsciiBuffer(ascii_buffer, ascii_source_rune_indices);
                ascii_buffer.clear();
                ascii_source_rune_indices.clear();
            }

            if (!pinyin.empty() && !chinese.empty()) {
                // Handle Chinese characters
                position_++;

                // Add separate first letter if configured
                if (config_->keepSeparateFirstLetter && pinyin.length() > 1) {
                    addCandidate(TermItem(std::string(1, pinyin[0]), static_cast<int>(i),
                                          static_cast<int>(i + 1), position_));
                }

                // Add full pinyin if configured
                if (config_->keepFullPinyin) {
                    addCandidate(TermItem(pinyin, static_cast<int>(i), static_cast<int>(i + 1),
                                          position_));
                }

                // Add separate Chinese character if configured
                if (config_->keepSeparateChinese) {
                    addCandidate(TermItem(chinese, static_cast<int>(i), static_cast<int>(i + 1),
                                          position_));
                }

                // Collect letters for combined processing
                if (config_->keepFirstLetter || config_->keepJoinedFullPinyin) {
                    if (!pinyin.empty()) {
                        first_letters_buffer += pinyin[0];
                        if (config_->keepJoinedFullPinyin) {
                            full_pinyin_buffer += pinyin;
                        }
                    }
                }
            }
            // For non-ASCII, non-Chinese characters (e.g., emoji, symbols),
            // we don't add them to candidate. They will only be kept if the fallback
            // mechanism is triggered (when candidate_ is empty).
        }
    }

    // Process any remaining ASCII buffer at the end
    if (!ascii_buffer.empty()) {
        processAsciiBuffer(ascii_buffer, ascii_source_rune_indices);
    }

    // Store the collected letters for later processing
    first_letters_ = first_letters_buffer;
    full_pinyin_letters_ = full_pinyin_buffer;

    return true;
}

void PinyinFilter::addCandidate(const TermItem& item) {
    std::string term = item.term;

    // Apply transformations
    if (config_->lowercase) {
        std::transform(term.begin(), term.end(), term.begin(), ::tolower);
    }

    if (config_->trimWhitespace) {
        term = trim(term);
    }

    if (term.empty()) {
        return;
    }

    // Filter duplicates
    std::string filter_key = term + std::to_string(item.position);

    if (config_->removeDuplicateTerm) {
        filter_key = term;
    }

    if (terms_filter_.find(filter_key) != terms_filter_.end()) {
        return;
    }

    terms_filter_.insert(filter_key);

    // Create new TermItem with modified term
    TermItem new_item = item;
    new_item.term = term;
    candidate_.push_back(new_item);
}

void PinyinFilter::processAsciiBuffer(const std::string& ascii_buffer,
                                      const std::vector<int>& source_rune_indices) {
    if (ascii_buffer.empty() || !config_->keepNoneChinese) {
        return;
    }
    DORIS_CHECK_EQ(ascii_buffer.size(), source_rune_indices.size());

    if (config_->noneChinesePinyinTokenize) {
        // Use PinyinAlphabetTokenizer to split ASCII buffer into meaningful tokens
        std::vector<std::string> tokens = PinyinAlphabetTokenizer::walk(ascii_buffer);

        size_t compact_offset = 0;
        int fixed_offset = source_rune_indices.front();
        for (const auto& token : tokens) {
            const size_t compact_end = compact_offset + token.size();
            DORIS_CHECK_LE(compact_end, source_rune_indices.size());
            position_++;
            if (config_->fixedPinyinOffset) {
                addCandidate(TermItem(token, fixed_offset, fixed_offset + 1, position_));
                ++fixed_offset;
            } else {
                const int source_start = source_rune_indices[compact_offset];
                const int source_end = source_rune_indices[compact_end - 1] + 1;
                addCandidate(TermItem(token, source_start, source_end, position_));
            }
            compact_offset = compact_end;
        }
        DORIS_CHECK_EQ(compact_offset, source_rune_indices.size());
    } else {
        // Treat the entire ASCII buffer as a single token
        position_++;
        addCandidate(TermItem(ascii_buffer, source_rune_indices.front(),
                              source_rune_indices.back() + 1, position_));
    }
}

void PinyinFilter::setTokenAttributes(Token* token, const std::string& term, int start_offset,
                                      int end_offset, int position) {
    set_text(token, term);

    int absolute_start = current_start_offset_;
    int absolute_end = current_end_offset_;
    const bool is_whole_token =
            start_offset == 0 && std::cmp_equal(end_offset, current_source_.length());
    if (!config_->ignorePinyinOffset && !is_whole_token && start_offset >= 0 && end_offset > 0 &&
        std::cmp_less(start_offset, current_runes_.size()) &&
        std::cmp_less_equal(end_offset, current_runes_.size())) {
        absolute_start += current_runes_[start_offset].byte_start;
        absolute_end = current_start_offset_ + current_runes_[end_offset - 1].byte_end;
    }
    token->setStartOffset(absolute_start);
    token->setEndOffset(absolute_end);
    publishCandidateProvenance(term, is_whole_token, absolute_end - absolute_start);

    int offset = position - last_increment_position_;
    if (offset < 0) {
        offset = 0;
    }
    token->setPositionIncrement(offset);
    last_increment_position_ = position;
}

bool PinyinFilter::get_conservative_source_byte_span(int32_t& start, int32_t& end) const {
    if (!has_published_token_ || !published_source_byte_offsets_.empty()) {
        return false;
    }
    start = 0;
    end = published_source_length_;
    return true;
}

void PinyinFilter::publishCandidateProvenance(const std::string& term, bool is_whole_token,
                                              int32_t source_length) {
    has_published_token_ = true;
    published_source_length_ = source_length;
    published_source_byte_offsets_.clear();
    published_source_byte_end_offsets_.clear();
    // Only an unchanged original token keeps exact rune boundaries; transformed candidates
    // publish their whole source span instead of the input token's map.
    if (config_->ignorePinyinOffset || !is_whole_token || current_runes_.empty() ||
        term != current_source_) {
        return;
    }
    published_source_byte_offsets_.reserve(current_runes_.size() + 1);
    published_source_byte_end_offsets_.reserve(current_runes_.size());
    for (const auto& rune : current_runes_) {
        published_source_byte_offsets_.push_back(rune.byte_start);
        published_source_byte_end_offsets_.push_back(rune.byte_end);
    }
    published_source_byte_offsets_.push_back(current_runes_.back().byte_end);
}

std::string PinyinFilter::trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) {
        return "";
    }
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, (last - first + 1));
}

std::vector<PinyinFilter::RuneInfo> PinyinFilter::convertToRunes(const std::string& text,
                                                                 std::vector<UChar32>& codepoints) {
    std::vector<RuneInfo> runes;
    codepoints.clear();

    const char* data = text.data();
    int32_t length = static_cast<int32_t>(text.length());
    int32_t offset = 0;

    while (offset < length) {
        int32_t byte_start = offset;
        UChar32 cp;
        U8_NEXT(data, offset, length, cp);
        int32_t byte_end = offset;

        RuneInfo rune;
        rune.cp = cp;
        rune.byte_start = byte_start;
        rune.byte_end = byte_end;

        runes.push_back(rune);
        codepoints.push_back(cp);
    }

    return runes;
}

void PinyinFilter::convertToCodepoints(const std::string& text, std::vector<UChar32>& codepoints) {
    codepoints.clear();
    const char* data = text.data();
    const auto length = static_cast<int32_t>(text.length());
    int32_t offset = 0;
    while (offset < length) {
        UChar32 codepoint = U_UNASSIGNED;
        U8_NEXT(data, offset, length, codepoint);
        codepoints.push_back(codepoint);
    }
}

} // namespace doris::segment_v2::inverted_index
