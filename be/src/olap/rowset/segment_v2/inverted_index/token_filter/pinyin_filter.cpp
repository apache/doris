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

#include "pinyin_filter.h"

#include <algorithm>
#include <iostream>

#include "common/exception.h"
#include "common/logging.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/chinese_util.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_format.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_util.h"

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
            addCandidate(TermItem(fl, 0, static_cast<int>(fl.length()), 1));
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

bool PinyinFilter::processCurrentToken() {
    processed_candidate_ = true;

    if (!has_current_token_) {
        return false;
    }

    current_source_ = current_token_text_;

    // Apply trimming if configured
    if (config_->trimWhitespace) {
        current_source_ = trim(current_source_);
    }

    if (current_source_.empty()) {
        return false;
    }

    // Convert to Unicode codepoints for processing
    std::vector<UChar32> source_codepoints;
    convertToRunes(current_source_, source_codepoints);

    if (source_codepoints.empty()) {
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
    int ascii_buffer_start_pos = -1;

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
                    processAsciiBuffer(ascii_buffer, ascii_buffer_start_pos, static_cast<int>(i));
                    ascii_buffer.clear();
                    ascii_buffer_start_pos = -1;
                }
                // Process individual ASCII character immediately
                position_++;
                std::string single_char(1, static_cast<char>(codepoint));
                addCandidate(TermItem(single_char, static_cast<int>(i), static_cast<int>(i + 1),
                                      position_));
            } else {
                // Accumulate ASCII characters for later processing
                if (ascii_buffer.empty()) {
                    ascii_buffer_start_pos = static_cast<int>(i);
                }
                ascii_buffer += static_cast<char>(codepoint);
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
                processAsciiBuffer(ascii_buffer, ascii_buffer_start_pos, static_cast<int>(i));
                ascii_buffer.clear();
                ascii_buffer_start_pos = -1;
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
        processAsciiBuffer(ascii_buffer, ascii_buffer_start_pos,
                           static_cast<int>(source_codepoints.size()));
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

void PinyinFilter::processAsciiBuffer(const std::string& ascii_buffer, int start_pos, int end_pos) {
    if (ascii_buffer.empty() || !config_->keepNoneChinese) {
        return;
    }

    if (config_->noneChinesePinyinTokenize) {
        // Use PinyinAlphabetTokenizer to split ASCII buffer into meaningful tokens
        std::vector<std::string> tokens = PinyinAlphabetTokenizer::walk(ascii_buffer);

        int current_offset = start_pos;
        for (const auto& token : tokens) {
            position_++;
            int token_end = (config_->fixedPinyinOffset)
                                    ? (current_offset + 1)
                                    : (current_offset + static_cast<int>(token.length()));
            addCandidate(TermItem(token, current_offset, token_end, position_));
            current_offset = token_end;
        }
    } else {
        // Treat the entire ASCII buffer as a single token
        position_++;
        addCandidate(TermItem(ascii_buffer, start_pos, end_pos, position_));
    }
}

void PinyinFilter::setTokenAttributes(Token* token, const std::string& term, int start_offset,
                                      int end_offset, int position) {
    set_text(token, term);

    token->setStartOffset(start_offset);
    token->setEndOffset(end_offset);

    int offset = position - last_increment_position_;
    if (offset < 0) {
        offset = 0;
    }
    token->setPositionIncrement(offset);
    last_increment_position_ = position;
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

} // namespace doris::segment_v2::inverted_index