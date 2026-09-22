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

#include "storage/index/inverted/tokenizer/pinyin/pinyin_tokenizer.h"

#include <unicode/unistr.h>
#include <unicode/utf8.h>

#include <algorithm>
#include <cctype>

#include "CLucene/analysis/AnalysisHeader.h"
#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/tokenizer/pinyin/chinese_util.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_alphabet_tokenizer.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_format.h"
#include "storage/index/inverted/tokenizer/pinyin/pinyin_util.h"

namespace doris::segment_v2::inverted_index {

PinyinTokenizer::PinyinTokenizer() = default;

PinyinTokenizer::PinyinTokenizer(std::shared_ptr<doris::segment_v2::PinyinConfig> config) {
    config_ = std::move(config);
    if (!config_) {
        config_ = std::make_shared<doris::segment_v2::PinyinConfig>();
    }
    candidate_.clear();
    terms_filter_.clear();
    first_letters_.clear();
    full_pinyin_letters_.clear();
}

void PinyinTokenizer::reset() {
    DorisTokenizer::reset();
    has_current_span_ = false;
    ascii_buff_rune_starts_.clear();
    ascii_buff_rune_ends_.clear();
    position_ = 0;
    candidate_offset_ = 0;
    done_ = false;
    processed_candidate_ = false;
    processed_first_letter_ = false;
    processed_full_pinyin_letter_ = false;
    processed_original_ = false;
    processed_sort_candidate_ = false;
    first_letters_.clear();
    full_pinyin_letters_.clear();
    terms_filter_.clear();
    candidate_.clear();
    source_codepoints_.clear();
    last_increment_position_ = 0;
    last_offset_ = 0;

    _char_buffer = nullptr;
    _char_length = _in->read((const void**)&_char_buffer, 0, static_cast<int32_t>(_in->size()));

    decode_to_runes();
}

void PinyinTokenizer::processInput() {
    if (!processed_candidate_) {
        processed_candidate_ = true;

        auto pinyin_list = PinyinUtil::instance().convert(source_codepoints_,
                                                          PinyinFormat::TONELESS_PINYIN_FORMAT);
        auto chinese_list = ChineseUtil::segmentChinese(source_codepoints_);

        if (pinyin_list.empty() || chinese_list.empty()) {
            return;
        }

        if (runes_.empty()) {
            return;
        }

        position_ = 0;
        int ascii_buff_start_byte = -1;
        std::string ascii_buff;
        int char_index = 0;

        for (const auto& r : runes_) {
            std::string pinyin = (char_index < static_cast<int>(pinyin_list.size()))
                                         ? pinyin_list[char_index]
                                         : "";
            std::string chinese = (char_index < static_cast<int>(chinese_list.size()))
                                          ? chinese_list[char_index]
                                          : "";
            bool is_ascii_context = r.cp >= 0 && r.cp < 128;
            bool is_alnum = (r.cp >= 'a' && r.cp <= 'z') || (r.cp >= 'A' && r.cp <= 'Z') ||
                            (r.cp >= '0' && r.cp <= '9');

            if (is_ascii_context) {
                if (is_alnum && config_->keepNoneChinese) {
                    if (config_->keepNoneChineseTogether) {
                        // The buffer only holds letters and digits, so remember where each one
                        // came from instead of assuming they are contiguous in the source.
                        if (ascii_buff.empty()) {
                            ascii_buff_start_byte = r.byte_start;
                        }
                        ascii_buff.push_back(static_cast<char>(r.cp));
                        ascii_buff_rune_starts_.push_back(r.byte_start);
                        ascii_buff_rune_ends_.push_back(r.byte_end);
                    } else {
                        position_++;
                        std::string single_char(1, static_cast<char>(r.cp));
                        // Use byte offset for single ASCII character
                        addCandidate(single_char, r.byte_start, r.byte_end, char_index + 1);
                    }
                }
                if (is_alnum && config_->keepNoneChineseInFirstLetter) {
                    first_letters_.push_back(static_cast<char>(r.cp));
                }
                if (is_alnum && config_->keepNoneChineseInJoinedFullPinyin) {
                    full_pinyin_letters_.push_back(static_cast<char>(r.cp));
                }
            } else {
                if (!ascii_buff.empty()) {
                    parseBuff(ascii_buff, ascii_buff_start_byte);
                }
                bool incr_position = false;
                if (!pinyin.empty() && !chinese.empty()) {
                    first_letters_.push_back(pinyin[0]);
                    if (config_->keepSeparateFirstLetter && pinyin.length() > 1) {
                        position_++;
                        incr_position = true;
                        std::string first_letter(1, pinyin[0]);
                        addCandidate(first_letter, r.byte_start, r.byte_end, position_);
                    }
                    if (config_->keepFullPinyin) {
                        if (!incr_position) position_++;
                        addCandidate(pinyin, r.byte_start, r.byte_end, position_);
                    }
                    if (config_->keepSeparateChinese) {
                        addCandidate(chinese, r.byte_start, r.byte_end, position_);
                    }
                    if (config_->keepJoinedFullPinyin) {
                        full_pinyin_letters_ += pinyin;
                    }
                }
            }
            last_offset_ = r.byte_end - 1;
            char_index++;
        }

        if (!ascii_buff.empty()) {
            parseBuff(ascii_buff, ascii_buff_start_byte);
        }
    }

    int total_byte_length = runes_.empty() ? 0 : runes_.back().byte_end;

    if (config_->keepOriginal && !processed_original_) {
        processed_original_ = true;
        std::string source_utf8 = codepointsToUtf8(source_codepoints_);
        addCandidate(source_utf8, 0, total_byte_length, 1);
    }
    if (config_->keepJoinedFullPinyin && !processed_full_pinyin_letter_ &&
        !full_pinyin_letters_.empty()) {
        processed_full_pinyin_letter_ = true;
        addCandidate(full_pinyin_letters_, 0, total_byte_length, 1);
        full_pinyin_letters_.clear();
    }
    if (config_->keepFirstLetter && !first_letters_.empty() && !processed_first_letter_) {
        processed_first_letter_ = true;
        std::string fl = first_letters_;

        if (config_->limitFirstLetterLength > 0 &&
            static_cast<int>(fl.length()) > config_->limitFirstLetterLength) {
            fl = fl.substr(0, config_->limitFirstLetterLength);
        }
        if (config_->lowercase) {
            std::transform(fl.begin(), fl.end(), fl.begin(),
                           [](unsigned char x) { return static_cast<char>(std::tolower(x)); });
        }
        if (!(config_->keepSeparateFirstLetter && fl.length() <= 1)) {
            addCandidate(fl, 0, total_byte_length, 1);
        }
    }

    if (!processed_sort_candidate_) {
        processed_sort_candidate_ = true;
        std::stable_sort(
                candidate_.begin(), candidate_.end(),
                [](const TermItem& a, const TermItem& b) { return a.position < b.position; });
    }
}

Token* PinyinTokenizer::next(Token* token) {
    if (!done_) {
        processInput();
        done_ = true;
    }

    if (candidate_offset_ < static_cast<int>(candidate_.size())) {
        const TermItem& item = candidate_[candidate_offset_];
        candidate_offset_++;

        const std::string& text = item.term;
        size_t size = std::min(text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
        token->setNoCopy(text.data(), 0, static_cast<int32_t>(size));

        int32_t start = item.start_offset;
        int32_t end = item.end_offset;
        if (config_->ignorePinyinOffset) {
            start = 0;
            end = runes_.empty() ? 0 : runes_.back().byte_end;
        }
        token->setStartOffset(correct_source_offset(start));
        token->setEndOffset(correct_source_offset(end));
        publishCandidateProvenance(std::string_view(text.data(), size), start, end);

        int offset = item.position - last_increment_position_;
        if (offset < 0) offset = 0;
        token->setPositionIncrement(offset);
        last_increment_position_ = item.position;
        return token;
    }

    done_ = true;
    return nullptr;
}

// Publish exact rune boundaries when the candidate is the untouched source slice, otherwise a
// conservative span over the whole candidate range; both are projected through the char filter.
void PinyinTokenizer::publishCandidateProvenance(std::string_view term, int32_t start,
                                                 int32_t end) {
    has_current_span_ = false;
    _source_byte_offsets.clear();
    _source_byte_end_offsets.clear();
    if (!_source_byte_offsets_enabled) {
        return;
    }
    start = std::clamp(start, 0, _char_length);
    end = std::clamp(end, start, _char_length);
    const std::string_view source(_char_buffer + start, end - start);
    if (source == term) {
        set_source_byte_offsets(term, source, start);
        if (!_source_byte_offsets.empty()) {
            return;
        }
    }
    current_span_end_ = correct_source_offset(end) - correct_source_offset(start);
    has_current_span_ = true;
}

bool PinyinTokenizer::get_conservative_source_byte_span(int32_t& start, int32_t& end) const {
    if (!has_current_span_) {
        return false;
    }
    start = 0;
    end = current_span_end_;
    return true;
}

void PinyinTokenizer::addCandidate(const TermItem& item_in) {
    std::string term = item_in.term;

    if (config_->lowercase) {
        std::transform(term.begin(), term.end(), term.begin(),
                       [](unsigned char x) { return static_cast<char>(std::tolower(x)); });
    }
    if (config_->trimWhitespace) {
        auto not_space = [](int ch) { return !std::isspace(ch); };
        term.erase(term.begin(), std::find_if(term.begin(), term.end(), not_space));
        term.erase(std::find_if(term.rbegin(), term.rend(), not_space).base(), term.end());
    }
    if (term.empty()) {
        return;
    }

    std::string key = config_->removeDuplicateTerm ? term : term + std::to_string(item_in.position);
    if (terms_filter_.find(key) != terms_filter_.end()) {
        return;
    }
    terms_filter_.insert(std::move(key));

    candidate_.emplace_back(term, item_in.start_offset, item_in.end_offset, item_in.position);
}

void PinyinTokenizer::addCandidate(const std::string& term, int start_offset, int end_offset,
                                   int position) {
    TermItem item(term, start_offset, end_offset, position);
    addCandidate(item);
}

void PinyinTokenizer::setTerm(std::string term, int start_offset, int end_offset, int position) {
    addCandidate(term, start_offset, end_offset, position);
}

bool PinyinTokenizer::hasMoreTokens() const {
    return candidate_offset_ < static_cast<int>(candidate_.size());
}

void PinyinTokenizer::decode_to_runes() {
    runes_.clear();
    source_codepoints_.clear();

    if (!_char_buffer || _char_length <= 0) {
        return;
    }

    runes_.reserve(static_cast<size_t>(_char_length));
    source_codepoints_.reserve(static_cast<size_t>(_char_length));

    int32_t i = 0;

    while (i < _char_length) {
        UChar32 c = U_UNASSIGNED;
        int32_t prev = i;
        U8_NEXT(_char_buffer, i, _char_length, c);
        if (c < 0) {
            c = 0xFFFD;
        }

        Rune r;
        r.cp = c;
        r.byte_start = prev;
        r.byte_end = i;
        runes_.push_back(r);
        source_codepoints_.push_back(c);
    }
}

void PinyinTokenizer::parseBuff(std::string& ascii_buff, int& ascii_buff_start_byte) {
    if (ascii_buff.empty()) return;
    if (!config_->keepNoneChinese) {
        ascii_buff.clear();
        ascii_buff_rune_starts_.clear();
        ascii_buff_rune_ends_.clear();
        ascii_buff_start_byte = -1;
        return;
    }

    // Each buffered letter keeps its own source range, so a candidate spans from the first letter
    // it covers to the end of the last one even when punctuation was skipped in between.
    DCHECK_EQ(ascii_buff.size(), ascii_buff_rune_starts_.size());
    const int32_t buff_end_byte = ascii_buff_rune_ends_.back();

    if (config_->noneChinesePinyinTokenize) {
        std::vector<std::string> result = PinyinAlphabetTokenizer::walk(ascii_buff);
        size_t covered = 0;
        int32_t fixed_start = ascii_buff_start_byte;
        for (const std::string& t : result) {
            if (covered >= ascii_buff.size()) {
                break;
            }
            const size_t last = std::min(covered + t.length(), ascii_buff.size()) - 1;
            int32_t start = ascii_buff_rune_starts_[covered];
            int32_t end = ascii_buff_rune_ends_[last];
            if (config_->fixedPinyinOffset) {
                start = fixed_start;
                end = fixed_start + 1;
                fixed_start = end;
            }
            position_++;
            addCandidate(t, start, end, position_);
            covered = last + 1;
        }
    } else if (config_->keepFirstLetter || config_->keepSeparateFirstLetter ||
               config_->keepFullPinyin || !config_->keepNoneChineseInJoinedFullPinyin) {
        position_++;
        addCandidate(ascii_buff, ascii_buff_start_byte, buff_end_byte, position_);
    }
    ascii_buff.clear();
    ascii_buff_rune_starts_.clear();
    ascii_buff_rune_ends_.clear();
    ascii_buff_start_byte = -1;
}

std::string PinyinTokenizer::codepointsToUtf8(const std::vector<UChar32>& codepoints) const {
    std::string result;
    for (UChar32 cp : codepoints) {
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
        result.append(utf8_buffer, utf8_len);
    }
    return result;
}

} // namespace doris::segment_v2::inverted_index
