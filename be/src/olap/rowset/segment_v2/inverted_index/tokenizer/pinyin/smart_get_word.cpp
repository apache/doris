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

#include "smart_get_word.h"

#include "common/logging.h"
#include "smart_forest.h"
#include "unicode/utf8.h"

namespace doris::segment_v2::inverted_index {

const std::string SmartGetWord::EMPTYSTRING = "";
const std::string SmartGetWord::NULL_RESULT = "\x01NULL\x01";

SmartGetWord::SmartGetWord(SmartForest* forest, const std::string& content)
        : forest_(forest), branch_(forest) {
    runes_ = utf8_to_runes(content);
    i_ = root_;
}

SmartGetWord::SmartGetWord(SmartForest* forest, const std::vector<Rune>& runes)
        : forest_(forest), runes_(runes), branch_(forest) {
    i_ = root_;
}

std::string SmartGetWord::getFrontWords() {
    std::string temp;
    do {
        temp = frontWords();
        if (temp == NULL_RESULT) {
            return NULL_RESULT;
        }
        temp = checkNumberOrEnglish(temp);
    } while (temp == EMPTYSTRING);

    return temp;
}

const SmartGetWord::ParamType& SmartGetWord::getParam() const {
    return param_;
}

void SmartGetWord::reset(const std::string& content) {
    runes_ = utf8_to_runes(content);
    reset(runes_);
}

void SmartGetWord::reset(const std::vector<Rune>& runes) {
    runes_ = runes;
    branch_ = forest_;
    status_ = 0;
    root_ = 0;
    i_ = root_;
    is_back_ = false;
    temp_offe_ = 0;
    offe = 0;
    param_.clear();
}

std::string SmartGetWord::frontWords() {
    if (i_ >= runes_.size()) {
        return NULL_RESULT;
    }

    size_t tempEndIndex = 0; // Track the end position (exclusive) for matched word
    branch_ = forest_;

    for (size_t j = i_; j < runes_.size(); j++) {
        UChar32 cp = runes_[j].cp;

        // Move to next branch first
        branch_ = branch_->getBranch(cp);
        if (!branch_) {
            break;
        }

        // Then check if current branch represents a word end
        if (branch_->getStatus() == SmartForest::WORD_END ||
            branch_->getStatus() == SmartForest::WORD_CONTINUE) {
            tempEndIndex = j + 1; // End position is exclusive, so j+1
            temp_offe_ = i_;
            param_ = branch_->getParam();
        }
    }

    if (tempEndIndex > i_) {
        offe = runes_[i_].byte_start;
        std::string result = runes_to_utf8(runes_, i_, tempEndIndex);
        i_ = tempEndIndex;
        return result;
    } else {
        if (i_ < runes_.size()) {
            i_++;
            return frontWords();
        }
    }

    return NULL_RESULT;
}

std::string SmartGetWord::allWords() {
    return frontWords();
}

std::string SmartGetWord::checkNumberOrEnglish(const std::string& temp) {
    if (temp.empty() || temp == NULL_RESULT) {
        return temp;
    }

    if (temp.length() == 1) {
        UChar32 cp;
        int32_t index = 0;
        U8_NEXT(temp.c_str(), index, static_cast<int32_t>(temp.length()), cp);

        if (isNum(cp) || isE(cp)) {
            size_t start_pos = i_ - 1;
            size_t end_pos = start_pos + 1;

            while (end_pos < runes_.size()) {
                UChar32 next_cp = runes_[end_pos].cp;
                if ((isNum(cp) && isNum(next_cp)) || (isE(cp) && checkSame(cp, next_cp))) {
                    end_pos++;
                } else {
                    break;
                }
            }

            if (end_pos > start_pos + 1) {
                i_ = end_pos;
                return runes_to_utf8(runes_, start_pos, end_pos);
            }
        }
    }

    return temp;
}

bool SmartGetWord::checkSame(UChar32 l, UChar32 c) {
    return u_tolower(l) == u_tolower(c);
}

bool SmartGetWord::isE(UChar32 c) const {
    return u_isalpha(c);
}

bool SmartGetWord::isNum(UChar32 c) const {
    return u_isdigit(c);
}

int SmartGetWord::getByteOffset() const {
    if (i_ < runes_.size()) {
        return runes_[i_].byte_start;
    }
    return runes_.empty() ? 0 : runes_.back().byte_end;
}

int SmartGetWord::getMatchStartByte() const {
    if (offe < static_cast<int>(runes_.size())) {
        return runes_[offe].byte_start;
    }
    return 0;
}

int SmartGetWord::getMatchEndByte() const {
    if (i_ > 0 && i_ <= runes_.size()) {
        return runes_[i_ - 1].byte_end;
    }
    return 0;
}

std::vector<Rune> SmartGetWord::utf8_to_runes(const std::string& utf8_str) {
    std::vector<Rune> runes;
    runes.reserve(utf8_str.length());

    int32_t byte_pos = 0;
    const char* str_ptr = utf8_str.c_str();
    int32_t str_length = static_cast<int32_t>(utf8_str.length());

    while (byte_pos < str_length) {
        UChar32 cp;
        int32_t byte_start = byte_pos;

        U8_NEXT(str_ptr, byte_pos, str_length, cp);

        if (cp >= 0) {
            runes.emplace_back(byte_start, byte_pos, cp);
        }
    }

    return runes;
}

std::string SmartGetWord::runes_to_utf8(const std::vector<Rune>& runes, size_t start, size_t end) {
    std::string result;
    for (size_t i = start; i < end && i < runes.size(); ++i) {
        UChar32 cp = runes[i].cp;
        char utf8_buffer[4];
        int32_t utf8_len = 0;
        U8_APPEND_UNSAFE(utf8_buffer, utf8_len, cp);
        result.append(utf8_buffer, utf8_len);
    }
    return result;
}

} // namespace doris::segment_v2::inverted_index
