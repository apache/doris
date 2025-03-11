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

#include "default_icu_tokenizer_config.h"

#include <atomic>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

namespace doris::segment_v2 {

BreakIteratorPtr DefaultICUTokenizerConfig::cjk_break_iterator_;
BreakIteratorPtr DefaultICUTokenizerConfig::default_break_iterator_;
BreakIteratorPtr DefaultICUTokenizerConfig::myanmar_syllable_iterator_;

DefaultICUTokenizerConfig::DefaultICUTokenizerConfig(bool cjk_as_words, bool myanmar_as_words) {
    cjk_as_words_ = cjk_as_words;
    myanmar_as_words_ = myanmar_as_words;
}

void DefaultICUTokenizerConfig::initialize(const std::string& dictPath) {
    static std::atomic<bool> initialized_(false);
    if (!initialized_) {
        static std::mutex mutex;
        std::lock_guard<std::mutex> lock(mutex);

        if (!initialized_) {
            try {
                UErrorCode status = U_ZERO_ERROR;
                cjk_break_iterator_.reset(
                        icu::BreakIterator::createWordInstance(icu::Locale::getRoot(), status));
                if (U_FAILURE(status)) {
                    std::string error_msg = "Failed to create CJK BreakIterator: ";
                    error_msg += u_errorName(status);
                    _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
                }

                read_break_iterator(default_break_iterator_, dictPath + "/uax29/Default.txt");
                read_break_iterator(myanmar_syllable_iterator_,
                                    dictPath + "/uax29/MyanmarSyllable.txt");

                initialized_ = true;
            } catch (...) {
                cjk_break_iterator_.reset();
                default_break_iterator_.reset();
                myanmar_syllable_iterator_.reset();
                throw; // Clean up resources and rethrow the original exception to the caller
            }
        }
    }
}

icu::BreakIterator* DefaultICUTokenizerConfig::get_break_iterator(int32_t script) {
    UErrorCode status = U_ZERO_ERROR;
    icu::BreakIterator* clone = nullptr;
    switch (script) {
    case USCRIPT_JAPANESE:
        clone = cjk_break_iterator_->clone();
        break;
    case USCRIPT_MYANMAR:
        if (myanmar_as_words_) {
            clone = default_break_iterator_->clone();
        } else {
            clone = myanmar_syllable_iterator_->clone();
        }
        break;
    default:
        clone = default_break_iterator_->clone();
        break;
    }
    if (clone == nullptr) {
        std::string error_msg = "UBreakIterator clone failed: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_Runtime, error_msg.c_str());
    }
    return clone;
}

void DefaultICUTokenizerConfig::read_break_iterator(BreakIteratorPtr& rbbi,
                                                    const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    if (!in) {
        std::string error_msg = "Unable to open the file: " + filename;
        _CLTHROWT(CL_ERR_IO, error_msg.c_str());
    }

    std::ostringstream ss;
    ss << in.rdbuf();
    in.close();

    std::string utf8Content = ss.str();
    icu::UnicodeString rulesData(utf8Content.data());

    UParseError parseErr;
    UErrorCode status = U_ZERO_ERROR;
    rbbi = std::make_unique<icu::RuleBasedBreakIterator>(rulesData, parseErr, status);
    if (U_FAILURE(status)) {
        std::string error_msg = "ubrk_openRules failed: ";
        error_msg += u_errorName(status);
        _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
    }
    if (parseErr.line != 0 || parseErr.offset != 0) {
        std::string error_msg = "Syntax error in break rules at line ";
        error_msg += std::to_string(parseErr.line);
        error_msg += ", offset ";
        error_msg += std::to_string(parseErr.offset);
        _CLTHROWT(CL_ERR_IllegalArgument, error_msg.c_str());
    }
}

} // namespace doris::segment_v2