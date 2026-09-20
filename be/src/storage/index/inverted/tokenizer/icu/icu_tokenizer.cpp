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

#include "storage/index/inverted/tokenizer/icu/icu_tokenizer.h"

#include <unicode/unistr.h>

#include <memory>
#include <string>

namespace doris::segment_v2::inverted_index {

ICUTokenizer::ICUTokenizer() {
    config_ = std::make_shared<DefaultICUTokenizerConfig>(true, true);
    breaker_ = std::make_unique<CompositeBreakIterator>(config_);
}

ICUTokenizer::ICUTokenizer(bool own_reader) : ICUTokenizer() {
    this->ownReader = own_reader;
}

void ICUTokenizer::initialize(const std::string& dictPath) {
    config_->initialize(dictPath);
    breaker_->initialize();
}

Token* ICUTokenizer::next(Token* token) {
    int32_t start = breaker_->current();
    assert(start != UBRK_DONE);

    int32_t end = breaker_->next();
    while (end != UBRK_DONE && breaker_->get_rule_status() == 0) {
        start = end;
        end = breaker_->next();
    }

    if (end == UBRK_DONE) {
        return nullptr;
    }

    utf8Str_.clear();
    int32_t length = std::min(end - start, LUCENE_MAX_WORD_LEN);
    auto subString = buffer_.tempSubString(start, length);
    sourceUtf8Str_.clear();
    subString.toUTF8String(sourceUtf8Str_);
    if (this->lowercase) {
        subString.toLower().toUTF8String(utf8Str_);
    } else {
        subString.toUTF8String(utf8Str_);
    }

    token->setNoCopy(utf8Str_.data(), 0, static_cast<int32_t>(utf8Str_.size()));
    if (start >= 0 && length >= 0 &&
        static_cast<size_t>(start + length) < utf16ToUtf8Offset_.size()) {
        const int32_t source_start = utf16ToUtf8Offset_[start];
        const int32_t source_end = utf16ToUtf8Offset_[start + length];
        set_source_byte_offsets(utf8Str_, sourceUtf8Str_, source_start);
        token->setStartOffset(correct_source_offset(source_start));
        token->setEndOffset(correct_source_offset(source_end));
    }
    return token;
}

void ICUTokenizer::reset() {
    DorisTokenizer::reset();
    const char* buf = nullptr;
    int32_t len = _in->read((const void**)&buf, 0, static_cast<int32_t>(_in->size()));
    buffer_ = icu::UnicodeString::fromUTF8(icu::StringPiece(buf, len));
    if (!buffer_.isEmpty() && buffer_.isBogus()) {
        _CLTHROWT(CL_ERR_Runtime, "Failed to convert UTF-8 string to UnicodeString.");
    }
    utf16ToUtf8Offset_.assign(buffer_.length() + 1, 0);
    int32_t utf8Offset = 0;
    int32_t utf16Offset = 0;
    while (utf8Offset < len) {
        const int32_t sourceStart = utf8Offset;
        UChar32 codePoint;
        U8_NEXT(buf, utf8Offset, len, codePoint);
        if (codePoint < 0) {
            utf16ToUtf8Offset_.clear();
            break;
        }
        const int32_t utf16Length = U16_LENGTH(codePoint);
        for (int32_t index = 0; index < utf16Length; ++index) {
            utf16ToUtf8Offset_[utf16Offset + index] = sourceStart;
        }
        utf16Offset += utf16Length;
        utf16ToUtf8Offset_[utf16Offset] = utf8Offset;
    }
    breaker_->set_text(buffer_.getBuffer(), 0, buffer_.length());
}

} // namespace doris::segment_v2::inverted_index
