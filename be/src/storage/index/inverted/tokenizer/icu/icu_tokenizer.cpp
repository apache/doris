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
    int32_t source_start = 0;
    int32_t source_end = 0;
    if (start >= 0 && length >= 0 && advance_source_offset(start, source_start) &&
        advance_source_offset(start + length, source_end)) {
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
    sourceBuffer_ = buf;
    sourceLength_ = len;
    sourceUtf8Offset_ = 0;
    sourceUtf16Offset_ = 0;
    sourceOffsetsValid_ = true;
    breaker_->set_text(buffer_.getBuffer(), 0, buffer_.length());
}

bool ICUTokenizer::advance_source_offset(int32_t utf16_offset, int32_t& utf8_offset) {
    if (!sourceOffsetsValid_ || utf16_offset < sourceUtf16Offset_) {
        return false;
    }

    while (sourceUtf16Offset_ < utf16_offset && sourceUtf8Offset_ < sourceLength_) {
        const int32_t code_point_start = sourceUtf8Offset_;
        UChar32 code_point;
        U8_NEXT(sourceBuffer_, sourceUtf8Offset_, sourceLength_, code_point);
        if (code_point < 0) {
            sourceOffsetsValid_ = false;
            return false;
        }

        const int32_t next_utf16_offset = sourceUtf16Offset_ + U16_LENGTH(code_point);
        sourceUtf16Offset_ = next_utf16_offset;
        if (utf16_offset < next_utf16_offset) {
            utf8_offset = code_point_start;
            return true;
        }
    }

    if (sourceUtf16Offset_ != utf16_offset) {
        sourceOffsetsValid_ = false;
        return false;
    }
    utf8_offset = sourceUtf8Offset_;
    return true;
}

} // namespace doris::segment_v2::inverted_index
