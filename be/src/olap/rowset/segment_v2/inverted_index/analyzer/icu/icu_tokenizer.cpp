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

#include "icu_tokenizer.h"

#include <unicode/unistr.h>

#include <memory>
#include <string>

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"
ICUTokenizer::ICUTokenizer() {
    this->lowercase = false;
    this->ownReader = false;

    config_ = std::make_shared<DefaultICUTokenizerConfig>(true, true);
    breaker_ = std::make_unique<CompositeBreakIterator>(config_);
}

ICUTokenizer::ICUTokenizer(bool lower_case, bool own_reader) : ICUTokenizer() {
    this->lowercase = lower_case;
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
    if (this->lowercase) {
        subString.toLower().toUTF8String(utf8Str_);
    } else {
        subString.toUTF8String(utf8Str_);
    }

    token->setNoCopy(utf8Str_.data(), 0, static_cast<int32_t>(utf8Str_.size()));
    return token;
}

void ICUTokenizer::reset(lucene::util::Reader* reader) {
    const char* buf = nullptr;
    int32_t len = reader->read((const void**)&buf, 0, static_cast<int32_t>(reader->size()));
    buffer_ = icu::UnicodeString::fromUTF8(icu::StringPiece(buf, len));
    if (!buffer_.isEmpty() && buffer_.isBogus()) {
        _CLTHROWT(CL_ERR_Runtime, "Failed to convert UTF-8 string to UnicodeString.");
    }
    breaker_->set_text(buffer_.getBuffer(), 0, buffer_.length());
}

} // namespace doris::segment_v2
#include "common/compile_check_end.h"