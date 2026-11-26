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

#include "storage/index/inverted/analysis/tokenizer/icu/composite_break_iterator.h"

#include <unicode/uchar.h>

#include <memory>
#include <utility>

namespace doris::segment_v2::inverted_index {

CompositeBreakIterator::CompositeBreakIterator(ICUTokenizerConfigPtr config)
        : config_(std::move(config)) {
    word_breakers_.resize(u_getIntPropertyMaxValue(UCHAR_SCRIPT) + 1);
}

void CompositeBreakIterator::set_text(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    scriptIterator_.set_text(text_, start, length);
    if (scriptIterator_.next()) {
        set_run_text();
    } else {
        rbbi_ = get_break_iterator(USCRIPT_COMMON);
        rbbi_->set_text(&utext_, text_, 0, 0);
    }
}

void CompositeBreakIterator::set_run_text() {
    rbbi_ = get_break_iterator(scriptIterator_.get_script_code());
    rbbi_->set_text(&utext_, text_, scriptIterator_.get_script_start(),
                    scriptIterator_.get_script_limit() - scriptIterator_.get_script_start());
}

BreakIteratorWrapper* CompositeBreakIterator::get_break_iterator(int32_t scriptCode) {
    if (!word_breakers_[scriptCode]) {
        word_breakers_[scriptCode] =
                std::make_unique<BreakIteratorWrapper>(config_->get_break_iterator(scriptCode));
    }
    return word_breakers_[scriptCode].get();
}

} // namespace doris::segment_v2::inverted_index
