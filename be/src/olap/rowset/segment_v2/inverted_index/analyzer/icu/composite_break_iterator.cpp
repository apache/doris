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

#include "composite_break_iterator.h"

#include <unicode/unistr.h>

#include <memory>

namespace doris::segment_v2 {

CompositeBreakIterator::CompositeBreakIterator(const ICUTokenizerConfigPtr& config)
        : config_(config) {
    scriptIterator_ = std::make_unique<ScriptIterator>(config->combine_cj());
    word_breakers_.resize(u_getIntPropertyMaxValue(UCHAR_SCRIPT) + 1);
}

void CompositeBreakIterator::initialize() {
    scriptIterator_->initialize();
}

int32_t CompositeBreakIterator::next() {
    int32_t next = rbbi_->next();
    while (next == UBRK_DONE && scriptIterator_->next()) {
        rbbi_ = get_break_iterator(scriptIterator_->get_script_code());
        rbbi_->set_text(text_, scriptIterator_->get_script_start(),
                        scriptIterator_->get_script_limit() - scriptIterator_->get_script_start());
        next = rbbi_->next();
    }
    return (next == UBRK_DONE) ? UBRK_DONE : next + scriptIterator_->get_script_start();
}

int32_t CompositeBreakIterator::current() {
    int32_t current = rbbi_->current();
    return (current == UBRK_DONE) ? UBRK_DONE : current + scriptIterator_->get_script_start();
}

int32_t CompositeBreakIterator::get_rule_status() {
    return rbbi_->get_rule_status();
}

int32_t CompositeBreakIterator::get_script_code() {
    return scriptIterator_->get_script_code();
}

void CompositeBreakIterator::set_text(const UChar* text, int32_t start, int32_t length) {
    text_ = text;
    scriptIterator_->set_text(text_, start, length);
    if (scriptIterator_->next()) {
        rbbi_ = get_break_iterator(scriptIterator_->get_script_code());
        rbbi_->set_text(text_, scriptIterator_->get_script_start(),
                        scriptIterator_->get_script_limit() - scriptIterator_->get_script_start());
    } else {
        rbbi_ = get_break_iterator(USCRIPT_COMMON);
        rbbi_->set_text(text_, 0, 0);
    }
}

BreakIteratorWrapper* CompositeBreakIterator::get_break_iterator(int32_t scriptCode) {
    if (!word_breakers_[scriptCode]) {
        auto wordBreak =
                std::make_unique<BreakIteratorWrapper>(config_->get_break_iterator(scriptCode));
        wordBreak->initialize();
        word_breakers_[scriptCode].swap(wordBreak);
    }
    return word_breakers_[scriptCode].get();
}

} // namespace doris::segment_v2