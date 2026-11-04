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

#pragma once

#include <unicode/umachine.h>
#include <unicode/utext.h>

#include <memory>
#include <vector>

#include "storage/index/inverted/analysis/tokenizer/icu/break_iterator_wrapper.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_common.h"
#include "storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_config.h"
#include "storage/index/inverted/analysis/tokenizer/icu/script_iterator.h"

namespace doris::segment_v2::inverted_index {

// The boundaries of a text, each script run broken by the iterator of its script. The iterators
// and the UText they read through are kept from one text to the next. The boundary after the end
// of a run is the start of the next run, so a word never spans two runs: in "abc中文", "abc" ends
// where 中文 starts.
class CompositeBreakIterator {
public:
    explicit CompositeBreakIterator(ICUTokenizerConfigPtr config);
    ~CompositeBreakIterator() { utext_close(&utext_); }

    CompositeBreakIterator(const CompositeBreakIterator&) = delete;
    CompositeBreakIterator& operator=(const CompositeBreakIterator&) = delete;

    // The next boundary of the text, or UBRK_DONE after the last one; the first boundary, 0, is
    // never returned.
    int32_t next() {
        // One call of the inline BreakIteratorWrapper::next(), which the compiler then inlines.
        for (;;) {
            const int32_t next = rbbi_->next();
            if (next != UBRK_DONE) {
                return next + scriptIterator_.get_script_start();
            }
            if (!scriptIterator_.next()) {
                return UBRK_DONE;
            }
            set_run_text();
        }
    }
    // The rule status of the boundary next() returned: 0 when the text before it is no word.
    int32_t get_rule_status() const { return rbbi_->get_rule_status(); }
    int32_t get_script_code() const { return scriptIterator_.get_script_code(); }
    void set_text(const UChar* text, int32_t start, int32_t length);

private:
    BreakIteratorWrapper* get_break_iterator(int32_t scriptCode);
    // Points the iterator of the current script run at the run.
    void set_run_text();

    const UChar* text_ = nullptr;

    ICUTokenizerConfigPtr config_;
    std::vector<BreakIteratorWrapperPtr> word_breakers_;
    BreakIteratorWrapper* rbbi_ = nullptr;
    ScriptIterator scriptIterator_;
    UText utext_ = UTEXT_INITIALIZER;
};
using CompositeBreakIteratorPtr = std::unique_ptr<CompositeBreakIterator>;

} // namespace doris::segment_v2::inverted_index
