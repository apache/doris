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

#include "storage/index/inverted/analysis/tokenizer/icu/break_iterator_wrapper.h"

#include "common/check.h"

namespace doris::segment_v2::inverted_index {

BreakIteratorWrapper::BreakIteratorWrapper(icu::BreakIterator* rbbi)
        : table_(ICUCharTable::instance()), rbbi_(rbbi) {}

void BreakIteratorWrapper::set_text(UText* utext, const UChar* text, int32_t start,
                                    int32_t length) {
    text_ = text;
    start_ = start;

    // Neither fails: `utext` is a valid UText that needs no allocation, and the range lies in the
    // text.
    UErrorCode status = U_ZERO_ERROR;
    utext_openUChars(utext, text + start, length, &status);
    rbbi_->setText(utext, status);
    DORIS_CHECK(U_SUCCESS(status));

    // setText() moved the iterator to the first boundary.
    position_ = 0;
    status_ = UBRK_WORD_NONE;
}

} // namespace doris::segment_v2::inverted_index
