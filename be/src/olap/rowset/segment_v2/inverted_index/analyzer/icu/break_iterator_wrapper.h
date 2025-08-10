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
#include <unordered_set>

#include "icu_common.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class BreakIteratorWrapper {
public:
    BreakIteratorWrapper(icu::BreakIterator* rbbi);
    ~BreakIteratorWrapper() = default;

    void initialize();
    int32_t current() { return rbbi_->current(); }
    int32_t get_rule_status() const { return status_; }
    int32_t next();
    int32_t calc_status(int32_t current, int32_t next);
    bool is_emoji(int32_t current, int32_t next);
    void set_text(const UChar* text, int32_t start, int32_t length);

private:
    static icu::UnicodeSet EMOJI_RK;
    static icu::UnicodeSet EMOJI;

    BreakIteratorPtr rbbi_;
    const UChar* text_ = nullptr;
    int32_t start_ = 0;
    int32_t status_ = UBRK_WORD_NONE;
};
using BreakIteratorWrapperPtr = std::unique_ptr<BreakIteratorWrapper>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2