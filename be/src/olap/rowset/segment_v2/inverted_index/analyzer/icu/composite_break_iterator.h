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
#include <unicode/unistr.h>
#include <unicode/utext.h>

#include <memory>
#include <vector>

#include "break_iterator_wrapper.h"
#include "icu_common.h"
#include "icu_tokenizer_config.h"
#include "script_iterator.h"

namespace doris::segment_v2 {

class CompositeBreakIterator {
public:
    CompositeBreakIterator(const ICUTokenizerConfigPtr& config);
    ~CompositeBreakIterator() = default;

    void initialize();
    int32_t next();
    int32_t current();
    int32_t get_rule_status();
    int32_t get_script_code();
    void set_text(const UChar* text, int32_t start, int32_t length);

private:
    BreakIteratorWrapper* get_break_iterator(int32_t scriptCode);

    const UChar* text_ = nullptr;

    ICUTokenizerConfigPtr config_;
    std::vector<BreakIteratorWrapperPtr> word_breakers_;
    BreakIteratorWrapper* rbbi_ = nullptr;
    ScriptIteratorPtr scriptIterator_;
};
using CompositeBreakIteratorPtr = std::unique_ptr<CompositeBreakIterator>;

} // namespace doris::segment_v2