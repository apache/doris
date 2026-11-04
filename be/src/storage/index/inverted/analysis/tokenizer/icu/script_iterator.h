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
#include <unicode/uscript.h>

#include <cstdint>

#include "storage/index/inverted/analysis/tokenizer/icu/icu_char_table.h"

namespace doris::segment_v2::inverted_index {

// The runs of one script in a UTF-16 text, by ICUCharTable::script(): characters of the Common
// and Inherited scripts and combining marks join the run they are in, and a character joins a run
// of another script when its script extensions name that script.
class ScriptIterator {
public:
    ScriptIterator() : table_(ICUCharTable::instance()) {}

    int32_t get_script_start() const { return script_start_; }
    int32_t get_script_limit() const { return script_limit_; }
    int32_t get_script_code() const { return script_code_; }

    bool next();
    void set_text(const UChar* text, int32_t start, int32_t length);

private:
    // The end of the ASCII units from `index`, a block of 8 at a time, so up to 7 short of it. A
    // Latin run takes every ASCII character, all of which are Latin or Common, so it may jump over
    // them: an ASCII value is one run.
    static int32_t skip_ascii(const UChar* text, int32_t index, int32_t limit);

    const ICUCharTable& table_;

    const UChar* text_ = nullptr;
    int32_t start_ = 0;
    int32_t index_ = 0;
    int32_t limit_ = 0;

    int32_t script_start_ = 0;
    int32_t script_limit_ = 0;
    int32_t script_code_ = USCRIPT_INVALID_CODE;
};

} // namespace doris::segment_v2::inverted_index
