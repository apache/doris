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

#include "icu_common.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class ScriptIterator {
public:
    ScriptIterator(bool combine_cj);
    ~ScriptIterator() = default;

    void initialize();

    int32_t get_script_start() const { return script_start_; }
    int32_t get_script_limit() const { return script_limit_; }
    int32_t get_script_code() const { return script_code_; }

    bool next();
    void set_text(const UChar* text, int32_t start, int32_t length);

private:
    int32_t get_script(UChar32 codepoint) const;
    static bool is_same_script(int32_t current_script, int32_t script, UChar32 codepoint);
    static bool is_combining_mark(UChar32 codepoint);

    static std::vector<int32_t> k_basic_latin;

    const UChar* text_ = nullptr;
    int32_t start_ = 0;
    int32_t index_ = 0;
    int32_t limit_ = 0;

    int32_t script_start_ = 0;
    int32_t script_limit_ = 0;
    int32_t script_code_ = USCRIPT_INVALID_CODE;

    bool combine_cj_ = false;
};
using ScriptIteratorPtr = std::unique_ptr<ScriptIterator>;

#include "common/compile_check_end.h"
} // namespace doris::segment_v2