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

#include "icu_tokenizer_config.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class DefaultICUTokenizerConfig : public ICUTokenizerConfig {
public:
    DefaultICUTokenizerConfig(bool cjkAsWords, bool myanmarAsWords);
    ~DefaultICUTokenizerConfig() override = default;

    void initialize(const std::string& dictPath) override;
    bool combine_cj() override { return cjk_as_words_; }
    icu::BreakIterator* get_break_iterator(int32_t script) override;

private:
    static void read_break_iterator(BreakIteratorPtr& rbbi, const std::string& filename);

    static BreakIteratorPtr cjk_break_iterator_;
    static BreakIteratorPtr default_break_iterator_;
    static BreakIteratorPtr myanmar_syllable_iterator_;

    bool cjk_as_words_ = false;
    bool myanmar_as_words_ = false;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2