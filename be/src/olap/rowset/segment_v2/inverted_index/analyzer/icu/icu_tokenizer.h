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

#include <unicode/utext.h>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "composite_break_iterator.h"
#include "default_icu_tokenizer_config.h"
#include "icu_common.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

class ICUTokenizer : public Tokenizer {
public:
    ICUTokenizer();
    ICUTokenizer(bool lowercase, bool ownReader);
    ~ICUTokenizer() override = default;

    void initialize(const std::string& dictPath);
    Token* next(Token* token) override;
    void reset(lucene::util::Reader* reader) override;

private:
    std::string utf8Str_;
    icu::UnicodeString buffer_;

    ICUTokenizerConfigPtr config_;
    CompositeBreakIteratorPtr breaker_;
};

} // namespace doris::segment_v2