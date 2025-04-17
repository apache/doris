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

#include <algorithm>
#include <memory>
#include <vector>

#include "AnalyzeContext.h"
#include "ISegmenter.h"

namespace doris::segment_v2 {

class LetterSegmenter : public ISegmenter {
public:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::LETTER_SEGMENTER;
    LetterSegmenter();
    ~LetterSegmenter() override = default;

    void analyze(AnalyzeContext& context) override;
    void reset() override;

private:
    bool processEnglishLetter(AnalyzeContext& context);
    bool processArabicLetter(AnalyzeContext& context);
    bool processMixLetter(AnalyzeContext& context);
    bool isLetterConnector(int32_t input);
    bool isNumConnector(int32_t input);

    Lexeme createLexeme(AnalyzeContext& context, int start, int end, Lexeme::Type type);

    int start_ {-1};
    int end_ {-1};
    int english_start_ {-1};
    int english_end_ {-1};
    int arabic_start_ {-1};
    int arabic_end_ {-1};

    std::vector<char> letter_connectors_;
    std::vector<char> num_connectors_;
};
} // namespace doris::segment_v2