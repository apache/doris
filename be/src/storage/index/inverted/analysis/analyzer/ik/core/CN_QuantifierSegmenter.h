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

#include <memory>
#include <unordered_set>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/ik/core/AnalyzeContext.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/ISegmenter.h"
namespace doris::segment_v2 {

class CN_QuantifierSegmenter final : public ISegmenter {
public:
    static constexpr AnalyzeContext::SegmenterType SEGMENTER_TYPE =
            AnalyzeContext::SegmenterType::CN_QUANTIFIER;
    static const std::u32string CHINESE_NUMBERS;
    static const std::unordered_set<char32_t> CHINESE_NUMBER_CHARS;

    explicit CN_QuantifierSegmenter(const Dictionary& dictionary);
    ~CN_QuantifierSegmenter() override = default;

    void analyze(AnalyzeContext& context) override;
    void reset() override;
    // No numeral and no quantifier in progress: analyze() then only releases the lock on a
    // character that is not Chinese.
    bool isIdle() const { return number_start_ == -1 && number_end_ == -1 && count_hits_.empty(); }

private:
    void processCNumber(AnalyzeContext& context);
    void processCount(AnalyzeContext& context);
    bool needCountScan(AnalyzeContext& context);
    void outputNumLexeme(AnalyzeContext& context);

    const Dictionary& dictionary_;
    int number_start_;
    int number_end_;
    std::vector<Hit> count_hits_;
};
} // namespace doris::segment_v2
