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
#include <optional>
#include <string_view>

#include "storage/index/inverted/abstract_analysis_factory.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"

namespace lucene::analysis {
class Analyzer;
}

// 只前置声明，不 #include gram_scheme.h：这个头文件挂在 tablet_schema.h ->
// inverted_index_parser.h 这条几乎所有 TU 都会拉到的路径上（exec/pipeline/dependency.h 的
// 前向闭包预算长期卡在 357），gram_scheme.h 本身很小但仍会把该闭包挤过预算。真正需要完整
// 定义的地方（CustomAnalyzerProvider 的 _gram_scheme 成员、gram_scheme() 的实际实现）分别在
// custom_analyzer.h 和 analyzer_provider.cpp 里包含它。
namespace doris::segment_v2::gram {
struct GramScheme;
}

namespace doris::segment_v2::inverted_index {

class CommonWordSet;

class AnalyzerProvider {
public:
    virtual ~AnalyzerProvider() = default;
    virtual std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            AnalysisPurpose purpose) const = 0;
    virtual std::string_view base_analyzer_fingerprint() const { return {}; }
    virtual bool uses_common_grams() const { return false; }
    virtual const CommonGramsQueryIdentity* common_grams_identity() const { return nullptr; }
    virtual const CommonWordSet* common_grams_word_set() const { return nullptr; }
    // gram 族识别：仅当 tokenizer 是 ngram 且携带 "mode" 属性（sparse|dense|auto）时，
    // CustomAnalyzerProvider 会覆写此函数返回有值的 GramScheme；其余 provider（内置 parser、
    // normalizer 等）沿用 analyzer_provider.cpp 里的默认实现，恒为 nullopt。默认实现放在
    // .cpp 而非这里内联，是因为 GramScheme 在此处只前置声明（见上方注释）。
    virtual std::optional<gram::GramScheme> gram_scheme() const;
};
using AnalyzerProviderPtr = std::shared_ptr<const AnalyzerProvider>;

} // namespace doris::segment_v2::inverted_index
