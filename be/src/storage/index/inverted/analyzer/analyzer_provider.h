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
#include <string_view>

#include "storage/index/inverted/abstract_analysis_factory.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"

namespace lucene::analysis {
class Analyzer;
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
};
using AnalyzerProviderPtr = std::shared_ptr<const AnalyzerProvider>;

} // namespace doris::segment_v2::inverted_index
