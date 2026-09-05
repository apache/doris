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

// Forward declaration only, no #include of gram_scheme.h: this header sits on the
// tablet_schema.h -> inverted_index_parser.h path that almost every TU pulls in (the forward
// include closure budget of exec/pipeline/dependency.h has long been pinned at 357), and
// gram_scheme.h, small as it is, would still push that closure over budget. The places that
// really need the complete definition (the _gram_scheme member of CustomAnalyzerProvider and the
// actual implementation of gram_scheme()) include it in custom_analyzer.h and
// analyzer_provider.cpp respectively.
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
    // Gram-family detection: only when the tokenizer is ngram and carries a "mode" property
    // (sparse|dense|auto) does CustomAnalyzerProvider override this to return a populated
    // GramScheme; every other provider (built-in parsers, normalizers, ...) keeps the default
    // implementation in analyzer_provider.cpp, which always returns nullopt. That default lives
    // in the .cpp instead of being inlined here because GramScheme is only forward-declared at
    // this point (see the comment above).
    virtual std::optional<gram::GramScheme> gram_scheme() const;
};
using AnalyzerProviderPtr = std::shared_ptr<const AnalyzerProvider>;

} // namespace doris::segment_v2::inverted_index
