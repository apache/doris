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

#include <cstdint>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/gram/gram_extractor.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer.h"

namespace doris::segment_v2::inverted_index {

// The gram tokenizer inside a CLucene chain. A gram-family analyzer is a GramTextAnalyzer and
// never reaches here: a CLucene chain meets an ngram tokenizer with "mode" only in an analyzer
// that also has a char or token filter. The FE refuses such an analyzer
// (IndexPolicyMgr.validateGramFamilyAnalyzerLocked) but does not check a policy again when it is
// replayed, so one created before that check would still come here. It goes with the CLucene
// chains.
//
// reset() reads the reader's whole input and hands it to GramExtractor; next() yields the grams in
// order.
class LuceneGramTokenizer : public DorisTokenizer {
public:
    explicit LuceneGramTokenizer(const gram::GramScheme& scheme) : _extractor(scheme) {}
    ~LuceneGramTokenizer() override = default;

    Token* next(Token* token) override;
    void reset() override;

private:
    gram::GramExtractor _extractor;
    const char* _char_buffer = nullptr;
    int32_t _char_length = 0;
    std::vector<std::string_view> _grams; // views into _char_buffer or the extractor's fold copy
    size_t _next = 0;
};

} // namespace doris::segment_v2::inverted_index
