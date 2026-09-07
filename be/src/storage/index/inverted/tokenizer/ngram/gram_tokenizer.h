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
#include <string_view>
#include <vector>

#include "storage/index/inverted/gram/gram_extractor.h"
#include "storage/index/inverted/tokenizer/tokenizer.h"

namespace doris::segment_v2::inverted_index {

// Adapts gram::GramExtractor to the DorisTokenizer interface: one column value = one reset, with
// every gram extracted at once. Unlike NGramTokenizer, which slides a window byte by byte, there
// is no incremental window state here: reset() reads the reader's entire input in one go and
// hands it to GramExtractor, and next() just yields the results in order.
class GramTokenizer : public DorisTokenizer {
public:
    explicit GramTokenizer(const gram::GramScheme& scheme) : _extractor(scheme) {}
    ~GramTokenizer() override = default;

    Token* next(Token* token) override;
    void reset() override;
    const gram::GramScheme& scheme() const { return _extractor.scheme(); }

private:
    gram::GramExtractor _extractor;
    const char* _char_buffer = nullptr;
    int32_t _char_length = 0;
    std::vector<std::string_view> _grams; // views into _char_buffer or the extractor's fold copy
    size_t _next = 0;
};

} // namespace doris::segment_v2::inverted_index
