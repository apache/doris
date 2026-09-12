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

    // Retunes the boundary rate before any row of a segment is cut. See
    // GramExtractor::set_density_permille for why it may only happen there.
    void set_density_permille(uint16_t density_permille) {
        _extractor.set_density_permille(density_permille);
    }

    // Bytes of buffer capacity this tokenizer keeps between rows: the gram list plus the
    // extractor's scratch. Both are sized by the longest row seen so far and are never shrunk,
    // so this is a steady resident cost rather than a per-row spike -- which is exactly the shape
    // SNII's MemoryReporter wants (SniiIndexColumnWriter mirrors it, delta-charged, after every
    // row). It only means anything while the tokenizer outlives the row, i.e. when the writer
    // obtained it from reusableTokenStream.
    size_t reserved_bytes() const {
        return _grams.capacity() * sizeof(std::string_view) + _extractor.reserved_bytes();
    }

private:
    gram::GramExtractor _extractor;
    const char* _char_buffer = nullptr;
    int32_t _char_length = 0;
    std::vector<std::string_view> _grams; // views into _char_buffer or the extractor's fold copy
    size_t _next = 0;
};

} // namespace doris::segment_v2::inverted_index
