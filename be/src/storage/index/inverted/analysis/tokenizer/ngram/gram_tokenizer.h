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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/gram/gram_extractor.h"
#include "storage/index/inverted/analysis/gram/gram_scheme.h"
#include "storage/index/inverted/analysis/text_analyzer.h"

namespace doris::segment_v2::inverted_index {

// The tokenizer of the gram family: the grams of a value, each one position after the last.
// reset() extracts all of them; next_batch() hands them out as views of the value or of the
// extractor's folded copy, without copying.
class GramTokenizer final : public TextTokenizer {
public:
    explicit GramTokenizer(const gram::GramScheme& scheme) : _extractor(scheme) {}

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The gram list plus the extractor's scratch, both sized by the last value and released above
    // the high water mark at the next reset(). The SNII writer reports it after every row.
    size_t reserved_bytes() const override;

    const gram::GramScheme& scheme() const { return _extractor.scheme(); }

    // Retunes the boundary rate before any row of a segment is cut. See
    // GramExtractor::set_density_permille for why it may only happen there.
    void set_density_permille(uint16_t density_permille) {
        _extractor.set_density_permille(density_permille);
    }

private:
    gram::GramExtractor _extractor;
    std::vector<std::string_view> _grams; // views into the value or the extractor's fold copy
    size_t _next = 0;
};

// The analyzer of a gram-family index. The FE gives a gram-family analyzer no char filter and no
// token filter, so the GramTokenizer is the whole stream, and the SNII writer casts its stream to
// one to retune the density.
class GramTextAnalyzer final : public TextAnalyzer {
public:
    explicit GramTextAnalyzer(const gram::GramScheme& scheme) : _scheme(scheme) {}

    std::unique_ptr<TextTokenStream> create_stream() const override {
        return std::make_unique<GramTokenizer>(_scheme);
    }

private:
    const gram::GramScheme _scheme;
};

} // namespace doris::segment_v2::inverted_index
