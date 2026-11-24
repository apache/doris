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

#include <unicode/normalizer2.h>

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/token_batch.h"

namespace doris::segment_v2::inverted_index {

// Normalizes every token with an ICU normalizer, NFKC_CF by default, where it stands in the batch,
// one token for one. A term the normalizer's quick check passes keeps its view; any other is
// normalized into the term storage of the batch.
class ICUNormalizerFilter final : public TextTokenFilter {
public:
    ICUNormalizerFilter(std::unique_ptr<TextTokenStream> in,
                        std::shared_ptr<const icu::Normalizer2> normalizer);

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The scratch of one normalized token, and the input.
    size_t reserved_bytes() const override {
        return TextTokenFilter::reserved_bytes() + _output_buffer.capacity();
    }

private:
    void normalize(TokenRef& token, TokenBatch& batch);

    std::shared_ptr<const icu::Normalizer2> _normalizer;
    // The UTF-8 of the token last normalized, before it is copied into the batch.
    std::string _output_buffer;
};

} // namespace doris::segment_v2::inverted_index
