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

#include <memory>
#include <string>

#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2::inverted_index {

// Normalizes every token with an ICU normalizer, NFKC_CF by default.
class ICUNormalizerFilter final : public PerTokenFilter {
public:
    ICUNormalizerFilter(std::unique_ptr<TextTokenStream> in,
                        std::shared_ptr<const icu::Normalizer2> normalizer);

    // The scratch of one normalized token, and the input.
    size_t reserved_bytes() const override {
        return PerTokenFilter::reserved_bytes() + _output_buffer.capacity();
    }

protected:
    void on_reset() override;
    bool next_token(TokenRef& token) override;

private:
    std::shared_ptr<const icu::Normalizer2> _normalizer;
    std::string _output_buffer;
};

} // namespace doris::segment_v2::inverted_index
