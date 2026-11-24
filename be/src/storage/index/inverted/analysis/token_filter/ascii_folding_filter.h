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
#include <optional>
#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/per_token_stream.h"
#include "storage/index/inverted/analysis/text_token_stream.h"
#include "storage/index/inverted/analysis/token_batch.h"

namespace doris::segment_v2::inverted_index {

// Folds the letters, digits and symbols of every token to their ASCII equivalents where it stands
// in the batch, one token for one: a term that folding changes is replaced by a view of its folded
// form in term storage, the rest keep their views. preserve_original can make two tokens of one,
// so ASCIIFoldingPreserveOriginalFilter does that instead.
class ASCIIFoldingFilter final : public TextTokenFilter {
public:
    explicit ASCIIFoldingFilter(std::unique_ptr<TextTokenStream> in);

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override;
    // The scratch of one folded token, and the input.
    size_t reserved_bytes() const override {
        return TextTokenFilter::reserved_bytes() + _output.capacity();
    }

    static int32_t fold_to_ascii(const char* in, int32_t input_pos, char* out, int32_t output_pos,
                                 int32_t length);

private:
    std::string _output;
};

// asciifolding with preserve_original: a token that folding changes is followed by the original at
// the same position, e.g. "café" makes "cafe" and then "café" with a position increment of 0.
class ASCIIFoldingPreserveOriginalFilter final : public PerTokenFilter {
public:
    explicit ASCIIFoldingPreserveOriginalFilter(std::unique_ptr<TextTokenStream> in)
            : PerTokenFilter(std::move(in)) {}

    // The scratch of one folded token, and the input.
    size_t reserved_bytes() const override {
        return PerTokenFilter::reserved_bytes() + _output.capacity();
    }

protected:
    void on_reset() override;
    bool next_token(TokenRef& token) override;

private:
    // The original of the token last folded, still to come out.
    std::optional<std::string_view> _original;
    std::string _output;
};

} // namespace doris::segment_v2::inverted_index
