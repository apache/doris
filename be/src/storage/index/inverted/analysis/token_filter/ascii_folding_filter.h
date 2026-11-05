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
#include <string>
#include <string_view>

#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2::inverted_index {

// Folds the letters, digits and symbols of every token to their ASCII equivalents; with
// preserve_original, a token that changed is followed by the original at the same position.
class ASCIIFoldingFilter final : public PerTokenFilter {
public:
    ASCIIFoldingFilter(std::unique_ptr<TextTokenStream> in, bool preserve_original);

    // The scratch of one folded token, and the input.
    size_t reserved_bytes() const override {
        return PerTokenFilter::reserved_bytes() + _output.capacity();
    }

    static int32_t fold_to_ascii(const char* in, int32_t input_pos, char* out, int32_t output_pos,
                                 int32_t length);

protected:
    void on_reset() override;
    bool next_token(TokenRef& token) override;

private:
    void fold_to_ascii(const char* in, int32_t length);
    bool need_to_preserve(const char* in, int32_t input_length);

    // The original of the token last folded, still to come out.
    std::optional<std::string_view> _state;
    bool _preserve_original = false;
    int32_t _output_pos = 0;
    std::string _output;
};

} // namespace doris::segment_v2::inverted_index
