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

#include <optional>
#include <string_view>

#include "token_filter.h"

namespace doris::segment_v2::inverted_index {

class ASCIIFoldingFilter : public DorisTokenFilter {
public:
    ASCIIFoldingFilter(const TokenStreamPtr& in, bool preserve_original);
    ~ASCIIFoldingFilter() override = default;

    Token* next(Token* t) override;
    void reset() override;

    static int32_t fold_to_ascii(const char* in, int32_t input_pos, char* out, int32_t output_pos,
                                 int32_t length);

private:
    void fold_to_ascii(const char* in, int32_t length);
    bool need_to_preserve(const char* in, int32_t input_length);

    std::optional<std::string_view> _state;

    bool _preserve_original = false;
    int32_t _output_pos;
    std::string _output;
};

} // namespace doris::segment_v2::inverted_index