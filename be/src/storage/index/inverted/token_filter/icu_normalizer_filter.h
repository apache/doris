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
#include <span>
#include <string>
#include <vector>

#include "storage/index/inverted/token_filter/token_filter.h"

namespace doris::segment_v2::inverted_index {

class ICUNormalizerFilter : public DorisTokenFilter {
public:
    ICUNormalizerFilter(TokenStreamPtr in, std::shared_ptr<const icu::Normalizer2> normalizer);
    ~ICUNormalizerFilter() override = default;

    Token* next(Token* t) override;
    void reset() override;

    std::span<const int32_t> get_source_byte_offsets() const override;
    std::span<const int32_t> get_source_byte_end_offsets() const override;

private:
    std::shared_ptr<const icu::Normalizer2> _normalizer;
    std::string _output_buffer;
    std::vector<int32_t> _source_byte_offsets;
    std::vector<int32_t> _source_byte_end_offsets;
    bool _has_normalized_offsets = false;
};
using ICUNormalizerFilterPtr = std::shared_ptr<ICUNormalizerFilter>;

} // namespace doris::segment_v2::inverted_index
