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
#include <string_view>

#include "storage/index/inverted/analysis/char_filter/char_filter.h"

namespace doris::segment_v2::inverted_index {

// Normalizes the text with an ICU normalizer, NFKC_CF by default.
class ICUNormalizerCharFilter final : public TextCharFilter {
public:
    explicit ICUNormalizerCharFilter(std::shared_ptr<const icu::Normalizer2> normalizer);

    std::string_view apply(std::string_view text, std::string& scratch) override;

private:
    const std::shared_ptr<const icu::Normalizer2> _normalizer;
};

} // namespace doris::segment_v2::inverted_index
