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

#include "storage/index/inverted/analysis/normalizer/custom_normalizer.h"

#include <vector>

#include "common/exception.h"
#include "storage/index/inverted/analysis/analyzer/custom_analyzer.h"

namespace doris::segment_v2::inverted_index {

TextAnalyzerPtr CustomNormalizer::build_custom_normalizer(const CustomNormalizerConfigPtr& config) {
    if (config == nullptr) {
        throw Exception(ErrorCode::ILLEGAL_STATE, "Null configuration detected.");
    }
    const std::vector<ComponentConfigPtr> char_filters = config->get_char_filter_configs();
    const std::vector<ComponentConfigPtr> token_filters = config->get_token_filter_configs();
    if (char_filters.empty() && token_filters.empty()) {
        throw Exception(ErrorCode::ILLEGAL_STATE,
                        "Normalizer must have at least one char_filter or token_filter.");
    }
    return CustomAnalyzer::build_chain(char_filters, ComponentConfig("keyword", Settings {}),
                                       token_filters);
}

} // namespace doris::segment_v2::inverted_index
