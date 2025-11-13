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

#include "custom_normalizer_config.h"

namespace doris::segment_v2::inverted_index {

CustomNormalizerConfig::CustomNormalizerConfig(Builder* builder) {
    _char_filters = builder->_char_filters;
    _token_filters = builder->_token_filters;
}

std::vector<ComponentConfigPtr> CustomNormalizerConfig::get_char_filter_configs() {
    return _char_filters;
}

std::vector<ComponentConfigPtr> CustomNormalizerConfig::get_token_filter_configs() {
    return _token_filters;
}

void CustomNormalizerConfig::Builder::add_char_filter_config(const std::string& name,
                                                             const Settings& params) {
    _char_filters.emplace_back(std::make_shared<ComponentConfig>(name, params));
}

void CustomNormalizerConfig::Builder::add_token_filter_config(const std::string& name,
                                                              const Settings& params) {
    _token_filters.emplace_back(std::make_shared<ComponentConfig>(name, params));
}

CustomNormalizerConfigPtr CustomNormalizerConfig::Builder::build() {
    return std::make_shared<CustomNormalizerConfig>(this);
}

} // namespace doris::segment_v2::inverted_index