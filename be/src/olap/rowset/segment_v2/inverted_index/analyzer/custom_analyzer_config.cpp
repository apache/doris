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

#include "custom_analyzer_config.h"

#include "olap/rowset/segment_v2/inverted_index/setting.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

CustomAnalyzerConfig::CustomAnalyzerConfig(Builder* builder) {
    _tokenizer_config = builder->_tokenizer_config;
    _token_filters = builder->_token_filters;
}

ComponentConfigPtr CustomAnalyzerConfig::get_tokenizer_config() {
    return _tokenizer_config;
}

std::vector<ComponentConfigPtr> CustomAnalyzerConfig::get_token_filter_configs() {
    return _token_filters;
}

void CustomAnalyzerConfig::Builder::with_tokenizer_config(const std::string& name,
                                                          const Settings& params) {
    _tokenizer_config = std::make_shared<ComponentConfig>(name, params);
}

void CustomAnalyzerConfig::Builder::add_token_filter_config(const std::string& name,
                                                            const Settings& params) {
    _token_filters.emplace_back(std::make_shared<ComponentConfig>(name, params));
}

CustomAnalyzerConfigPtr CustomAnalyzerConfig::Builder::build() {
    return std::make_shared<CustomAnalyzerConfig>(this);
}

ComponentConfig::ComponentConfig(std::string name, Settings params)
        : _name(std::move(name)), _params(std::move(params)) {}

std::string ComponentConfig::get_name() const {
    return _name;
}

Settings ComponentConfig::get_params() const {
    return _params;
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index