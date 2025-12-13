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

#include "olap/rowset/segment_v2/inverted_index/analyzer/custom_analyzer_config.h"

namespace doris::segment_v2::inverted_index {

class CustomNormalizerConfig;
using CustomNormalizerConfigPtr = std::shared_ptr<CustomNormalizerConfig>;

class CustomNormalizerConfig {
public:
    class Builder {
    public:
        Builder() = default;
        ~Builder() = default;

        void add_char_filter_config(const std::string& name, const Settings& params);
        void add_token_filter_config(const std::string& name, const Settings& params);
        CustomNormalizerConfigPtr build();

    private:
        std::vector<ComponentConfigPtr> _char_filters;
        std::vector<ComponentConfigPtr> _token_filters;

        friend class CustomNormalizerConfig;
    };

    CustomNormalizerConfig(Builder* builder);
    ~CustomNormalizerConfig() = default;

    std::vector<ComponentConfigPtr> get_char_filter_configs();
    std::vector<ComponentConfigPtr> get_token_filter_configs();

private:
    std::vector<ComponentConfigPtr> _char_filters;
    std::vector<ComponentConfigPtr> _token_filters;
};

} // namespace doris::segment_v2::inverted_index