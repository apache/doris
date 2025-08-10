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
#include <string>
#include <unordered_map>

#include "olap/rowset/segment_v2/inverted_index/setting.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class ComponentConfig;
using ComponentConfigPtr = std::shared_ptr<ComponentConfig>;

class CustomAnalyzerConfig;
using CustomAnalyzerConfigPtr = std::shared_ptr<CustomAnalyzerConfig>;

class CustomAnalyzerConfig {
public:
    class Builder {
    public:
        Builder() = default;
        ~Builder() = default;

        void with_tokenizer_config(const std::string& name, const Settings& params);
        void add_token_filter_config(const std::string& name, const Settings& params);
        CustomAnalyzerConfigPtr build();

    private:
        ComponentConfigPtr _tokenizer_config;
        std::vector<ComponentConfigPtr> _token_filters;

        friend class CustomAnalyzerConfig;
    };

    CustomAnalyzerConfig(Builder* builder);
    ~CustomAnalyzerConfig() = default;

    ComponentConfigPtr get_tokenizer_config();
    std::vector<ComponentConfigPtr> get_token_filter_configs();

private:
    ComponentConfigPtr _tokenizer_config;
    std::vector<ComponentConfigPtr> _token_filters;
};

class ComponentConfig {
public:
    ComponentConfig(std::string name, Settings params);
    ~ComponentConfig() = default;

    std::string get_name() const;
    Settings get_params() const;

private:
    std::string _name;
    Settings _params;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index