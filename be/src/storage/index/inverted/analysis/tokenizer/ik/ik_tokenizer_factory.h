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

#include "common/config.h"
#include "storage/index/inverted/analysis/analyzer/ik/IKTokenizer.h"
#include "storage/index/inverted/analysis/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class IKTokenizerFactory : public TokenizerFactory {
public:
    // A custom ik tokenizer always lowercases its words; the builtin ik parser follows lower_case.
    explicit IKTokenizerFactory(bool use_smart, bool lowercase = true)
            : _config(std::make_shared<Configuration>(use_smart, true)), _lowercase(lowercase) {
        _config->setDictPath(config::inverted_index_dict_path + "/ik");
    }
    ~IKTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {}

    // Loads the dictionaries unless they are loaded already; throws when they cannot be.
    void load_dictionary() const { Dictionary::initial(*_config); }

    std::unique_ptr<TextTokenizer> create() const override {
        load_dictionary();
        return std::make_unique<IKTokenizer>(_config, _lowercase);
    }

private:
    // Dictionary matching always lowercases ASCII; `_lowercase` only applies to the words. The
    // streams share this configuration and only read it.
    const std::shared_ptr<Configuration> _config;
    const bool _lowercase;
};

} // namespace doris::segment_v2::inverted_index
