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
#include "storage/index/inverted/analyzer/ik/IKTokenizer.h"
#include "storage/index/inverted/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

class IKTokenizerFactory : public TokenizerFactory {
public:
    explicit IKTokenizerFactory(bool use_smart) : _use_smart(use_smart) {}
    ~IKTokenizerFactory() override = default;

    void initialize(const Settings& settings) override {}

    TokenizerPtr create() override {
        auto ik_config = std::make_shared<Configuration>(_use_smart, true);
        ik_config->setDictPath(config::inverted_index_dict_path + "/ik");
        Dictionary::initial(*ik_config);
        return std::make_shared<IKTokenizer>(ik_config, true, false);
    }

    PositionCapability position_capability() const override {
        return PositionCapability::kAlwaysUnitIncrement;
    }

private:
    bool _use_smart;
};

} // namespace doris::segment_v2::inverted_index
