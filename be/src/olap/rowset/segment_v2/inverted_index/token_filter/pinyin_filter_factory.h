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

#include "olap/rowset/segment_v2/inverted_index/token_filter/pinyin_filter.h"
#include "olap/rowset/segment_v2/inverted_index/token_filter/token_filter_factory.h"
#include "olap/rowset/segment_v2/inverted_index/tokenizer/pinyin/pinyin_config.h"

namespace doris::segment_v2::inverted_index {

class PinyinFilterFactory : public TokenFilterFactory {
public:
    PinyinFilterFactory() = default;
    ~PinyinFilterFactory() override = default;

    void initialize(const Settings& settings) override;

    TokenFilterPtr create(const TokenStreamPtr& in) override;

private:
    std::shared_ptr<PinyinConfig> config_;
};

using PinyinFilterFactoryPtr = std::shared_ptr<PinyinFilterFactory>;

} // namespace doris::segment_v2::inverted_index
