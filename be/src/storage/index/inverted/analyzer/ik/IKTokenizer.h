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
#include <string_view>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "storage/index/inverted/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analyzer/ik/core/IKSegmenter.h"
#include "storage/index/inverted/tokenizer/tokenizer.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

class IKTokenizer : public inverted_index::DorisTokenizer {
public:
    IKTokenizer();
    IKTokenizer(std::shared_ptr<Configuration> config, bool lowercase, bool ownReader);
    ~IKTokenizer() override = default;

    Token* next(Token* token) override;
    void reset() override;
    void reset(lucene::util::Reader* reader) override;
    std::span<const int32_t> get_source_byte_offsets(std::string_view term) const override;

private:
    struct TokenData {
        std::string text;
        int32_t start_offset;
        int32_t end_offset;
        std::vector<int32_t> source_byte_offsets;
    };

    int32_t buffer_index_ {0};
    int32_t data_length_ {0};
    std::string buffer_;
    std::vector<TokenData> tokens_;
    std::shared_ptr<Configuration> config_;
    std::unique_ptr<IKSegmenter> ik_segmenter_;
    TokenData* current_token_ {nullptr};
};

} // namespace doris::segment_v2
