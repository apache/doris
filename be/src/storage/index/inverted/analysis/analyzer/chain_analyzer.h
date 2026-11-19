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
#include <vector>

#include "storage/index/inverted/analysis/char_filter/char_filter_factory.h"
#include "storage/index/inverted/analysis/text_analyzer.h"
#include "storage/index/inverted/analysis/token_filter/token_filter_factory.h"
#include "storage/index/inverted/analysis/tokenizer/tokenizer_factory.h"

namespace doris::segment_v2::inverted_index {

// An analyzer made of component factories, applied in this order: the char filters, the tokenizer,
// the token filters. Each stream gets components of its own, so the analyzer is shared freely.
class ChainAnalyzer final : public TextAnalyzer {
public:
    ChainAnalyzer(std::vector<CharFilterFactoryPtr> char_filters, TokenizerFactoryPtr tokenizer,
                  std::vector<TokenFilterFactoryPtr> token_filters);

    std::unique_ptr<TextTokenStream> create_stream() const override;

private:
    const std::vector<CharFilterFactoryPtr> _char_filters;
    const TokenizerFactoryPtr _tokenizer;
    const std::vector<TokenFilterFactoryPtr> _token_filters;
};

} // namespace doris::segment_v2::inverted_index
