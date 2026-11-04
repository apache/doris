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

#include "storage/index/inverted/analysis/analyzer/chain_analyzer.h"

#include "common/logging.h"
#include "storage/index/inverted/analysis/char_filter/char_filtered_stream.h"

namespace doris::segment_v2::inverted_index {

ChainAnalyzer::ChainAnalyzer(std::vector<CharFilterFactoryPtr> char_filters,
                             TokenizerFactoryPtr tokenizer,
                             std::vector<TokenFilterFactoryPtr> token_filters)
        : _char_filters(std::move(char_filters)),
          _tokenizer(std::move(tokenizer)),
          _token_filters(std::move(token_filters)) {
    DORIS_CHECK(_tokenizer != nullptr);
}

std::unique_ptr<TextTokenStream> ChainAnalyzer::create_stream() const {
    std::unique_ptr<TextTokenStream> stream = _tokenizer->create();
    for (const TokenFilterFactoryPtr& filter : _token_filters) {
        stream = filter->create(std::move(stream));
    }
    return make_char_filtered_stream(_char_filters, std::move(stream));
}

} // namespace doris::segment_v2::inverted_index
