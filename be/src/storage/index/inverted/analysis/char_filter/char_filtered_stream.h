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
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/char_filter/char_filter.h"
#include "storage/index/inverted/analysis/char_filter/char_filter_factory.h"
#include "storage/index/inverted/analysis/text_analyzer.h"
#include "storage/index/inverted/analysis/text_token_stream.h"

namespace doris::segment_v2::inverted_index {

// A stream that runs char filters over each value, in order, and hands the result to its input.
class CharFilteredStream final : public TextTokenStream {
public:
    CharFilteredStream(std::vector<std::unique_ptr<TextCharFilter>> filters,
                       std::unique_ptr<TextTokenStream> in);

    void reset(std::string_view text) override;
    bool next_batch(TokenBatch& batch) override { return _in->next_batch(batch); }
    // The filtered copies of the current value and the bytes of the input.
    size_t reserved_bytes() const override;
    void set_strict_utf8(bool strict) override { _in->set_strict_utf8(strict); }

private:
    const std::vector<std::unique_ptr<TextCharFilter>> _filters;
    // One per filter, so no filter writes over the text it reads.
    std::vector<std::string> _scratch;
    const std::unique_ptr<TextTokenStream> _in;
};

// `in` behind the char filters `factories` make, or `in` itself when there are none.
std::unique_ptr<TextTokenStream> make_char_filtered_stream(
        const std::vector<CharFilterFactoryPtr>& factories, std::unique_ptr<TextTokenStream> in);

// An analyzer behind char filters: each of its streams runs them over every value first.
class CharFilteredAnalyzer final : public TextAnalyzer {
public:
    CharFilteredAnalyzer(std::vector<CharFilterFactoryPtr> char_filters, TextAnalyzerPtr analyzer);

    std::unique_ptr<TextTokenStream> create_stream() const override;

private:
    const std::vector<CharFilterFactoryPtr> _char_filters;
    const TextAnalyzerPtr _analyzer;
};

} // namespace doris::segment_v2::inverted_index
