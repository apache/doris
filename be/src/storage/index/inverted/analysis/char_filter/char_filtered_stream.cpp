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

#include "storage/index/inverted/analysis/char_filter/char_filtered_stream.h"

#include "common/logging.h"
#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2::inverted_index {

CharFilteredStream::CharFilteredStream(std::vector<std::unique_ptr<TextCharFilter>> filters,
                                       std::unique_ptr<TextTokenStream> in)
        : _filters(std::move(filters)), _scratch(_filters.size()), _in(std::move(in)) {
    DORIS_CHECK(_in != nullptr);
}

void CharFilteredStream::reset(std::string_view text) {
    std::string_view filtered = text;
    for (size_t i = 0; i < _filters.size(); ++i) {
        release_oversized_scratch(_scratch[i]);
        filtered = _filters[i]->apply(filtered, _scratch[i]);
    }
    _in->reset(filtered);
}

size_t CharFilteredStream::reserved_bytes() const {
    size_t bytes = _in->reserved_bytes();
    for (const std::string& scratch : _scratch) {
        bytes += scratch.capacity();
    }
    return bytes;
}

std::unique_ptr<TextTokenStream> make_char_filtered_stream(
        const std::vector<CharFilterFactoryPtr>& factories, std::unique_ptr<TextTokenStream> in) {
    if (factories.empty()) {
        return in;
    }
    std::vector<std::unique_ptr<TextCharFilter>> filters;
    filters.reserve(factories.size());
    for (const CharFilterFactoryPtr& factory : factories) {
        filters.push_back(factory->create());
    }
    return std::make_unique<CharFilteredStream>(std::move(filters), std::move(in));
}

CharFilteredAnalyzer::CharFilteredAnalyzer(std::vector<CharFilterFactoryPtr> char_filters,
                                           TextAnalyzerPtr analyzer)
        : _char_filters(std::move(char_filters)), _analyzer(std::move(analyzer)) {
    DORIS_CHECK(!_char_filters.empty());
    DORIS_CHECK(_analyzer != nullptr);
}

std::unique_ptr<TextTokenStream> CharFilteredAnalyzer::create_stream() const {
    return make_char_filtered_stream(_char_filters, _analyzer->create_stream());
}

} // namespace doris::segment_v2::inverted_index
