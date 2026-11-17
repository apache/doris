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

#include "storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h"

#include <algorithm>

#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2::inverted_index {

void GramTokenizer::reset(std::string_view text) {
    release_oversized_scratch(_grams);
    _extractor.release_oversized();
    _grams.clear();
    _next = 0;
    if (text.empty()) {
        return;
    }
    _extractor.extract(text, &_grams);
}

bool GramTokenizer::next_batch(TokenBatch& batch) {
    batch.clear();
    const size_t end = std::min(_grams.size(), _next + kBatchTokens);
    for (; _next < end; ++_next) {
        batch.push(_grams[_next], 1);
    }
    return batch.size() != 0;
}

size_t GramTokenizer::reserved_bytes() const {
    return _grams.capacity() * sizeof(std::string_view) + _extractor.reserved_bytes();
}

} // namespace doris::segment_v2::inverted_index
