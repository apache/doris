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

#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2::inverted_index {

namespace {

// Copies tokens into `batch` until it is full or `next_token` returns false, which latches
// `exhausted`.
template <typename NextToken>
bool fill_batch(TokenBatch& batch, bool& exhausted, NextToken&& next_token) {
    batch.clear();
    TokenRef token;
    while (!exhausted && batch.size() < TextTokenStream::kBatchTokens) {
        if (next_token(token)) {
            batch.push_copy(token.term(), token.position_increment);
        } else {
            exhausted = true;
        }
    }
    return batch.size() != 0;
}

} // namespace

void PerTokenTokenizer::reset(std::string_view text) {
    // Exhausted until on_reset() returns, so a stream whose on_reset() threw makes no token.
    _exhausted = true;
    on_reset(text);
    _exhausted = false;
}

bool PerTokenTokenizer::next_batch(TokenBatch& batch) {
    return fill_batch(batch, _exhausted, [this](TokenRef& token) { return next_token(token); });
}

void PerTokenFilter::reset(std::string_view text) {
    _exhausted = true;
    TextTokenFilter::reset(text);
    // A half-read value leaves tokens in the input batch; none of them may come out again.
    _input.clear();
    _input.release_oversized();
    _cursor = 0;
    on_reset();
    _exhausted = false;
}

bool PerTokenFilter::next_batch(TokenBatch& batch) {
    return fill_batch(batch, _exhausted, [this](TokenRef& token) { return next_token(token); });
}

size_t PerTokenFilter::reserved_bytes() const {
    return _input.capacity_bytes() + TextTokenFilter::reserved_bytes();
}

bool PerTokenFilter::input_token(TokenRef& token) {
    if (_cursor == _input.size()) {
        _cursor = 0;
        // An exhausted input keeps returning false with an empty batch.
        if (!_in->next_batch(_input)) {
            return false;
        }
    }
    token = _input.tokens()[_cursor++];
    return true;
}

} // namespace doris::segment_v2::inverted_index
