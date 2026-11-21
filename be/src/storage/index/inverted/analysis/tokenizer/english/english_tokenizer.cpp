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

#include "storage/index/inverted/analysis/tokenizer/english/english_tokenizer.h"

#include <algorithm>

#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"

namespace doris::segment_v2::inverted_index {

namespace {

// The run [start, end) as tokens of at most MAX_WORD_LEN bytes.
template <bool kLower>
void push_run(TokenBatch& batch, const char* src, int64_t start, int64_t end) {
    while (start < end) {
        const auto length = static_cast<size_t>(std::min<int64_t>(end - start, MAX_WORD_LEN));
        // Only emitted bytes need storage; separators must not allocate a row-sized copy.
        if constexpr (kLower) {
            char* lowered = batch.allocate_term(length);
            ascii_lower(src + start, lowered, length);
            batch.push({lowered, length}, 1);
        } else {
            batch.push({src + start, length}, 1);
        }
        start += length;
    }
}

} // namespace

void EnglishTokenizer::reset(std::string_view text) {
    _text = text;
    _block = 0;
    _carry = 0;
    _run_start = -1;
}

bool EnglishTokenizer::next_batch(TokenBatch& batch) {
    return _lowercase ? scan<true>(batch) : scan<false>(batch);
}

template <bool kLower>
bool EnglishTokenizer::scan(TokenBatch& batch) {
    batch.clear();
    const size_t size = _text.size();
    const char* const src = _text.data();
    const size_t blocks = (size + BYTE_CLASS_BLOCK - 1) / BYTE_CLASS_BLOCK;
    while (_block < blocks && batch.size() < kBatchTokens) {
        const size_t offset = _block * BYTE_CLASS_BLOCK;
        const ByteClasses classes =
                offset + BYTE_CLASS_BLOCK <= size
                        ? classify_block<false>(src + offset, nullptr)
                        : classify_tail<false>(src + offset, size - offset, nullptr);
        const uint64_t alnum = classes.alnum;
        // A bit where a run starts or where the byte after a run is.
        uint64_t edges = alnum ^ ((alnum << 1) | _carry);
        _carry = alnum >> 63;
        while (edges != 0) {
            const auto position = static_cast<int64_t>(offset) + __builtin_ctzll(edges);
            edges &= edges - 1;
            if (_run_start < 0) {
                _run_start = position;
            } else {
                push_run<kLower>(batch, src, _run_start, position);
                _run_start = -1;
            }
        }
        ++_block;
        // Emit complete pieces before the run ends so an uninterrupted value can yield a batch.
        if (_run_start >= 0) {
            const auto end = static_cast<int64_t>(std::min(_block * BYTE_CLASS_BLOCK, size));
            while (end - _run_start >= MAX_WORD_LEN) {
                push_run<kLower>(batch, src, _run_start, _run_start + MAX_WORD_LEN);
                _run_start += MAX_WORD_LEN;
            }
        }
    }
    // A run that reaches the end of a value of whole blocks has its end edge in no block.
    if (_block == blocks && _run_start >= 0) {
        push_run<kLower>(batch, src, _run_start, static_cast<int64_t>(size));
        _run_start = -1;
    }
    return batch.size() != 0;
}

} // namespace doris::segment_v2::inverted_index
