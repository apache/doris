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

#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"

namespace doris::segment_v2::inverted_index {

namespace {

// The run [start, end) as tokens of at most MAX_WORD_LEN bytes.
void push_run(TokenBatch& batch, const char* base, int64_t start, int64_t end) {
    while (end - start > MAX_WORD_LEN) {
        batch.push({base + start, static_cast<size_t>(MAX_WORD_LEN)}, 1);
        start += MAX_WORD_LEN;
    }
    batch.push({base + start, static_cast<size_t>(end - start)}, 1);
}

} // namespace

void EnglishTokenizer::reset(std::string_view text) {
    _text = text;
    _block = 0;
    _carry = 0;
    _run_start = -1;
    if (_lowercase) {
        release_oversized_scratch(_lowered);
        // The copy only grows, so a shorter value writes over the head of it without a resize.
        if (_lowered.size() < text.size()) {
            _lowered.resize(text.size());
        }
    }
}

bool EnglishTokenizer::next_batch(TokenBatch& batch) {
    return _lowercase ? scan<true>(batch) : scan<false>(batch);
}

template <bool kLower>
bool EnglishTokenizer::scan(TokenBatch& batch) {
    batch.clear();
    const size_t size = _text.size();
    const char* const src = _text.data();
    char* const lowered = kLower ? _lowered.data() : nullptr;
    const char* const base = kLower ? lowered : src;
    const size_t blocks = (size + BYTE_CLASS_BLOCK - 1) / BYTE_CLASS_BLOCK;
    while (_block < blocks && batch.size() < kBatchTokens) {
        const size_t offset = _block * BYTE_CLASS_BLOCK;
        char* const block_lowered = kLower ? lowered + offset : nullptr;
        const ByteClasses classes =
                offset + BYTE_CLASS_BLOCK <= size
                        ? classify_block<kLower>(src + offset, block_lowered)
                        : classify_tail<kLower>(src + offset, size - offset, block_lowered);
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
                push_run(batch, base, _run_start, position);
                _run_start = -1;
            }
        }
        ++_block;
    }
    // A run that reaches the end of a value of whole blocks has its end edge in no block.
    if (_block == blocks && _run_start >= 0) {
        push_run(batch, base, _run_start, static_cast<int64_t>(size));
        _run_start = -1;
    }
    return batch.size() != 0;
}

} // namespace doris::segment_v2::inverted_index
