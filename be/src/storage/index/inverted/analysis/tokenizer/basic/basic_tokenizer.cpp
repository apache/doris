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

#include "storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer.h"

#include <unicode/uchar.h>
#include <unicode/utf8.h>

#include <algorithm>

#include "common/compiler_util.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"

namespace doris::segment_v2::inverted_index {

namespace {

bool in_range(UChar32 c, uint32_t start, uint32_t end) {
    return static_cast<uint32_t>(c) - start <= end - start;
}

// The five ranges of Chinese characters the basic tokenizer makes a token of each.
bool is_chinese_char(UChar32 c) {
    return in_range(c, 0x4E00, 0x9FFF) || in_range(c, 0x3400, 0x4DBF) ||
           in_range(c, 0x20000, 0x2A6DF) || in_range(c, 0x2A700, 0x2EBEF) ||
           in_range(c, 0x30000, 0x3134A);
}

// The character that starts at `position` of the value, as a token when it is in one of the
// Chinese ranges. Decoded from the value: the next block of the lowered copy is not written yet.
// Called once per lead byte, so every character of Chinese or Japanese text; out of line, the
// call made those corpora 5-8% slower.
ALWAYS_INLINE inline void push_chinese_char(TokenBatch& batch, const char* src, int64_t position,
                                            size_t size) {
    auto next = static_cast<int32_t>(position);
    UChar32 c = U_UNASSIGNED;
    U8_NEXT(src, next, static_cast<int32_t>(size), c);
    if (c >= 0 && is_chinese_char(c)) {
        batch.push({src + position, static_cast<size_t>(next - position)}, 1);
    }
}

// The run [start, end), cut to MAX_WORD_LEN bytes.
void push_run(TokenBatch& batch, const char* base, int64_t start, int64_t end) {
    batch.push({base + start, static_cast<size_t>(std::min<int64_t>(end - start, MAX_WORD_LEN))},
               1);
}

} // namespace

void BasicTokenizer::initialize(const std::string& extra_chars) {
    DORIS_CHECK(!_lowercase || extra_chars.empty());
    _extra_char_set.fill(false);
    _has_extra_chars = !extra_chars.empty();

    for (uint8_t c : extra_chars) {
        if (c < 128) {
            _extra_char_set[c] = true;
        }
    }
}

void BasicTokenizer::reset(std::string_view text) {
    _text = text;
    if (_has_extra_chars) {
        _next = 0;
        _tokens_text.clear();
        release_oversized_scratch(_tokens_text);
        cut_with_extra_chars(text);
        return;
    }
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

bool BasicTokenizer::next_batch(TokenBatch& batch) {
    if (_has_extra_chars) {
        return hand_out(batch);
    }
    return _lowercase ? scan<true>(batch) : scan<false>(batch);
}

template <bool kLower>
bool BasicTokenizer::scan(TokenBatch& batch) {
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
        const uint64_t edges = alnum ^ ((alnum << 1) | _carry);
        _carry = alnum >> 63;
        // In order: in "abc中" the run ends at the byte that starts 中, and "abc" comes first.
        uint64_t events = edges | classes.lead;
        while (events != 0) {
            const int bit = __builtin_ctzll(events);
            const uint64_t one = 1ULL << bit;
            events &= events - 1;
            const auto position = static_cast<int64_t>(offset) + bit;
            if ((edges & one) != 0) {
                if (_run_start < 0) {
                    _run_start = position;
                } else {
                    push_run(batch, base, _run_start, position);
                    _run_start = -1;
                }
            }
            if ((classes.lead & one) != 0) {
                push_chinese_char(batch, src, position, size);
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

bool BasicTokenizer::hand_out(TokenBatch& batch) {
    batch.clear();
    while (_next < _tokens_text.size() && batch.size() < kBatchTokens) {
        const std::string_view token = _tokens_text[_next++];
        batch.push(token.substr(0, MAX_WORD_LEN), 1);
    }
    return batch.size() != 0;
}

void BasicTokenizer::cut_with_extra_chars(std::string_view text) {
    const auto* s = reinterpret_cast<const uint8_t*>(text.data());
    const auto length = static_cast<int32_t>(text.size());

    for (int32_t i = 0; i < length;) {
        uint8_t firstByte = s[i];

        if (is_ascii_alnum(firstByte)) {
            int32_t start = i;
            while (i < length && is_ascii_alnum(s[i])) {
                i++;
            }
            _tokens_text.emplace_back(reinterpret_cast<const char*>(s + start), i - start);
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;

            U8_NEXT(s, i, length, c);
            if (c < 0) {
                continue;
            }

            if (is_chinese_char(c)) {
                const int32_t len = i - prev_i;
                _tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
            }

            if (c < 128 && _extra_char_set[c]) {
                const int32_t len = i - prev_i;
                _tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
            }
        }
    }
}

} // namespace doris::segment_v2::inverted_index
