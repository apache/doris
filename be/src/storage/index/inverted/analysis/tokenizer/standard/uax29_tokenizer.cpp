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

#include "storage/index/inverted/analysis/tokenizer/standard/uax29_tokenizer.h"

#include <algorithm>
#include <array>
#include <cstdint>

#include "common/logging.h"
#include "storage/index/inverted/analysis/scratch.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/tokenizer/byte_classifier.h"
#include "storage/index/inverted/analysis/tokenizer/standard/uax29_table.h"
#include "storage/index/inverted/analysis/tokenizer/utf8_validator.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {

namespace {

constexpr std::array<std::string_view, 33> STOP_WORDS = {
        "a",   "an",    "and",  "are",   "as",    "at",   "be",   "but", "by",  "for",  "if",
        "in",  "into",  "is",   "it",    "no",    "not",  "of",   "on",  "or",  "such", "that",
        "the", "their", "then", "there", "these", "they", "this", "to",  "was", "will", "with"};
constexpr size_t LONGEST_STOP_WORD = 5;

// A term of at most 7 bytes as one integer: its bytes from the low end and its length in the top
// byte, so that terms of different lengths never compare equal.
constexpr uint64_t pack_term(std::string_view term) {
    uint64_t key = static_cast<uint64_t>(term.size()) << 56;
    for (size_t i = 0; i < term.size(); ++i) {
        key |= static_cast<uint64_t>(static_cast<uint8_t>(term[i])) << (8 * i);
    }
    return key;
}

// Sorted for binary search; most tokens leave on the length test before it.
constexpr std::array<uint64_t, STOP_WORDS.size()> STOP_WORD_KEYS = [] {
    std::array<uint64_t, STOP_WORDS.size()> keys {};
    for (size_t i = 0; i < STOP_WORDS.size(); ++i) {
        keys[i] = pack_term(STOP_WORDS[i]);
    }
    std::ranges::sort(keys);
    return keys;
}();

} // namespace

std::span<const std::string_view> standard_stop_words() {
    return STOP_WORDS;
}

bool is_standard_stop_word(std::string_view term) {
    return term.size() <= LONGEST_STOP_WORD &&
           std::ranges::binary_search(STOP_WORD_KEYS, pack_term(term));
}

Uax29Tokenizer::Uax29Tokenizer(bool lowercase, bool stop_words)
        : _table(Uax29Table::instance()), _lowercase(lowercase), _stop_words(stop_words) {}

void Uax29Tokenizer::reset(std::string_view text) {
    _pos = 0;
    // Strict UTF-8 passing decides alone; the lenient rule decides the rest, the overlong forms.
    if (!doris::validate_utf8(text.data(), text.size()) && !lenient_utf8_valid(text)) {
        _text = {};
        return;
    }
    if (!_lowercase) {
        _text = text;
        return;
    }
    release_oversized_scratch(_lowered);
    // The copy only grows, so a shorter value writes over the head of it without a resize.
    if (_lowered.size() < text.size()) {
        _lowered.resize(text.size());
    }
    const char* in = text.data();
    char* out = _lowered.data();
    for (size_t i = 0; i < text.size(); ++i) {
        out[i] = ascii_lower(in[i]);
    }
    _text = {out, text.size()};
}

bool Uax29Tokenizer::next_batch(TokenBatch& batch) {
    batch.clear();
    const auto* text = reinterpret_cast<const uint8_t*>(_text.data());
    const size_t size = _text.size();
    while (_pos < size && batch.size() < kBatchTokens) {
        const size_t start = _pos;
        const Uax29Match match = _table.longest_match(
                text, size, start,
                [start](size_t cur, size_t /*length*/) { return cur - start < MAX_TOKEN_LENGTH; });
        // The window always takes the first character.
        DCHECK_GE(match.action, 0);
        _pos = match.end;
        const size_t length = match.end - start;
        if (match.action == Uax29Table::kSkip || length > MAX_TOKEN_LENGTH) {
            continue;
        }
        const std::string_view term(_text.data() + start, length);
        if (_stop_words && match.action == Uax29Table::kWord && is_standard_stop_word(term)) {
            continue;
        }
        batch.push(term, 1);
    }
    return batch.size() != 0;
}

} // namespace doris::segment_v2::inverted_index
