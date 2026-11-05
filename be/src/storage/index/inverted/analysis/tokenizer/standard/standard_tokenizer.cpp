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

#include "storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer.h"

#include <memory>
#include <string>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/token_batch.h"
#include "storage/index/inverted/analysis/tokenizer/standard/uax29_table.h"
#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {

StandardTokenizer::StandardTokenizer()
        : _table(Uax29Table::instance()), _scanner(std::make_unique<StandardTokenizerImpl>()) {}

void StandardTokenizer::set_max_token_length(int32_t length) {
    if (length < 1) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "max_token_length must be greater than zero");
    } else if (length > MAX_TOKEN_LENGTH_LIMIT) {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "max_token_length may not exceed " + std::to_string(MAX_TOKEN_LENGTH_LIMIT));
    }
    if (length != _max_token_length) {
        _max_token_length = length;
        _scanner->set_buffer_size(length);
    }
}

void StandardTokenizer::reset(std::string_view text) {
    _text = text;
    _valid = doris::validate_utf8(text.data(), text.size());
    _pos = 0;
    _exhausted = false;
    if (!_valid) {
        _scanner->yyreset(text);
    }
}

bool StandardTokenizer::next_batch(TokenBatch& batch) {
    return _valid ? next_valid_batch(batch) : next_scanner_batch(batch);
}

bool StandardTokenizer::next_valid_batch(TokenBatch& batch) {
    batch.clear();
    const auto* text = reinterpret_cast<const uint8_t*>(_text.data());
    const size_t size = _text.size();
    const auto window = static_cast<size_t>(_max_token_length);
    while (_pos < size && batch.size() < kBatchTokens) {
        const size_t start = _pos;
        const Uax29Match match =
                _table.longest_match(text, size, start, [start, window](size_t cur, size_t length) {
                    return cur + length - start <= window;
                });
        if (match.action < 0) {
            // The character at `start` is longer than max_token_length, so the scanner's buffer
            // could not take it, and the scanner ended the value there.
            _pos = size;
            break;
        }
        _pos = match.end;
        if (match.action != Uax29Table::kSkip) {
            batch.push({_text.data() + start, match.end - start}, 1);
        }
    }
    return batch.size() != 0;
}

bool StandardTokenizer::next_scanner_batch(TokenBatch& batch) {
    batch.clear();
    while (!_exhausted && batch.size() < kBatchTokens) {
        if (_scanner->get_next_token() == StandardTokenizerImpl::TokenType::EOF_TOKEN) {
            _exhausted = true;
            break;
        }
        // The buffer of max_token_length bytes holds the whole token.
        DCHECK_LE(_scanner->yylength(), _max_token_length);
        // The term points into the scanner's buffer, which its next refill moves.
        batch.push_copy(_scanner->get_text(), 1);
    }
    return batch.size() != 0;
}

} // namespace doris::segment_v2::inverted_index
