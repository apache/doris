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

#include "storage/index/inverted/analysis/analyzer/ik/IKTokenizer.h"

#include <unicode/utf8.h>

#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2 {

namespace {

// Folds the word rune by rune as CharacterUtil::regularizeString() does, except that an invalid
// byte sequence is kept as it is and decoding goes on after it.
void regularize_strictly(std::string& token, bool lowercase) {
    const auto length = static_cast<int32_t>(token.size());
    std::string normalized;
    normalized.reserve(token.size());
    int32_t offset = 0;
    while (offset < length) {
        const int32_t source_start = offset;
        UChar32 codepoint;
        U8_NEXT(token.c_str(), offset, length, codepoint);
        if (codepoint < 0) {
            normalized.append(token, source_start, offset - source_start);
            continue;
        }
        UChar32 regularized = CharacterUtil::regularize(codepoint, false);
        if (lowercase && regularized >= 'A' && regularized <= 'Z') {
            regularized += 'a' - 'A';
        }
        char encoded[U8_MAX_LENGTH];
        int32_t encoded_length = 0;
        U8_APPEND_UNSAFE(encoded, encoded_length, regularized);
        normalized.append(encoded, encoded_length);
    }
    token = std::move(normalized);
}

} // namespace

IKTokenizer::IKTokenizer(std::shared_ptr<Configuration> config, bool lowercase)
        : inverted_index::PerTokenTokenizer(TermLifetime::kUntilReset),
          config_(std::move(config)),
          lowercase_(lowercase) {
    ik_segmenter_ = std::make_unique<IKSegmenter>(config_);
}

bool IKTokenizer::next_token(inverted_index::TokenRef& token) {
    if (buffer_index_ >= tokens_.size()) {
        return false;
    }
    std::string& text = tokens_[buffer_index_++];
    // full-width to half-width, and lowercase
    if (strict_utf8_) {
        regularize_strictly(text, lowercase_);
    } else {
        CharacterUtil::regularizeString(text, lowercase_);
    }
    size_t size = text.size();
    if (size > static_cast<size_t>(inverted_index::MAX_WORD_LEN)) {
        size = inverted_index::utf8_prefix_at_most(text, inverted_index::MAX_WORD_LEN).first;
    }
    token = {.data = text.data(), .size = static_cast<uint32_t>(size), .position_increment = 1};
    return true;
}

void IKTokenizer::on_reset(std::string_view text) {
    buffer_index_ = 0;
    tokens_.clear();
    inverted_index::release_oversized_scratch(tokens_);
    try {
        ik_segmenter_->reset(text);
        Lexeme lexeme;
        while (ik_segmenter_->next(lexeme)) {
            tokens_.push_back(lexeme.getText());
        }
    } catch (const Exception&) {
        throw;
    } catch (const std::exception& e) {
        LOG(ERROR) << "IKTokenizer encountered an uncaught exception: " << e.what();
        throw Exception(ErrorCode::INVERTED_INDEX_ANALYZER_ERROR,
                        "Uncaught exception in IKTokenizer: {}", e.what());
    }
}

size_t IKTokenizer::reserved_bytes() const {
    size_t bytes = tokens_.capacity() * sizeof(std::string) + ik_segmenter_->reserved_bytes();
    for (const std::string& text : tokens_) {
        bytes += text.capacity();
    }
    return bytes;
}

} // namespace doris::segment_v2
