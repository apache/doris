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

#include <cstring>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/index/inverted/analysis/common/rune.h"
#include "storage/index/inverted/analysis/scratch.h"

namespace doris::segment_v2 {

namespace {

// Folds the word rune by rune as CharacterUtil::regularizeInPlace() does, except that an invalid
// byte sequence is kept as it is and decoding goes on after it; returns the new size. In place: a
// valid rune is written back in at most the bytes it was read from, so with `lowercase` ＡB
// (EF BC A1 42) becomes ab in its first two bytes, and an invalid sequence is copied as it is.
size_t regularize_strictly(char* data, size_t size, bool lowercase) {
    const auto length = static_cast<int32_t>(size);
    int32_t out = 0;
    int32_t offset = 0;
    while (offset < length) {
        const int32_t source_start = offset;
        UChar32 codepoint;
        U8_NEXT(data, offset, length, codepoint);
        if (codepoint < 0) {
            std::memmove(data + out, data + source_start, offset - source_start);
            out += offset - source_start;
            continue;
        }
        UChar32 regularized = CharacterUtil::regularize(codepoint, false);
        if (lowercase && regularized >= 'A' && regularized <= 'Z') {
            regularized += 'a' - 'A';
        }
        U8_APPEND_UNSAFE(data, out, regularized);
    }
    return static_cast<size_t>(out);
}

} // namespace

IKTokenizer::IKTokenizer(std::shared_ptr<Configuration> config, bool lowercase)
        : inverted_index::PerTokenTokenizer(TermLifetime::kUntilReset),
          config_(std::move(config)),
          lowercase_(lowercase) {
    ik_segmenter_ = std::make_unique<IKSegmenter>(config_);
}

bool IKTokenizer::next_token(inverted_index::TokenRef& token) {
    if (next_word_ >= words_.size()) {
        return false;
    }
    const Word& word = words_[next_word_++];
    char* text = words_bytes_.data() + word.offset;
    // full-width to half-width, and lowercase
    size_t size = strict_utf8_ ? regularize_strictly(text, word.size, lowercase_)
                               : CharacterUtil::regularizeInPlace(text, word.size, lowercase_);
    if (size > static_cast<size_t>(inverted_index::MAX_WORD_LEN)) {
        size = inverted_index::utf8_prefix_at_most({text, size}, inverted_index::MAX_WORD_LEN)
                       .first;
    }
    token = {.data = text, .size = static_cast<uint32_t>(size), .position_increment = 1};
    return true;
}

void IKTokenizer::on_reset(std::string_view text) {
    next_word_ = 0;
    words_.clear();
    words_bytes_.clear();
    inverted_index::release_oversized_scratch(words_);
    inverted_index::release_oversized_scratch(words_bytes_);
    try {
        ik_segmenter_->reset(text);
        Lexeme lexeme;
        std::string_view word;
        while (ik_segmenter_->next(lexeme, word)) {
            Word& entry = words_.emplace_back();
            entry.offset = words_bytes_.size();
            entry.size = word.size();
            words_bytes_.append(word);
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
    return words_.capacity() * sizeof(Word) + words_bytes_.capacity() +
           ik_segmenter_->reserved_bytes();
}

} // namespace doris::segment_v2
