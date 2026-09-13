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

#include "storage/index/inverted/analyzer/ik/IKTokenizer.h"

#include <unicode/utf8.h>

namespace doris::segment_v2 {

namespace {

// Normalize the token and collect normalized-rune to source-byte boundaries in the same pass.
// NOLINTNEXTLINE(readability-function-cognitive-complexity): ICU UTF-8 macros expand to branches.
std::vector<int32_t> regularize_with_source_byte_offsets(std::string& token, bool lowercase) {
    const auto length = static_cast<int32_t>(token.size());
    std::string normalized;
    normalized.reserve(token.size());
    std::vector<int32_t> offsets {0};
    int32_t offset = 0;
    while (offset < length) {
        const int32_t source_start = offset;
        UChar32 codepoint;
        U8_NEXT(token.c_str(), offset, length, codepoint);
        if (codepoint < 0) {
            normalized.append(token, source_start, offset - source_start);
            offsets.push_back(offset);
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
        offsets.push_back(offset);
    }
    token = std::move(normalized);
    return offsets;
}

} // namespace

IKTokenizer::IKTokenizer(std::shared_ptr<Configuration> config, bool lower_case, bool own_reader) {
    this->lowercase = lower_case;
    this->ownReader = own_reader;
    config_ = config;
    ik_segmenter_ = std::make_unique<IKSegmenter>(config_);
}

Token* IKTokenizer::next(Token* token) {
    if (buffer_index_ >= data_length_) {
        return nullptr;
    }

    TokenData& token_data = tokens_[buffer_index_++];
    // full-width to half-width, and lowercase
    // TODO(ryan19929): do regularizeString in fillBuffer.
    if (source_byte_offsets_enabled_) {
        current_source_byte_offsets_ =
                regularize_with_source_byte_offsets(token_data.text, this->lowercase);
    } else {
        CharacterUtil::regularizeString(token_data.text, this->lowercase);
        current_source_byte_offsets_.clear();
    }
    current_token_ = &token_data;
    size_t size = std::min(token_data.text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    set(token, std::string_view(token_data.text.data(), size));
    token->setStartOffset(token_data.start_offset);
    token->setEndOffset(token_data.end_offset);
    return token;
}

std::span<const int32_t> IKTokenizer::get_source_byte_offsets() const {
    return current_token_ == nullptr ? std::span<const int32_t> {}
                                     : std::span<const int32_t> {current_source_byte_offsets_};
}

void IKTokenizer::reset() {
    if (_in_pending == nullptr) {
        return;
    }
    inverted_index::DorisTokenizer::reset();
    _in_pending.reset();
    reset(_in.get());
}

void IKTokenizer::reset(lucene::util::Reader* reader) {
    _in_pending.reset();
    this->input = reader;
    this->buffer_index_ = 0;
    this->data_length_ = 0;
    this->tokens_.clear();
    this->current_token_ = nullptr;
    this->current_source_byte_offsets_.clear();

    try {
        buffer_.reserve(input->size());
        ik_segmenter_->reset(reader);
        Lexeme lexeme;
        while (ik_segmenter_->next(lexeme)) {
            TokenData token_data {
                    .text = lexeme.getText(),
                    .start_offset = static_cast<int32_t>(lexeme.getByteBeginPosition()),
                    .end_offset = static_cast<int32_t>(lexeme.getByteEndPosition())};
            tokens_.push_back(std::move(token_data));
        }
    } catch (const CLuceneError&) {
        throw;
    } catch (const std::exception& e) {
        LOG(ERROR) << "IKTokenizer encountered an uncaught exception: " << e.what();
        _CLTHROWT(CL_ERR_Runtime,
                  ("Uncaught exception in IKTokenizer: " + std::string(e.what())).c_str());
    }
    data_length_ = static_cast<int32_t>(tokens_.size());
}

} // namespace doris::segment_v2
