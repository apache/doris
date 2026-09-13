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

namespace doris::segment_v2 {

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
    CharacterUtil::regularizeString(token_data.text, this->lowercase);
    size_t size = std::min(token_data.text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    set(token, std::string_view(token_data.text.data(), size));
    token->setStartOffset(token_data.start_offset);
    token->setEndOffset(token_data.end_offset);
    return token;
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

    try {
        buffer_.reserve(input->size());
        ik_segmenter_->reset(reader);
        Lexeme lexeme;
        while (ik_segmenter_->next(lexeme)) {
            tokens_.push_back({lexeme.getText(),
                               static_cast<int32_t>(lexeme.getByteBeginPosition()),
                               static_cast<int32_t>(lexeme.getByteEndPosition())});
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
