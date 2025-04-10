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

#include "IKTokenizer.h"

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

    std::string& token_text = tokens_text_[buffer_index_++];
    size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    if (this->lowercase) {
        if (!token_text.empty() && static_cast<uint8_t>(token_text[0]) < 0x80) {
            std::transform(token_text.begin(), token_text.end(), token_text.begin(),
                           [](char c) { return to_lower(c); });
        }
    }
    token->setNoCopy(token_text.data(), 0, size);
    return token;
}

void IKTokenizer::reset(lucene::util::Reader* reader) {
    this->input = reader;
    this->buffer_index_ = 0;
    this->data_length_ = 0;
    this->tokens_text_.clear();

    try {
        buffer_.reserve(input->size());
        ik_segmenter_->reset(reader);
        Lexeme lexeme;
        while (ik_segmenter_->next(lexeme)) {
            tokens_text_.emplace_back(lexeme.getText());
        }
    } catch (const CLuceneError&) {
        throw;
    } catch (const std::exception& e) {
        LOG(ERROR) << "IKTokenizer encountered an uncaught exception: " << e.what();
        _CLTHROWT(CL_ERR_Runtime,
                  ("Uncaught exception in IKTokenizer: " + std::string(e.what())).c_str());
    }
    data_length_ = tokens_text_.size();
}

} // namespace doris::segment_v2
