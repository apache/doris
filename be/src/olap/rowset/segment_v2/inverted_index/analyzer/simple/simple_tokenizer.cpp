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

#include "simple_tokenizer.h"

#include <unicode/unistr.h>

namespace doris::segment_v2 {

SimpleTokenizer::SimpleTokenizer() {
    Tokenizer::lowercase = false;
    Tokenizer::ownReader = false;
}

SimpleTokenizer::SimpleTokenizer(bool lower_case, bool own_reader) : SimpleTokenizer() {
    Tokenizer::lowercase = lower_case;
    Tokenizer::ownReader = own_reader;
}

Token* SimpleTokenizer::next(Token* token) {
    if (_buffer_index >= _data_len) {
        return nullptr;
    }

    std::string_view& token_text = _tokens_text[_buffer_index++];
    size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    if (Tokenizer::lowercase) {
        if (!token_text.empty() && static_cast<uint8_t>(token_text[0]) < 0x80) {
            std::transform(token_text.begin(), token_text.end(),
                           const_cast<char*>(token_text.data()),
                           [](char c) { return to_lower(c); });
        }
    }
    token->setNoCopy(token_text.data(), 0, size);
    return token;
}

void SimpleTokenizer::reset(lucene::util::Reader* reader) {
    _buffer_index = 0;
    _data_len = 0;
    _tokens_text.clear();

    _buffer.resize(reader->size());
    int32_t numRead = reader->readCopy(_buffer.data(), 0, _buffer.size());
    (void)numRead;
    assert(_buffer.size() == numRead);

    cut();

    _data_len = _tokens_text.size();
}

void SimpleTokenizer::cut() {
    auto* s = (uint8_t*)_buffer.data();
    int32_t length = _buffer.size();

    for (int32_t i = 0; i < length;) {
        uint8_t firstByte = s[i];

        if (is_alnum(firstByte)) {
            int32_t start = i;
            while (i < length) {
                uint8_t nextByte = s[i];
                if (!is_alnum(nextByte)) {
                    break;
                }
                s[i] = to_lower(nextByte);
                i++;
            }
            std::string_view token((const char*)(s + start), i - start);
            _tokens_text.emplace_back(std::move(token));
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;

            U8_NEXT(s, i, length, c);

            if (c == U_UNASSIGNED) {
                _CLTHROWT(CL_ERR_Runtime, "invalid UTF-8 sequence");
            }

            const auto category = static_cast<UCharCategory>(u_charType(c));
            if (category == U_OTHER_LETTER) {
                const int32_t len = i - prev_i;
                _tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
            }
        }
    }
}

} // namespace doris::segment_v2