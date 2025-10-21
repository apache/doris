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

#include "basic_tokenizer.h"

#include <unicode/unistr.h>

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

#define IS_IN_RANGE(c, start, end) ((uint32_t)((c) - (start)) <= ((end) - (start)))

#define IS_CHINESE_CHAR(c)                                                   \
    (IS_IN_RANGE(c, 0x4E00, 0x9FFF) || IS_IN_RANGE(c, 0x3400, 0x4DBF) ||     \
     IS_IN_RANGE(c, 0x20000, 0x2A6DF) || IS_IN_RANGE(c, 0x2A700, 0x2EBEF) || \
     IS_IN_RANGE(c, 0x30000, 0x3134A))

BasicTokenizer::BasicTokenizer() {
    this->lowercase = false;
    this->ownReader = false;
}

BasicTokenizer::BasicTokenizer(bool lower_case, bool own_reader) : BasicTokenizer() {
    this->lowercase = lower_case;
    this->ownReader = own_reader;
}

Token* BasicTokenizer::next(Token* token) {
    if (_buffer_index >= _data_len) {
        return nullptr;
    }

    std::string_view& token_text = _tokens_text[_buffer_index++];
    size_t size = std::min(token_text.size(), static_cast<size_t>(LUCENE_MAX_WORD_LEN));
    token->setNoCopy(token_text.data(), 0, static_cast<int32_t>(size));
    return token;
}

void BasicTokenizer::reset(lucene::util::Reader* reader) {
    _buffer_index = 0;
    _data_len = 0;
    _tokens_text.clear();

    _buffer.resize(reader->size());
    size_t numRead = reader->readCopy(_buffer.data(), 0, static_cast<int32_t>(_buffer.size()));
    (void)numRead;
    assert(_buffer.size() == numRead);

    cut();

    _data_len = static_cast<int32_t>(_tokens_text.size());
}

void BasicTokenizer::cut() {
    auto* s = (uint8_t*)_buffer.data();
    auto length = static_cast<int32_t>(_buffer.size());

    for (int32_t i = 0; i < length;) {
        uint8_t firstByte = s[i];

        if (is_alnum(firstByte)) {
            int32_t start = i;
            while (i < length) {
                uint8_t nextByte = s[i];
                if (!is_alnum(nextByte)) {
                    break;
                }
                if (this->lowercase) {
                    s[i] = to_lower(nextByte);
                } else {
                    s[i] = nextByte;
                }
                i++;
            }
            std::string_view token((const char*)(s + start), i - start);
            _tokens_text.emplace_back(token);
        } else {
            UChar32 c = U_UNASSIGNED;
            const int32_t prev_i = i;

            U8_NEXT(s, i, length, c);
            if (c < 0) {
                continue;
            }

            if (IS_CHINESE_CHAR(c)) {
                const int32_t len = i - prev_i;
                _tokens_text.emplace_back(reinterpret_cast<const char*>(s + prev_i), len);
            }
        }
    }
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2