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

#include "storage/index/inverted/tokenizer/ngram/gram_tokenizer.h"

#include "common/logging.h"

namespace doris::segment_v2::inverted_index {

void GramTokenizer::reset() {
    DorisTokenizer::reset();
    _grams.clear();
    _next = 0;
    _char_buffer = nullptr;
    _char_length = 0;
    if (_in == nullptr || _in->size() == 0) {
        return;
    }
    // Same as NGramTokenizer::reset(): read the whole input in one go
    // (ngram_tokenizer.cpp:83-94).
    _char_length = _in->read(reinterpret_cast<const void**>(&_char_buffer), 0,
                             static_cast<int32_t>(_in->size()));
    // The read must cover everything: one byte short means a handful of grams fewer, which
    // breaks the invariant "the grams stored in the index are a superset of the grams the query
    // side needs" outright (rows go missing with no way to notice).
    DCHECK_EQ(_char_length, static_cast<int32_t>(_in->size()));
    if (_char_length <= 0 || _char_buffer == nullptr) {
        return;
    }
    _extractor.extract(std::string_view(_char_buffer, _char_length), &_grams);
}

Token* GramTokenizer::next(Token* token) {
    if (_next >= _grams.size()) {
        return nullptr;
    }
    set(token, _grams[_next++], 1);
    return token;
}

} // namespace doris::segment_v2::inverted_index
