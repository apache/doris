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

#pragma once

#include "ngram_tokenizer.h"

namespace doris::segment_v2::inverted_index {
#include "common/compile_check_begin.h"

class EdgeNGramTokenizer : public NGramTokenizer {
public:
    EdgeNGramTokenizer() : NGramTokenizer(DEFAULT_MAX_GRAM_SIZE, DEFAULT_MIN_GRAM_SIZE, true) {}

    EdgeNGramTokenizer(int32_t min_gram, int32_t max_gram)
            : NGramTokenizer(min_gram, max_gram, true) {}

    ~EdgeNGramTokenizer() override = default;

    static constexpr int32_t DEFAULT_MAX_GRAM_SIZE = 1;
    static constexpr int32_t DEFAULT_MIN_GRAM_SIZE = 1;
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2::inverted_index