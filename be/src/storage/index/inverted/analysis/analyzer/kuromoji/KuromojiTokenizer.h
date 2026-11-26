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

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/kuromoji/KuromojiMode.h"
#include "storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi.h"
#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2 {

// Japanese tokenizer. Performs Viterbi morphological segmentation using the dictionary.
class KuromojiTokenizer final : public inverted_index::PerTokenTokenizer {
public:
    explicit KuromojiTokenizer(KuromojiMode mode = KuromojiMode::Search, bool lowercase = true,
                               const inverted_index::kuromoji::KuromojiDictionary* dict = nullptr);

    // The terms of the current value.
    size_t reserved_bytes() const override;

protected:
    void on_reset(std::string_view text) override;
    bool next_token(inverted_index::TokenRef& token) override;

private:
    KuromojiMode mode_;
    const bool lowercase_;
    const inverted_index::kuromoji::KuromojiDictionary* dict_ {nullptr};
    size_t buffer_index_ {0};
    // Backing storage for emitted terms; must outlive tokens.
    std::vector<std::string> tokens_text_;
};

} // namespace doris::segment_v2
