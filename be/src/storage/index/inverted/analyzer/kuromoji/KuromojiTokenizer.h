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
#include <vector>

#include "CLucene.h"
#include "CLucene/analysis/AnalysisHeader.h"
#include "storage/index/inverted/analyzer/kuromoji/KuromojiMode.h"
#include "storage/index/inverted/analyzer/kuromoji/kuromoji_viterbi.h"

using namespace lucene::analysis;

namespace doris::segment_v2 {

// Japanese tokenizer. With a dictionary it does Viterbi morphological
// segmentation (kuromoji::KuromojiViterbi); without one it falls back to a
// per-codepoint CJK split.
class KuromojiTokenizer : public Tokenizer {
public:
    explicit KuromojiTokenizer(KuromojiMode mode = KuromojiMode::Search, bool lowercase = true,
                               bool own_reader = false,
                               const kuromoji::KuromojiDictionary* dict = nullptr);
    ~KuromojiTokenizer() override = default;

    Token* next(Token* token) override;
    void reset(lucene::util::Reader* reader) override;

private:
    KuromojiMode mode_;
    const kuromoji::KuromojiDictionary* dict_ {nullptr};
    int32_t buffer_index_ {0};
    int32_t data_length_ {0};
    // Backing storage for emitted terms; must outlive tokens (setNoCopy contract).
    std::vector<std::string> tokens_text_;
};

} // namespace doris::segment_v2
