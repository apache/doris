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

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "storage/index/inverted/analysis/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/IKSegmenter.h"
#include "storage/index/inverted/analysis/per_token_stream.h"

namespace doris::segment_v2 {

// The words IK segments the value into, full-width forms folded to half-width and, with
// `lowercase`, ASCII lowercased; at most MAX_WORD_LEN bytes each, cut on a rune boundary.
//
// reset() segments the whole value, so that a value IK cannot decode throws there, and keeps the
// words back to back in one buffer reused from value to value; next_token() folds each word in
// place, which never makes it longer.
class IKTokenizer final : public inverted_index::PerTokenTokenizer {
public:
    // The dictionaries must be loaded.
    IKTokenizer(std::shared_ptr<Configuration> config, bool lowercase);

    // Strict: folds rune by rune and keeps an invalid byte sequence as it is.
    void set_strict_utf8(bool strict) override { strict_utf8_ = strict; }

    // The words of the current value, and the segmenter's window.
    size_t reserved_bytes() const override;

protected:
    void on_reset(std::string_view text) override;
    bool next_token(inverted_index::TokenRef& token) override;

private:
    // A word of the current value: words_bytes_[offset, offset + size).
    struct Word {
        size_t offset;
        size_t size;
    };

    size_t next_word_ {0};
    std::string words_bytes_;
    std::vector<Word> words_;
    std::shared_ptr<Configuration> config_;
    std::unique_ptr<IKSegmenter> ik_segmenter_;
    const bool lowercase_;
    bool strict_utf8_ {false};
};

} // namespace doris::segment_v2
