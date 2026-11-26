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

#include <memory>
#include <string_view>

#include "core/arena.h"
#include "storage/index/inverted/analysis/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/AnalyzeContext.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CJKSegmenter.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/CN_QuantifierSegmenter.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/IKArbitrator.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/LetterSegmenter.h"
#include "storage/index/inverted/analysis/analyzer/ik/core/SurrogatePairSegmenter.h"
namespace doris::segment_v2 {

class IKSegmenter {
public:
    // The dictionaries must be loaded: the segmenter takes the loaded Dictionary once, here.
    IKSegmenter(std::shared_ptr<Configuration> config);
    // The next lexeme of the value, or false once it is exhausted. `text` views the lexeme's bytes
    // in the window and stays valid until the next call.
    bool next(Lexeme& lexeme, std::string_view& text);
    void reset(std::string_view text);
    size_t getLastUselessCharNum();
    // The arena of the current window and the window itself.
    size_t reserved_bytes() const { return arena_.size() + context_->reserved_bytes(); }

private:
    // Runs the segmenters on the character at the cursor, in the order letter, quantifier, CJK,
    // surrogate pair.
    void analyzeCurrentChar();
    void resetSegmenters();

    Arena arena_;
    inverted_index::TextCursor input_;
    std::shared_ptr<Configuration> config_;
    const Dictionary& dictionary_;
    std::unique_ptr<AnalyzeContext> context_;
    LetterSegmenter letter_segmenter_;
    CN_QuantifierSegmenter quantifier_segmenter_;
    CJKSegmenter cjk_segmenter_;
    SurrogatePairSegmenter surrogate_pair_segmenter_;
    IKArbitrator arbitrator_;
};
} // namespace doris::segment_v2
