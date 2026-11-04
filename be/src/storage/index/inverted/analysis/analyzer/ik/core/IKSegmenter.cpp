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

#include "storage/index/inverted/analysis/analyzer/ik/core/IKSegmenter.h"

#include <utility>

namespace doris::segment_v2 {

IKSegmenter::IKSegmenter(std::shared_ptr<Configuration> config)
        : config_(std::move(config)),
          dictionary_(*Dictionary::getSingleton()),
          context_(std::make_unique<AnalyzeContext>(arena_, config_, dictionary_)),
          quantifier_segmenter_(dictionary_),
          cjk_segmenter_(dictionary_),
          arbitrator_(arena_) {}

void IKSegmenter::analyzeCurrentChar() {
    const CharacterUtil::CharType type = context_->getCurrentCharType();
    letter_segmenter_.analyze(*context_);
    // An idle quantifier segmenter does nothing on a character that is not Chinese but release
    // its lock, which it may still hold from the end of the previous window: say a window ends
    // in 三 and the next one starts with "a", then only the release of the lock is left to do.
    if (type == CharacterUtil::CharType::CHINESE || !quantifier_segmenter_.isIdle()) {
        quantifier_segmenter_.analyze(*context_);
    } else {
        context_->unlockBuffer(CN_QuantifierSegmenter::SEGMENTER_TYPE);
    }
    cjk_segmenter_.analyze(*context_);
    // The surrogate pair segmenter only makes a lexeme of a surrogate pair character. It never
    // locks the buffer, so there is no lock to release on any other character.
    if (type == CharacterUtil::CharType::SURROGATE) {
        surrogate_pair_segmenter_.analyze(*context_);
    }
}

void IKSegmenter::resetSegmenters() {
    letter_segmenter_.reset();
    quantifier_segmenter_.reset();
    cjk_segmenter_.reset();
    surrogate_pair_segmenter_.reset();
}

bool IKSegmenter::next(Lexeme& lexeme, std::string_view& text) {
    while (!context_->getNextLexeme(lexeme)) {
        // Read data from the reader and fill the buffer
        auto available = static_cast<int32_t>(context_->fillBuffer(input_));
        if (available <= 0) {
            context_->reset();
            return false;
        } else {
            context_->initCursor();
            do {
                analyzeCurrentChar();
                // The buffer is nearly read, new characters need to be read in.
                if (context_->needRefillBuffer()) {
                    break;
                }
            } while (context_->moveCursor());
            resetSegmenters();
        }
        arbitrator_.process(*context_, config_->isUseSmart());
        context_->outputToResult();
        context_->markBufferOffset();
        arena_.clear();
    }
    text = context_->getText(lexeme);
    return true;
}

void IKSegmenter::reset(std::string_view text) {
    input_.reset(text);
    context_->reset();
    resetSegmenters();
}

size_t IKSegmenter::getLastUselessCharNum() {
    return context_->getLastUselessCharNum();
}

} // namespace doris::segment_v2
