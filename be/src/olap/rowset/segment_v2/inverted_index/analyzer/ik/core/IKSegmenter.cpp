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

#include "IKSegmenter.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

IKSegmenter::IKSegmenter(std::shared_ptr<Configuration> config)
        : arena_(),
          config_(config),
          context_(std::make_unique<AnalyzeContext>(arena_, config_)),
          segmenters_(loadSegmenters()),
          arbitrator_(IKArbitrator(arena_)) {}

std::vector<std::unique_ptr<ISegmenter>> IKSegmenter::loadSegmenters() {
    std::vector<std::unique_ptr<ISegmenter>> segmenters;
    segmenters.push_back(std::make_unique<LetterSegmenter>());
    segmenters.push_back(std::make_unique<CN_QuantifierSegmenter>());
    segmenters.push_back(std::make_unique<CJKSegmenter>());
    segmenters.push_back(std::make_unique<SurrogatePairSegmenter>());
    return segmenters;
}

bool IKSegmenter::next(Lexeme& lexeme) {
    while (!context_->getNextLexeme(lexeme)) {
        // Read data from the reader and fill the buffer
        auto available = static_cast<int32_t>(context_->fillBuffer(input_));
        if (available <= 0) {
            context_->reset();
            return false;
        } else {
            context_->initCursor();
            do {
                for (const auto& segmenter : segmenters_) {
                    segmenter->analyze(*context_);
                }
                // The buffer is nearly read, new characters need to be read in.
                if (context_->needRefillBuffer()) {
                    break;
                }
            } while (context_->moveCursor());
            for (const auto& segmenter : segmenters_) {
                segmenter->reset();
            }
        }
        arbitrator_.process(*context_, config_->isUseSmart());
        context_->outputToResult();
        context_->markBufferOffset();
        arena_.clear();
    }
    return true;
}

void IKSegmenter::reset(lucene::util::Reader* newInput) {
    input_ = newInput;
    context_->reset();
    for (const auto& segmenter : segmenters_) {
        segmenter->reset();
    }
}

size_t IKSegmenter::getLastUselessCharNum() {
    return context_->getLastUselessCharNum();
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
