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

#include <iostream>
#include <memory>
#include <vector>

#include "core/arena.h"
#include "storage/index/inverted/analyzer/ik/cfg/Configuration.h"
#include "storage/index/inverted/analyzer/ik/core/AnalyzeContext.h"
#include "storage/index/inverted/analyzer/ik/core/CJKSegmenter.h"
#include "storage/index/inverted/analyzer/ik/core/CN_QuantifierSegmenter.h"
#include "storage/index/inverted/analyzer/ik/core/IKArbitrator.h"
#include "storage/index/inverted/analyzer/ik/core/ISegmenter.h"
#include "storage/index/inverted/analyzer/ik/core/LetterSegmenter.h"
#include "storage/index/inverted/analyzer/ik/core/SurrogatePairSegmenter.h"
namespace doris::segment_v2 {

class IKSegmenter {
public:
    IKSegmenter(std::shared_ptr<Configuration> config);
    bool next(Lexeme& lexeme);
    void reset(lucene::util::Reader* newInput);
    size_t getLastUselessCharNum();

private:
    std::vector<std::unique_ptr<ISegmenter>> loadSegmenters();
    vectorized::Arena arena_;
    lucene::util::Reader* input_;
    std::shared_ptr<Configuration> config_;
    std::unique_ptr<AnalyzeContext> context_;
    std::vector<std::unique_ptr<ISegmenter>> segmenters_;
    IKArbitrator arbitrator_;
};
} // namespace doris::segment_v2
