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

#include "olap/rowset/segment_v2/inverted_index/query_v2/scorer.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class ScoreCombiner;
using ScoreCombinerPtr = std::shared_ptr<ScoreCombiner>;

class ScoreCombiner {
public:
    ScoreCombiner() = default;
    virtual ~ScoreCombiner() = default;

    virtual void update(const ScorerPtr& scorer) = 0;
    virtual void clear() = 0;
    virtual float score() const = 0;
};

class SumCombiner;
using SumCombinerPtr = std::shared_ptr<SumCombiner>;

class SumCombiner final : public ScoreCombiner {
public:
    SumCombiner() = default;
    ~SumCombiner() override = default;

    void update(const ScorerPtr& scorer) override { _score += scorer->score(); }
    void clear() override { _score = 0; }
    float score() const override { return _score; }

    SumCombinerPtr clone() { return std::make_shared<SumCombiner>(); }

private:
    float _score = 0.0F;
};

class DoNothingCombiner;
using DoNothingCombinerPtr = std::shared_ptr<DoNothingCombiner>;

class DoNothingCombiner final : public ScoreCombiner {
public:
    DoNothingCombiner() = default;
    ~DoNothingCombiner() override = default;

    void update(const ScorerPtr& scorer) override {}
    void clear() override {}
    float score() const override { return 0.0F; }

    DoNothingCombinerPtr clone() { return std::make_shared<DoNothingCombiner>(); }
};

} // namespace doris::segment_v2::inverted_index::query_v2