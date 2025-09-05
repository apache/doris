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

#include "olap/rowset/segment_v2/inverted_index/query_v2/doc_set.h"

namespace doris::segment_v2::inverted_index::query_v2 {

class Scorer : public DocSet {
public:
    Scorer() = default;
    ~Scorer() override = default;

    virtual float score() = 0;
};
using ScorerPtr = std::shared_ptr<Scorer>;

class EmptyScorer : public Scorer {
public:
    EmptyScorer() = default;
    ~EmptyScorer() override = default;

    uint32_t advance() override { return TERMINATED; }
    uint32_t seek(uint32_t) override { return TERMINATED; }
    uint32_t doc() const override { return TERMINATED; }
    uint32_t size_hint() const override { return 0; }

    float score() override { return 0.0F; }
};

} // namespace doris::segment_v2::inverted_index::query_v2