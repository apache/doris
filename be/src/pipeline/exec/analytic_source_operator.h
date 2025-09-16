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

#include <stdint.h>

#include "common/status.h"
#include "operator.h"

namespace doris {
class RuntimeState;

namespace pipeline {
#include "common/compile_check_begin.h"

class AnalyticSourceOperatorX;
class AnalyticLocalState final : public PipelineXLocalState<AnalyticSharedState> {
public:
    ENABLE_FACTORY_CREATOR(AnalyticLocalState);
    AnalyticLocalState(RuntimeState* state, OperatorXBase* parent);
    Status init(RuntimeState* state, LocalStateInfo& info) override;

private:
    friend class AnalyticSourceOperatorX;
    RuntimeProfile::Counter* _get_next_timer = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
};

class AnalyticSourceOperatorX final : public OperatorX<AnalyticLocalState> {
public:
    AnalyticSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                            const DescriptorTbl& descs);

#ifdef BE_TEST
    AnalyticSourceOperatorX() = default;
#endif
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status prepare(RuntimeState* state) override;

private:
    friend class AnalyticLocalState;
};

} // namespace pipeline
} // namespace doris
#include "common/compile_check_end.h"