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
#include "pipeline/common/data_gen_functions/vdata_gen_function_inf.h"
#include "pipeline/exec/operator.h"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

class DataGenSourceOperatorX;
class DataGenLocalState final : public PipelineXLocalState<> {
public:
    ENABLE_FACTORY_CREATOR(DataGenLocalState);

    DataGenLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<>(state, parent) {}
    ~DataGenLocalState() = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

private:
    friend class DataGenSourceOperatorX;
    std::shared_ptr<VDataGenFunctionInf> _table_func;
};

class DataGenSourceOperatorX final : public OperatorX<DataGenLocalState> {
public:
    DataGenSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                           const DescriptorTbl& descs);

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    Status open(RuntimeState* state) override;
    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    [[nodiscard]] bool is_source() const override { return true; }

private:
    friend class DataGenLocalState;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of tuples generated
    const TupleDescriptor* _tuple_desc = nullptr;

    std::vector<TRuntimeFilterDesc> _runtime_filter_descs;
};

} // namespace doris::pipeline