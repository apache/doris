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

#include <string>

#include "common/status.h"
#include "operator.h"
#include "pipeline/exec/scan_operator.h"
#include "runtime/group_commit_mgr.h"

namespace doris::pipeline {

class GroupCommitOperatorX;
class GroupCommitLocalState final : public ScanLocalState<GroupCommitLocalState> {
public:
    using Parent = GroupCommitOperatorX;
    ENABLE_FACTORY_CREATOR(GroupCommitLocalState);
    GroupCommitLocalState(RuntimeState* state, OperatorXBase* parent)
            : ScanLocalState(state, parent) {}
    Status init(RuntimeState* state, LocalStateInfo& info) override;
    std::shared_ptr<LoadBlockQueue> load_block_queue;
    std::vector<Dependency*> dependencies() const override {
        return {_scan_dependency.get(), _get_block_dependency.get()};
    }

private:
    friend class GroupCommitOperatorX;
    Status _process_conjuncts(RuntimeState* state) override;

    std::shared_ptr<Dependency> _get_block_dependency = nullptr;
};

class GroupCommitOperatorX final : public ScanOperatorX<GroupCommitLocalState> {
public:
    GroupCommitOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                         const DescriptorTbl& descs, int parallel_tasks);

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

protected:
    friend class GroupCommitLocalState;
    const int64_t _table_id;
};

} // namespace doris::pipeline
