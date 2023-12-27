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

#include <stddef.h>
#include <stdint.h>

#include <iosfwd>
#include <memory>
#include <vector>

#include "common/status.h"
#include "exec/exec_node.h"
#include "util/runtime_profile.h"
#include "vec/common/sort/sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/core/field.h"

namespace doris {
class DescriptorTbl;
class ObjectPool;
class RuntimeState;
class TPlanNode;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
// Node that implements a full sort of its input with a fixed memory budget
// In open() the input Block to VSortNode will sort firstly, using the expressions specified in _sort_exec_exprs.
// In get_next(), VSortNode do the merge sort to gather data to a new block

// support spill to disk in the future
class VSortNode final : public doris::ExecNode {
public:
    VSortNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~VSortNode() override = default;

    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    Status prepare(RuntimeState* state) override;

    Status alloc_resource(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    Status reset(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    void release_resource(RuntimeState* state) override;

    Status pull(RuntimeState* state, vectorized::Block* output_block, bool* eos) override;

    Status sink(RuntimeState* state, vectorized::Block* input_block, bool eos) override;

protected:
    void debug_string(int indentation_level, std::stringstream* out) const override;

private:
    // Number of rows to skip.
    int64_t _offset;

    // Expressions and parameters used for build _sort_description
    VSortExecExprs _vsort_exec_exprs;
    std::vector<bool> _is_asc_order;
    std::vector<bool> _nulls_first;

    RuntimeProfile::Counter* _memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _sort_blocks_memory_usage = nullptr;

    bool _use_topn_opt = false;
    // topn top value
    Field old_top {Field::Types::Null};

    bool _reuse_mem;

    std::unique_ptr<Sorter> _sorter;

    RuntimeProfile::Counter* _child_get_next_timer = nullptr;
    RuntimeProfile::Counter* _sink_timer = nullptr;
    RuntimeProfile::Counter* _get_next_timer = nullptr;

    static constexpr size_t ACCUMULATED_PARTIAL_SORT_THRESHOLD = 256;
};

} // namespace doris::vectorized
