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

#include "common/be_mock_util.h"
#include "common/status.h"
#include "exec/operator/operator.h"

namespace doris {
class RuntimeState;

#include "common/compile_check_begin.h"
class AggSourceOperatorX;

class AggLocalState MOCK_REMOVE(final) : public PipelineXLocalState<AggSharedState> {
public:
    using Base = PipelineXLocalState<AggSharedState>;
    ENABLE_FACTORY_CREATOR(AggLocalState);
    AggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~AggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    void make_nullable_output_key(Block* block);
    Status merge_with_serialized_key_helper(Block* block);
    void do_agg_limit(Block* block, bool* eos);

protected:
    friend class AggSourceOperatorX;
    void _ensure_agg_source_ready();
    bool _agg_source_ready = false;
};

class AggSourceOperatorX : public OperatorX<AggLocalState> {
public:
    using Base = OperatorX<AggLocalState>;
    AggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs);
    ~AggSourceOperatorX() override = default;

#ifdef BE_TEST
    AggSourceOperatorX() = default;
#endif

    Status get_block(RuntimeState* state, Block* block, bool* eos) override;

    bool is_source() const override { return true; }

    Status merge_with_serialized_key_helper(RuntimeState* state, Block* block);

    size_t get_estimated_memory_size_for_merging(RuntimeState* state, size_t rows) const;

    Status reset_hash_table(RuntimeState* state);

    /// Get a block of serialized intermediate aggregate states from the hash table.
    /// Unlike get_block() which may finalize, this always outputs the serialized
    /// intermediate format (key columns + serialized agg state columns), which is
    /// the same format as the spill block. This is needed for repartitioning during
    /// multi-level spill recovery: the data must be re-mergeable after repartitioning.
    Status get_serialized_block(RuntimeState* state, Block* block, bool* eos);

private:
    friend class AggLocalState;

    bool _needs_finalize;
    bool _without_key;
};

} // namespace doris
#include "common/compile_check_end.h"
