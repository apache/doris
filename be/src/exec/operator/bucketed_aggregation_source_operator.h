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

#include <atomic>

#include "common/status.h"
#include "exec/operator/operator.h"

namespace doris {
class BucketedAggSourceOperatorX;

/// Source-side local state for bucketed hash aggregation.
///
/// Pipelined merge model: source instances are woken up each time a sink instance
/// finishes. They scan buckets [0..255], acquire per-bucket CAS locks, and merge
/// data from finished sink instances into the merge target's bucket hash table.
/// When all sinks have finished and a bucket is fully merged, it is output.
/// After all 256 buckets are output, one source instance handles null key output.
class BucketedAggLocalState final : public PipelineXLocalState<BucketedAggSharedState> {
public:
    using Base = PipelineXLocalState<BucketedAggSharedState>;
    ENABLE_FACTORY_CREATOR(BucketedAggLocalState);
    BucketedAggLocalState(RuntimeState* state, OperatorXBase* parent);
    ~BucketedAggLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status close(RuntimeState* state) override;

private:
    friend class BucketedAggSourceOperatorX;

    /// Main output function. Scans buckets, merges available data, outputs when ready.
    /// Returns with block filled if data is available, or with eos=true when done.
    /// If no work is available (sinks still running, no unprocessed buckets), blocks
    /// the source dependency and returns an empty block.
    Status _get_results(RuntimeState* state, Block* block, bool* eos);

    /// Merge finished sink instances' bucket B data into the merge target's bucket B.
    /// Called under the per-bucket CAS lock. Returns the number of sink instances
    /// that were actually merged in this call.
    int _merge_bucket(int bucket, int merge_target);

    /// Output entries from a merged bucket's hash table. Returns the number of rows output.
    /// Resumes from the current iterator position if a previous get_block call
    /// didn't finish the bucket.
    Status _output_bucket(RuntimeState* state, Block* block, int bucket, int merge_target,
                          uint32_t* rows_output);

    /// Merge null keys from all 256 buckets (in merge target) into one, and output.
    Status _merge_and_output_null_keys(RuntimeState* state, Block* block);

    /// Build value columns (finalize or serialize), combine with key_columns,
    /// and write the result into *block. Shared by _output_bucket and
    /// _merge_and_output_null_keys to avoid duplicating the finalize/serialize
    /// and Block assembly logic.
    void _build_output_block(Block* block, MutableColumns& key_columns,
                             const std::vector<AggregateDataPtr>& values, uint32_t num_rows,
                             bool mem_reuse);

    void _make_nullable_output_key(Block* block);

    /// Wake up all source instances (including self) by setting their dependencies ready.
    /// Called when this source releases a bucket CAS lock, so that blocked
    /// source instances can re-check for available work.
    void _wake_up_other_sources();

    // Bucket currently being output (-1 means no active bucket).
    // If a bucket has too many rows for one batch, we resume output here.
    int _current_output_bucket = -1;

    // This source instance's task index [0, num_instances).
    // Used to wake up other source instances when a bucket lock is released.
    int _task_idx = 0;

    /// Reusable buffer for _output_bucket() to avoid per-call heap allocation.
    /// Holds AggregateDataPtr entries for the current batch.
    std::vector<AggregateDataPtr> _output_values;

    RuntimeProfile::Counter* _get_results_timer = nullptr;
    RuntimeProfile::Counter* _hash_table_iterate_timer = nullptr;
    RuntimeProfile::Counter* _insert_keys_to_column_timer = nullptr;
    RuntimeProfile::Counter* _insert_values_to_column_timer = nullptr;
    RuntimeProfile::Counter* _merge_timer = nullptr;
};

class BucketedAggSourceOperatorX : public OperatorX<BucketedAggLocalState> {
public:
    using Base = OperatorX<BucketedAggLocalState>;
    BucketedAggSourceOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                               const DescriptorTbl& descs);
    ~BucketedAggSourceOperatorX() override = default;

    Status get_block(RuntimeState* state, Block* block, bool* eos) override;

    bool is_source() const override { return true; }

private:
    friend class BucketedAggLocalState;

    bool _needs_finalize;
};

} // namespace doris
