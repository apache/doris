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

#include "pipeline/exec/data_queue.h"
#include "vec/common/sort/sorter.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vanalytic_eval_node.h"

namespace doris {
namespace pipeline {
class Dependency;
using DependencySPtr = std::shared_ptr<Dependency>;

class Dependency {
public:
    Dependency(int id) : _id(id), _done(false) {}
    virtual ~Dependency() = default;

    [[nodiscard]] bool done() const { return _done; }
    void set_done() { _done = true; }

    virtual void* shared_state() = 0;
    [[nodiscard]] int id() const { return _id; }

private:
    int _id;
    std::atomic<bool> _done;
};

struct AggSharedState {
public:
    AggSharedState() {
        agg_data = std::make_unique<vectorized::AggregatedDataVariants>();
        agg_arena_pool = std::make_unique<vectorized::Arena>();
        data_queue = std::make_unique<DataQueue>(1);
    }
    void init_spill_partition_helper(size_t spill_partition_count_bits) {
        spill_partition_helper =
                std::make_unique<vectorized::SpillPartitionHelper>(spill_partition_count_bits);
    }
    vectorized::AggregatedDataVariantsUPtr agg_data;
    std::unique_ptr<vectorized::AggregateDataContainer> aggregate_data_container;
    vectorized::AggSpillContext spill_context;
    vectorized::ArenaUPtr agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> aggregate_evaluators;
    std::unique_ptr<vectorized::SpillPartitionHelper> spill_partition_helper;
    // group by k1,k2
    vectorized::VExprContextSPtrs probe_expr_ctxs;
    std::vector<size_t> probe_key_sz;
    size_t input_num_rows = 0;
    std::vector<vectorized::AggregateDataPtr> values;
    std::unique_ptr<vectorized::Arena> agg_profile_arena;
    std::unique_ptr<DataQueue> data_queue;
};

class AggDependency final : public Dependency {
public:
    AggDependency(int id) : Dependency(id) {
        _mem_tracker = std::make_unique<MemTracker>("AggregateOperator:");
    }
    ~AggDependency() override = default;

    void* shared_state() override { return (void*)&_agg_state; };

    Status reset_hash_table();

    Status merge_spilt_data();

    void set_total_size_of_aggregate_states(size_t total_size_of_aggregate_states) {
        _total_size_of_aggregate_states = total_size_of_aggregate_states;
    }
    void set_align_aggregate_states(size_t align_aggregate_states) {
        _align_aggregate_states = align_aggregate_states;
    }

    void set_offsets_of_aggregate_states(vectorized::Sizes& offsets_of_aggregate_states) {
        _offsets_of_aggregate_states = offsets_of_aggregate_states;
    }

    Status destroy_agg_status(vectorized::AggregateDataPtr data);
    Status create_agg_status(vectorized::AggregateDataPtr data);

    const vectorized::Sizes& offsets_of_aggregate_states() { return _offsets_of_aggregate_states; }

    void set_make_nullable_keys(std::vector<size_t>& make_nullable_keys) {
        _make_nullable_keys = make_nullable_keys;
    }

    const std::vector<size_t>& make_nullable_keys() { return _make_nullable_keys; }
    void release_tracker();

    struct MemoryRecord {
        MemoryRecord() : used_in_arena(0), used_in_state(0) {}
        int64_t used_in_arena;
        int64_t used_in_state;
    };
    MemoryRecord& mem_usage_record() { return _mem_usage_record; }

    MemTracker* mem_tracker() { return _mem_tracker.get(); }

private:
    AggSharedState _agg_state;
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;
    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes _offsets_of_aggregate_states;
    std::vector<size_t> _make_nullable_keys;

    MemoryRecord _mem_usage_record;
    std::unique_ptr<MemTracker> _mem_tracker;
};

struct SortSharedState {
public:
    std::unique_ptr<vectorized::Sorter> sorter;
};

class SortDependency final : public Dependency {
public:
    SortDependency(int id) : Dependency(id) {}
    ~SortDependency() override = default;
    void* shared_state() override { return (void*)&_sort_state; };

private:
    SortSharedState _sort_state;
};

struct AnalyticSharedState {
public:
    AnalyticSharedState() = default;

    int64_t current_row_position = 0;
    vectorized::BlockRowPos partition_by_end;
    vectorized::VExprContextSPtrs partition_by_eq_expr_ctxs;
    int64_t input_total_rows = 0;
    vectorized::BlockRowPos all_block_end;
    std::vector<vectorized::Block> input_blocks;
    bool input_eos = false;
    std::atomic_bool need_more_input = true;
    vectorized::BlockRowPos found_partition_end;
    std::vector<int64_t> origin_cols;
    vectorized::VExprContextSPtrs order_by_eq_expr_ctxs;
    std::vector<int64_t> input_block_first_row_positions;
    std::vector<std::vector<vectorized::MutableColumnPtr>> agg_input_columns;

    // TODO: maybe global?
    std::vector<int64_t> partition_by_column_idxs;
    std::vector<int64_t> ordey_by_column_idxs;
};

class AnalyticDependency final : public Dependency {
public:
    AnalyticDependency(int id) : Dependency(id) {}
    ~AnalyticDependency() override = default;

    void* shared_state() override { return (void*)&_analytic_state; };

    vectorized::BlockRowPos get_partition_by_end();

    bool whether_need_next_partition(vectorized::BlockRowPos found_partition_end);
    vectorized::BlockRowPos compare_row_to_find_end(int idx, vectorized::BlockRowPos start,
                                                    vectorized::BlockRowPos end,
                                                    bool need_check_first = false);

private:
    AnalyticSharedState _analytic_state;
};

} // namespace pipeline
} // namespace doris
