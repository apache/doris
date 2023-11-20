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

#include <sqltypes.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "common/logging.h"
#include "concurrentqueue.h"
#include "gutil/integral_types.h"
#include "pipeline/exec/data_queue.h"
#include "pipeline/exec/multi_cast_data_streamer.h"
#include "vec/common/hash_table/hash_map_context_creator.h"
#include "vec/common/sort/partition_sorter.h"
#include "vec/common/sort/sorter.h"
#include "vec/exec/join/process_hash_table_probe.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vanalytic_eval_node.h"
#include "vec/exec/vpartition_sort_node.h"

namespace doris::pipeline {
class Dependency;
class PipelineXTask;
using DependencySPtr = std::shared_ptr<Dependency>;

static constexpr auto SLOW_DEPENDENCY_THRESHOLD = 60 * 1000L * 1000L * 1000L;
static constexpr auto TIME_UNIT_DEPENDENCY_LOG = 30 * 1000L * 1000L * 1000L;
static_assert(TIME_UNIT_DEPENDENCY_LOG < SLOW_DEPENDENCY_THRESHOLD);

class Dependency : public std::enable_shared_from_this<Dependency> {
public:
    Dependency(int id, int node_id, std::string name)
            : _id(id), _node_id(node_id), _name(std::move(name)), _ready_for_read(false) {}
    virtual ~Dependency() = default;

    [[nodiscard]] int id() const { return _id; }
    [[nodiscard]] virtual std::string name() const { return _name; }
    virtual void* shared_state() = 0;
    virtual std::string debug_string(int indentation_level = 0);
    virtual bool is_write_dependency() { return false; }

    // Start the watcher. We use it to count how long this dependency block the current pipeline task.
    void start_read_watcher() {
        for (auto& child : _children) {
            child->start_read_watcher();
        }
        _read_dependency_watcher.start();
    }

    [[nodiscard]] virtual int64_t read_watcher_elapse_time() {
        return _read_dependency_watcher.elapsed_time();
    }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] virtual Dependency* read_blocked_by(PipelineXTask* task = nullptr);

    // Notify downstream pipeline tasks this dependency is ready.
    virtual void set_ready_for_read();

    // Notify downstream pipeline tasks this dependency is blocked.
    virtual void block_reading() { _ready_for_read = false; }

    void set_parent(std::weak_ptr<Dependency> parent) { _parent = parent; }

    virtual void add_child(std::shared_ptr<Dependency> child) { _children.push_back(child); }

    virtual void add_block_task(PipelineXTask* task);

protected:
    bool _should_log(uint64_t cur_time) {
        if (cur_time < SLOW_DEPENDENCY_THRESHOLD) {
            return false;
        }
        if ((cur_time - _last_log_time) < TIME_UNIT_DEPENDENCY_LOG) {
            return false;
        }
        _last_log_time = cur_time;
        return true;
    }

    int _id;
    const int _node_id;
    std::string _name;
    std::atomic<bool> _ready_for_read;
    MonotonicStopWatch _read_dependency_watcher;

    std::weak_ptr<Dependency> _parent;

    std::list<std::shared_ptr<Dependency>> _children;

    uint64_t _last_log_time = 0;
    std::mutex _task_lock;
    std::vector<PipelineXTask*> _blocked_task;
};

class WriteDependency : public Dependency {
public:
    WriteDependency(int id, int node_id, std::string name) : Dependency(id, node_id, name) {}
    ~WriteDependency() override = default;

    bool is_write_dependency() override { return true; }
    void start_write_watcher() {
        for (auto& child : _children) {
            CHECK(child->is_write_dependency());
            ((WriteDependency*)child.get())->start_write_watcher();
        }
        _write_dependency_watcher.start();
    }

    [[nodiscard]] virtual int64_t write_watcher_elapse_time() {
        return _write_dependency_watcher.elapsed_time();
    }

    [[nodiscard]] virtual WriteDependency* write_blocked_by(PipelineXTask* task);

    virtual void set_ready_for_write();

    virtual void block_writing() { _ready_for_write = false; }

    std::string debug_string(int indentation_level = 0) override;
    void add_write_block_task(PipelineXTask* task);

protected:
    friend class Dependency;
    std::atomic<bool> _ready_for_write {true};
    MonotonicStopWatch _write_dependency_watcher;

private:
    std::vector<PipelineXTask*> _write_blocked_task;
};

class FinishDependency final : public Dependency {
public:
    FinishDependency(int id, int node_id, std::string name) : Dependency(id, node_id, name) {}
    ~FinishDependency() override = default;

    void should_finish_after_check() { _ready_to_finish = false; }
    void start_finish_watcher() {
        for (auto& child : _children) {
            ((FinishDependency*)child.get())->start_finish_watcher();
        }
        _finish_dependency_watcher.start();
    }

    [[nodiscard]] int64_t finish_watcher_elapse_time() {
        return _finish_dependency_watcher.elapsed_time();
    }

    [[nodiscard]] FinishDependency* finish_blocked_by(PipelineXTask* task);

    void set_ready_to_finish();

    void* shared_state() override { return nullptr; }
    std::string debug_string(int indentation_level = 0) override;

    void add_block_task(PipelineXTask* task) override;

private:
    std::atomic<bool> _ready_to_finish {true};
    MonotonicStopWatch _finish_dependency_watcher;
    std::vector<PipelineXTask*> _finish_blocked_task;
};

class RuntimeFilterDependency;
class RuntimeFilterTimer {
public:
    RuntimeFilterTimer(int64_t registration_time, int32_t wait_time_ms,
                       std::shared_ptr<RuntimeFilterDependency> parent,
                       IRuntimeFilter* runtime_filter)
            : _parent(std::move(parent)),
              _registration_time(registration_time),
              _wait_time_ms(wait_time_ms),
              _runtime_filter(runtime_filter) {}

    void call_ready();

    void call_timeout();

    void call_has_ready();

    void call_has_release();

    bool has_ready();

    int64_t registration_time() const { return _registration_time; }
    int32_t wait_time_ms() const { return _wait_time_ms; }

private:
    bool _call_ready {};
    bool _call_timeout {};
    std::shared_ptr<RuntimeFilterDependency> _parent;
    std::mutex _lock;
    const int64_t _registration_time;
    const int32_t _wait_time_ms;
    IRuntimeFilter* _runtime_filter;
};

class RuntimeFilterDependency final : public Dependency {
public:
    RuntimeFilterDependency(int id, int node_id, std::string name)
            : Dependency(id, node_id, name) {}
    RuntimeFilterDependency* filter_blocked_by(PipelineXTask* task);
    void* shared_state() override { return nullptr; }
    void add_filters(IRuntimeFilter* runtime_filter);
    void sub_filters();
    void set_blocked_by_rf(std::shared_ptr<std::atomic_bool> blocked_by_rf) {
        _blocked_by_rf = blocked_by_rf;
    }
    std::string debug_string(int indentation_level = 0) override;

    void add_block_task(PipelineXTask* task) override;

protected:
    std::atomic_int _filters;
    std::shared_ptr<std::atomic_bool> _blocked_by_rf;

private:
    std::vector<PipelineXTask*> _filter_blocked_task;
};

class AndDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(AndDependency);
    AndDependency(int id, int node_id) : WriteDependency(id, node_id, "AndDependency") {}

    [[nodiscard]] std::string name() const override {
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "{}[", _name);
        for (auto& child : _children) {
            fmt::format_to(debug_string_buffer, "{}, ", child->name());
        }
        fmt::format_to(debug_string_buffer, "]");
        return fmt::to_string(debug_string_buffer);
    }

    void* shared_state() override { return nullptr; }

    std::string debug_string(int indentation_level = 0) override;

    [[nodiscard]] Dependency* read_blocked_by(PipelineXTask* task) override {
        for (auto& child : _children) {
            if (auto* dep = child->read_blocked_by(task)) {
                return dep;
            }
        }
        return nullptr;
    }

    [[nodiscard]] WriteDependency* write_blocked_by(PipelineXTask* task) override {
        for (auto& child : _children) {
            CHECK(child->is_write_dependency());
            if (auto* dep = ((WriteDependency*)child.get())->write_blocked_by(task)) {
                return dep;
            }
        }
        return nullptr;
    }
};

struct FakeSharedState {};
struct FakeDependency final : public WriteDependency {
public:
    FakeDependency(int id, int node_id) : WriteDependency(id, node_id, "FakeDependency") {}
    using SharedState = FakeSharedState;
    void* shared_state() override { return nullptr; }
    [[nodiscard]] Dependency* read_blocked_by(PipelineXTask* task) override { return nullptr; }
    [[nodiscard]] WriteDependency* write_blocked_by(PipelineXTask* task) override {
        return nullptr;
    }
    [[nodiscard]] int64_t read_watcher_elapse_time() override { return 0; }
    [[nodiscard]] int64_t write_watcher_elapse_time() override { return 0; }
};

struct AggSharedState {
public:
    AggSharedState() {
        agg_data = std::make_unique<vectorized::AggregatedDataVariants>();
        agg_arena_pool = std::make_unique<vectorized::Arena>();
    }
    virtual ~AggSharedState() = default;
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
    size_t input_num_rows = 0;
    std::vector<vectorized::AggregateDataPtr> values;
    std::unique_ptr<vectorized::Arena> agg_profile_arena;
    std::unique_ptr<DataQueue> data_queue = nullptr;
};

class AggDependency final : public WriteDependency {
public:
    using SharedState = AggSharedState;
    AggDependency(int id, int node_id) : WriteDependency(id, node_id, "AggDependency") {
        _mem_tracker = std::make_unique<MemTracker>("AggregateOperator:");
    }
    ~AggDependency() override = default;

    void block_reading() override {
        if (_is_streaming_agg_state()) {
            if (_agg_state.data_queue->_cur_blocks_nums_in_queue[0] == 0 &&
                !_agg_state.data_queue->_is_finished[0]) {
                Dependency::block_reading();
            }
        } else {
            Dependency::block_reading();
        }
    }

    void block_writing() override {
        if (_is_streaming_agg_state()) {
            if (!_agg_state.data_queue->has_enough_space_to_push()) {
                WriteDependency::block_writing();
            }
        } else {
            WriteDependency::block_writing();
        }
    }

    void set_ready_for_write() override {
        if (_is_streaming_agg_state()) {
            if (_agg_state.data_queue->has_enough_space_to_push()) {
                WriteDependency::set_ready_for_write();
            }
        } else {
            WriteDependency::set_ready_for_write();
        }
    }

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
    void _make_nullable_output_key(vectorized::Block* block) {
        if (block->rows() != 0) {
            for (auto cid : _make_nullable_keys) {
                block->get_by_position(cid).column =
                        make_nullable(block->get_by_position(cid).column);
                block->get_by_position(cid).type = make_nullable(block->get_by_position(cid).type);
            }
        }
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

protected:
    /// The total size of the row from the aggregate functions.
    size_t _total_size_of_aggregate_states = 0;
    size_t _align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes _offsets_of_aggregate_states;
    std::vector<size_t> _make_nullable_keys;

    MemoryRecord _mem_usage_record;
    std::unique_ptr<MemTracker> _mem_tracker;

private:
    bool _is_streaming_agg_state() { return _agg_state.data_queue != nullptr; }
    AggSharedState _agg_state;
};

struct SortSharedState {
public:
    std::unique_ptr<vectorized::Sorter> sorter;
};

class SortDependency final : public WriteDependency {
public:
    using SharedState = SortSharedState;
    SortDependency(int id, int node_id) : WriteDependency(id, node_id, "SortDependency") {}
    ~SortDependency() override = default;
    void* shared_state() override { return (void*)&_sort_state; };

private:
    SortSharedState _sort_state;
};

struct UnionSharedState {
public:
    UnionSharedState(int child_count = 1, WriteDependency* dependency = nullptr)
            : data_queue(child_count, dependency), _child_count(child_count) {};
    int child_count() const { return _child_count; }
    DataQueue data_queue;
    const int _child_count;
};

class UnionDependency final : public WriteDependency {
public:
    using SharedState = UnionSharedState;
    UnionDependency(int id, int node_id) : WriteDependency(id, node_id, "UnionDependency") {}
    ~UnionDependency() override = default;

    void* shared_state() override { return (void*)_union_state.get(); }
    void set_shared_state(std::shared_ptr<UnionSharedState> union_state) {
        _union_state = union_state;
    }

    [[nodiscard]] Dependency* read_blocked_by(PipelineXTask* task) override {
        if (_union_state->child_count() == 0) {
            return nullptr;
        }
        if (_union_state->data_queue.is_all_finish() ||
            _union_state->data_queue.remaining_has_data()) {
            return nullptr;
        }
        return this;
    }
    void block_reading() override {}
    void block_writing() override {}

private:
    std::shared_ptr<UnionSharedState> _union_state;
};

struct MultiCastSharedState {
public:
    MultiCastSharedState(const RowDescriptor& row_desc, ObjectPool* pool, int cast_sender_count)
            : multi_cast_data_streamer(row_desc, pool, cast_sender_count, true) {}
    pipeline::MultiCastDataStreamer multi_cast_data_streamer;
};

class MultiCastDependency final : public WriteDependency {
public:
    using SharedState = MultiCastSharedState;
    MultiCastDependency(int id, int node_id)
            : WriteDependency(id, node_id, "MultiCastDependency") {}
    ~MultiCastDependency() override = default;
    void* shared_state() override { return (void*)_multi_cast_state.get(); };
    void set_shared_state(std::shared_ptr<MultiCastSharedState> multi_cast_state) {
        _multi_cast_state = multi_cast_state;
    }

private:
    std::shared_ptr<MultiCastSharedState> _multi_cast_state;
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
    vectorized::BlockRowPos found_partition_end;
    std::vector<int64_t> origin_cols;
    vectorized::VExprContextSPtrs order_by_eq_expr_ctxs;
    std::vector<int64_t> input_block_first_row_positions;
    std::vector<std::vector<vectorized::MutableColumnPtr>> agg_input_columns;

    // TODO: maybe global?
    std::vector<int64_t> partition_by_column_idxs;
    std::vector<int64_t> ordey_by_column_idxs;
};

class AnalyticDependency final : public WriteDependency {
public:
    using SharedState = AnalyticSharedState;
    AnalyticDependency(int id, int node_id) : WriteDependency(id, node_id, "AnalyticDependency") {}
    ~AnalyticDependency() override = default;

    void* shared_state() override { return (void*)&_analytic_state; };

    vectorized::BlockRowPos get_partition_by_end();

    bool refresh_need_more_input() {
        auto need_more_input = whether_need_next_partition(_analytic_state.found_partition_end);
        if (need_more_input) {
            block_reading();
            set_ready_for_write();
        } else {
            block_writing();
            set_ready_for_read();
        }
        return need_more_input;
    }

    bool whether_need_next_partition(vectorized::BlockRowPos& found_partition_end);
    vectorized::BlockRowPos compare_row_to_find_end(int idx, vectorized::BlockRowPos start,
                                                    vectorized::BlockRowPos end,
                                                    bool need_check_first = false);

private:
    AnalyticSharedState _analytic_state;
};

struct JoinSharedState {
    // For some join case, we can apply a short circuit strategy
    // 1. _has_null_in_build_side = true
    // 2. build side rows is empty, Join op is: inner join/right outer join/left semi/right semi/right anti
    bool _has_null_in_build_side = false;
    bool short_circuit_for_probe = false;
    // for some join, when build side rows is empty, we could return directly by add some additional null data in probe table.
    bool empty_right_table_need_probe_dispose = false;
    vectorized::JoinOpVariants join_op_variants;
};

struct HashJoinSharedState : public JoinSharedState {
    // mark the join column whether support null eq
    std::vector<bool> is_null_safe_eq_join;
    // mark the build hash table whether it needs to store null value
    std::vector<bool> store_null_in_hash_table;
    std::shared_ptr<vectorized::Arena> arena = std::make_shared<vectorized::Arena>();

    // maybe share hash table with other fragment instances
    std::shared_ptr<vectorized::HashTableVariants> hash_table_variants =
            std::make_shared<vectorized::HashTableVariants>();
    const std::vector<TupleDescriptor*> build_side_child_desc;
    size_t build_exprs_size = 0;
    std::shared_ptr<std::vector<vectorized::Block>> build_blocks = nullptr;
    bool probe_ignore_null = false;
};

class HashJoinDependency final : public WriteDependency {
public:
    using SharedState = HashJoinSharedState;
    HashJoinDependency(int id, int node_id) : WriteDependency(id, node_id, "HashJoinDependency") {}
    ~HashJoinDependency() override = default;

    void* shared_state() override { return (void*)&_join_state; }

    Status do_evaluate(vectorized::Block& block, vectorized::VExprContextSPtrs& exprs,
                       RuntimeProfile::Counter& expr_call_timer, std::vector<int>& res_col_ids);

    std::vector<uint16_t> convert_block_to_null(vectorized::Block& block);

    template <bool BuildSide>
    Status extract_join_column(vectorized::Block& block,
                               vectorized::ColumnUInt8::MutablePtr& null_map,
                               vectorized::ColumnRawPtrs& raw_ptrs,
                               const std::vector<int>& res_col_ids);

private:
    HashJoinSharedState _join_state;
};

struct NestedLoopJoinSharedState : public JoinSharedState {
    // if true, left child has no more rows to process
    bool left_side_eos = false;
    // Visited flags for each row in build side.
    vectorized::MutableColumns build_side_visited_flags;
    // List of build blocks, constructed in prepare()
    vectorized::Blocks build_blocks;
};

class NestedLoopJoinDependency final : public WriteDependency {
public:
    using SharedState = NestedLoopJoinSharedState;
    NestedLoopJoinDependency(int id, int node_id)
            : WriteDependency(id, node_id, "NestedLoopJoinDependency") {}
    ~NestedLoopJoinDependency() override = default;

    void* shared_state() override { return (void*)&_join_state; }

private:
    NestedLoopJoinSharedState _join_state;
};

struct PartitionSortNodeSharedState {
public:
    std::queue<vectorized::Block> blocks_buffer;
    std::mutex buffer_mutex;
    std::vector<std::unique_ptr<vectorized::PartitionSorter>> partition_sorts;
    std::unique_ptr<vectorized::SortCursorCmp> previous_row = nullptr;
};

class PartitionSortDependency final : public WriteDependency {
public:
    using SharedState = PartitionSortNodeSharedState;
    PartitionSortDependency(int id, int node_id)
            : WriteDependency(id, node_id, "PartitionSortDependency"), _eos(false) {}
    ~PartitionSortDependency() override = default;
    void* shared_state() override { return (void*)&_partition_sort_state; };
    void set_ready_for_write() override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Should not reach here!");
    }
    void block_writing() override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Should not reach here!");
    }

    void block_reading() override {
        if (_eos) {
            return;
        }
        Dependency::block_reading();
    }

    void set_eos() {
        if (_eos) {
            return;
        }
        _eos = true;
        WriteDependency::set_ready_for_read();
    }

private:
    PartitionSortNodeSharedState _partition_sort_state;
    std::atomic<bool> _eos;
};

class AsyncWriterDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(AsyncWriterDependency);
    AsyncWriterDependency(int id, int node_id)
            : WriteDependency(id, node_id, "AsyncWriterDependency") {}
    ~AsyncWriterDependency() override = default;
    void* shared_state() override { return nullptr; }
};

class SetDependency;

struct SetSharedState {
public:
    SetSharedState(int num_deps) { probe_finished_children_dependency.resize(num_deps, nullptr); }
    /// default init
    //record memory during running
    int64_t mem_used = 0;
    std::vector<vectorized::Block> build_blocks; // build to source
    int build_block_index = 0;                   // build to source
    //record element size in hashtable
    int64_t valid_element_in_hash_tbl = 0;
    //first:column_id, could point to origin column or cast column
    //second:idx mapped to column types
    std::unordered_map<int, int> build_col_idx;

    //// shared static states (shared, decided in prepare/open...)

    /// init in setup_local_state
    std::unique_ptr<vectorized::HashTableVariants> hash_table_variants; // the real data HERE.
    std::vector<bool> build_not_ignore_null;

    /// init in both upstream side.
    //The i-th result expr list refers to the i-th child.
    std::vector<vectorized::VExprContextSPtrs> child_exprs_lists;

    /// init in build side
    int child_quantity;
    vectorized::VExprContextSPtrs build_child_exprs;
    std::vector<SetDependency*> probe_finished_children_dependency;

    /// init in probe side
    std::vector<vectorized::VExprContextSPtrs> probe_child_exprs_lists;

    std::atomic<bool> ready_for_read = false;

    void set_probe_finished_children(int child_id);

    /// called in setup_local_state
    void hash_table_init() {
        if (child_exprs_lists[0].size() == 1 && (!build_not_ignore_null[0])) {
            // Single column optimization
            switch (child_exprs_lists[0][0]->root()->result_type()) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                hash_table_variants->emplace<
                        vectorized::I8HashTableContext<vectorized::RowRefListWithFlags>>();
                break;
            case TYPE_SMALLINT:
                hash_table_variants->emplace<
                        vectorized::I16HashTableContext<vectorized::RowRefListWithFlags>>();
                break;
            case TYPE_INT:
            case TYPE_FLOAT:
            case TYPE_DATEV2:
            case TYPE_DECIMAL32:
                hash_table_variants->emplace<
                        vectorized::I32HashTableContext<vectorized::RowRefListWithFlags>>();
                break;
            case TYPE_BIGINT:
            case TYPE_DOUBLE:
            case TYPE_DATETIME:
            case TYPE_DATE:
            case TYPE_DECIMAL64:
            case TYPE_DATETIMEV2:
                hash_table_variants->emplace<
                        vectorized::I64HashTableContext<vectorized::RowRefListWithFlags>>();
                break;
            case TYPE_LARGEINT:
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL128I:
                hash_table_variants->emplace<
                        vectorized::I128HashTableContext<vectorized::RowRefListWithFlags>>();
                break;
            default:
                hash_table_variants->emplace<
                        vectorized::SerializedHashTableContext<vectorized::RowRefListWithFlags>>();
            }
            return;
        }

        if (!try_get_hash_map_context_fixed<PartitionedHashMap, HashCRC32,
                                            vectorized::RowRefListWithFlags>(
                    *hash_table_variants, child_exprs_lists[0])) {
            hash_table_variants->emplace<
                    vectorized::SerializedHashTableContext<vectorized::RowRefListWithFlags>>();
        }
    }
};

class SetDependency final : public WriteDependency {
public:
    using SharedState = SetSharedState;
    SetDependency(int id, int node_id) : WriteDependency(id, node_id, "SetDependency") {}
    ~SetDependency() override = default;
    void* shared_state() override { return (void*)_set_state.get(); }

    void set_shared_state(std::shared_ptr<SetSharedState> set_state) { _set_state = set_state; }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] Dependency* read_blocked_by(PipelineXTask* task) override {
        if (config::enable_fuzzy_mode && !_set_state->ready_for_read &&
            _should_log(_read_dependency_watcher.elapsed_time())) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id() << " " << _node_id << " block tasks: " << _blocked_task.size();
        }
        std::unique_lock<std::mutex> lc(_task_lock);
        if (!_set_state->ready_for_read && task) {
            add_block_task(task);
        }
        return _set_state->ready_for_read ? nullptr : this;
    }

    // Notify downstream pipeline tasks this dependency is ready.
    void set_ready_for_read() override {
        if (_set_state->ready_for_read) {
            return;
        }
        _read_dependency_watcher.stop();
        _set_state->ready_for_read = true;
    }

    void set_cur_child_id(int id) {
        _set_state->probe_finished_children_dependency[id] = this;
        if (id != 0) {
            block_writing();
        }
    }

private:
    std::shared_ptr<SetSharedState> _set_state;
};

using PartitionedBlock = std::pair<std::shared_ptr<vectorized::Block>,
                                   std::tuple<std::shared_ptr<std::vector<int>>, size_t, size_t>>;
struct LocalExchangeSharedState {
public:
    ENABLE_FACTORY_CREATOR(LocalExchangeSharedState);
    std::vector<moodycamel::ConcurrentQueue<PartitionedBlock>> data_queue;
    std::vector<Dependency*> source_dependencies;
    std::atomic<int> running_sink_operators = 0;
    void add_running_sink_operators() { running_sink_operators++; }
    void sub_running_sink_operators() {
        auto val = running_sink_operators.fetch_sub(1);
        if (val == 1) {
            _set_ready_for_read();
        }
    }
    void _set_ready_for_read() {
        for (auto* dep : source_dependencies) {
            DCHECK(dep);
            dep->set_ready_for_read();
        }
    }
    void set_dep_by_channel_id(Dependency* dep, int channel_id) {
        source_dependencies[channel_id] = dep;
        dep->block_reading();
    }
    void set_ready_for_read(int channel_id) {
        auto* dep = source_dependencies[channel_id];
        DCHECK(dep);
        dep->set_ready_for_read();
    }
};

struct LocalExchangeDependency final : public WriteDependency {
public:
    using SharedState = LocalExchangeSharedState;
    LocalExchangeDependency(int id, int node_id)
            : WriteDependency(id, node_id, "LocalExchangeDependency"),
              _local_exchange_shared_state(nullptr) {}
    ~LocalExchangeDependency() override = default;
    void* shared_state() override { return _local_exchange_shared_state.get(); }
    void set_shared_state(std::shared_ptr<LocalExchangeSharedState> state) {
        DCHECK(_local_exchange_shared_state == nullptr);
        _local_exchange_shared_state = state;
    }

private:
    std::shared_ptr<LocalExchangeSharedState> _local_exchange_shared_state;
};

} // namespace doris::pipeline
