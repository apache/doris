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

#include <functional>
#include <memory>
#include <mutex>

#include "concurrentqueue.h"
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

namespace doris {
namespace pipeline {
class Dependency;
using DependencySPtr = std::shared_ptr<Dependency>;

static constexpr auto SLOW_DEPENDENCY_THRESHOLD = 10 * 1000L * 1000L * 1000L;

class Dependency : public std::enable_shared_from_this<Dependency> {
public:
    Dependency(int id, std::string name) : _id(id), _name(name), _ready_for_read(false) {}
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
    [[nodiscard]] virtual Dependency* read_blocked_by() {
        if (config::enable_fuzzy_mode && !_ready_for_read &&
            _read_dependency_watcher.elapsed_time() > SLOW_DEPENDENCY_THRESHOLD) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id();
        }
        return _ready_for_read ? nullptr : this;
    }

    // Notify downstream pipeline tasks this dependency is ready.
    virtual void set_ready_for_read() {
        if (_ready_for_read) {
            return;
        }
        _read_dependency_watcher.stop();
        _ready_for_read = true;
    }

    // Notify downstream pipeline tasks this dependency is blocked.
    virtual void block_reading() { _ready_for_read = false; }

    void set_parent(std::weak_ptr<Dependency> parent) { _parent = parent; }

    void add_child(std::shared_ptr<Dependency> child) {
        _children.push_back(child);
        child->set_parent(weak_from_this());
    }

    void remove_first_child() { _children.erase(_children.begin()); }

protected:
    int _id;
    std::string _name;
    std::atomic<bool> _ready_for_read;
    MonotonicStopWatch _read_dependency_watcher;

    std::weak_ptr<Dependency> _parent;

    std::list<std::shared_ptr<Dependency>> _children;
};

class WriteDependency : public Dependency {
public:
    WriteDependency(int id, std::string name) : Dependency(id, name), _ready_for_write(true) {}
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

    [[nodiscard]] virtual WriteDependency* write_blocked_by() {
        if (config::enable_fuzzy_mode && !_ready_for_write &&
            _write_dependency_watcher.elapsed_time() > SLOW_DEPENDENCY_THRESHOLD) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id();
        }
        return _ready_for_write ? nullptr : this;
    }

    virtual void set_ready_for_write() {
        if (_ready_for_write) {
            return;
        }
        _write_dependency_watcher.stop();
        _ready_for_write = true;
    }

    virtual void block_writing() { _ready_for_write = false; }

protected:
    std::atomic<bool> _ready_for_write;
    MonotonicStopWatch _write_dependency_watcher;
};

class FinishDependency final : public Dependency {
public:
    FinishDependency(int id, std::string name) : Dependency(id, name), _ready_to_finish(true) {}
    ~FinishDependency() override = default;

    void start_finish_watcher() {
        for (auto& child : _children) {
            ((FinishDependency*)child.get())->start_finish_watcher();
        }
        _finish_dependency_watcher.start();
    }

    [[nodiscard]] virtual int64_t finish_watcher_elapse_time() {
        return _finish_dependency_watcher.elapsed_time();
    }

    [[nodiscard]] virtual FinishDependency* finish_blocked_by() {
        if (config::enable_fuzzy_mode && !_ready_to_finish &&
            _finish_dependency_watcher.elapsed_time() > SLOW_DEPENDENCY_THRESHOLD) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id();
        }
        return _ready_to_finish ? nullptr : this;
    }

    void set_ready_to_finish() {
        if (_ready_to_finish) {
            return;
        }
        _finish_dependency_watcher.stop();
        _ready_to_finish = true;
    }

    void block_finishing() { _ready_to_finish = false; }

    void* shared_state() override { return nullptr; }

protected:
    std::atomic<bool> _ready_to_finish;
    MonotonicStopWatch _finish_dependency_watcher;
};

class AndDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(AndDependency);
    AndDependency(int id) : WriteDependency(id, "AndDependency") {}

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

    [[nodiscard]] Dependency* read_blocked_by() override {
        for (auto& child : _children) {
            if (auto* dep = child->read_blocked_by()) {
                return dep;
            }
        }
        return nullptr;
    }

    [[nodiscard]] WriteDependency* write_blocked_by() override {
        for (auto& child : _children) {
            CHECK(child->is_write_dependency());
            if (auto* dep = ((WriteDependency*)child.get())->write_blocked_by()) {
                return dep;
            }
        }
        return nullptr;
    }
};

class OrDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(OrDependency);
    OrDependency(int id) : WriteDependency(id, "OrDependency") {}

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

    [[nodiscard]] Dependency* read_blocked_by() override {
        Dependency* res = nullptr;
        for (auto& child : _children) {
            auto* cur_res = child->read_blocked_by();
            if (cur_res == nullptr) {
                return nullptr;
            } else {
                res = cur_res;
            }
        }
        return res;
    }

    [[nodiscard]] WriteDependency* write_blocked_by() override {
        WriteDependency* res = nullptr;
        for (auto& child : _children) {
            CHECK(child->is_write_dependency());
            auto* cur_res = ((WriteDependency*)child.get())->write_blocked_by();
            if (cur_res == nullptr) {
                return nullptr;
            } else {
                res = cur_res;
            }
        }
        return res;
    }
};

struct FakeSharedState {};
struct FakeDependency final : public WriteDependency {
public:
    FakeDependency(int id) : WriteDependency(id, "FakeDependency") {}
    using SharedState = FakeSharedState;
    void* shared_state() override { return nullptr; }
    [[nodiscard]] Dependency* read_blocked_by() override { return nullptr; }
    [[nodiscard]] WriteDependency* write_blocked_by() override { return nullptr; }
    [[nodiscard]] int64_t read_watcher_elapse_time() override { return 0; }
    [[nodiscard]] int64_t write_watcher_elapse_time() override { return 0; }
};

class AsyncWriterSinkDependency : public WriteDependency {
public:
    AsyncWriterSinkDependency(int id) : WriteDependency(id, "AsyncWriterSinkDependency") {}
    using SharedState = FakeSharedState;
    void* shared_state() override { return nullptr; }
    [[nodiscard]] Dependency* read_blocked_by() override { return nullptr; }
    [[nodiscard]] WriteDependency* write_blocked_by() override { return _call_func(); }
    void set_write_blocked_by(std::function<WriteDependency*()> call_func) {
        _call_func = call_func;
    }
    std::function<WriteDependency*()> _call_func;
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
    AggDependency(int id) : WriteDependency(id, "AggDependency") {
        _mem_tracker = std::make_unique<MemTracker>("AggregateOperator:");
    }
    ~AggDependency() override = default;

    void block_reading() override {
        if (_is_streaming_agg_state()) {
            if (_agg_state.data_queue->_cur_blocks_nums_in_queue[0] == 0 &&
                !_agg_state.data_queue->_is_finished[0]) {
                _ready_for_read = false;
            }
        } else {
            _ready_for_read = false;
        }
    }

    void block_writing() override {
        if (_is_streaming_agg_state()) {
            if (!_agg_state.data_queue->has_enough_space_to_push()) {
                _ready_for_write = false;
            }
        } else {
            _ready_for_write = false;
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
    SortDependency(int id) : WriteDependency(id, "SortDependency") {}
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
    UnionDependency(int id) : WriteDependency(id, "UnionDependency") {}
    ~UnionDependency() override = default;
    void* shared_state() override { return (void*)_union_state.get(); }
    void set_shared_state(std::shared_ptr<UnionSharedState> union_state) {
        _union_state = union_state;
    }
    void set_ready_for_write() override {}
    void set_ready_for_read() override {
        if (!_union_state->data_queue.is_all_finish()) {
            return;
        }
        if (_ready_for_read) {
            return;
        }
        _read_dependency_watcher.stop();
        _ready_for_read = true;
    }
    [[nodiscard]] Dependency* read_blocked_by() override {
        if (_union_state->child_count() == 0) {
            return nullptr;
        }
        return WriteDependency::read_blocked_by();
    }
    void block_reading() override {}
    void block_writing() override {}

private:
    std::shared_ptr<UnionSharedState> _union_state;
};

struct MultiCastSharedState {
public:
    MultiCastSharedState(const RowDescriptor& row_desc, ObjectPool* pool, int cast_sender_count)
            : multi_cast_data_streamer(row_desc, pool, cast_sender_count) {}
    pipeline::MultiCastDataStreamer multi_cast_data_streamer;
};

class MultiCastDependency final : public WriteDependency {
public:
    using SharedState = MultiCastSharedState;
    MultiCastDependency(int id) : WriteDependency(id, "MultiCastDependency") {}
    ~MultiCastDependency() override = default;
    void* shared_state() override { return (void*)_multi_cast_state.get(); };
    void set_shared_state(std::shared_ptr<MultiCastSharedState> multi_cast_state) {
        _multi_cast_state = multi_cast_state;
    }
    WriteDependency* read_blocked_by() override {
        if (_multi_cast_state->multi_cast_data_streamer.can_read(_consumer_id)) {
            return nullptr;
        }
        return this;
    }
    int _consumer_id {};
    void set_consumer_id(int consumer_id) { _consumer_id = consumer_id; }

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
    AnalyticDependency(int id) : WriteDependency(id, "AnalyticDependency") {}
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
    HashJoinDependency(int id) : WriteDependency(id, "HashJoinDependency") {}
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
    NestedLoopJoinDependency(int id) : WriteDependency(id, "NestedLoopJoinDependency") {}
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
    PartitionSortDependency(int id) : WriteDependency(id, "PartitionSortDependency"), _eos(false) {}
    ~PartitionSortDependency() override = default;
    void* shared_state() override { return (void*)&_partition_sort_state; };
    void set_ready_for_write() override {}
    void block_writing() override {}

    [[nodiscard]] Dependency* read_blocked_by() override {
        if (config::enable_fuzzy_mode && !(_ready_for_read || _eos) &&
            _read_dependency_watcher.elapsed_time() > SLOW_DEPENDENCY_THRESHOLD) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id();
        }
        return _ready_for_read || _eos ? nullptr : this;
    }

    void set_eos() { _eos = true; }

private:
    PartitionSortNodeSharedState _partition_sort_state;
    std::atomic<bool> _eos;
};

class AsyncWriterDependency final : public WriteDependency {
public:
    ENABLE_FACTORY_CREATOR(AsyncWriterDependency);
    AsyncWriterDependency(int id) : WriteDependency(id, "AsyncWriterDependency") {}
    ~AsyncWriterDependency() override = default;
    void* shared_state() override { return nullptr; }
};

struct SetSharedState {
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
    std::vector<bool> probe_finished_children_index; // use in probe side

    /// init in probe side
    std::vector<vectorized::VExprContextSPtrs> probe_child_exprs_lists;

    std::atomic<bool> ready_for_read = false;

public:
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
    SetDependency(int id) : WriteDependency(id, "SetDependency") {}
    ~SetDependency() override = default;
    void* shared_state() override { return (void*)_set_state.get(); }

    void set_shared_state(std::shared_ptr<SetSharedState> set_state) { _set_state = set_state; }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] Dependency* read_blocked_by() override {
        if (config::enable_fuzzy_mode && !_set_state->ready_for_read &&
            _read_dependency_watcher.elapsed_time() > SLOW_DEPENDENCY_THRESHOLD) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id();
        }
        return _set_state->ready_for_read ? nullptr : this;
    }

    [[nodiscard]] WriteDependency* write_blocked_by() override {
        if (is_set_probe) {
            DCHECK((_cur_child_id - 1) < _set_state->probe_finished_children_index.size());
            return _set_state->probe_finished_children_index[_cur_child_id - 1] ? nullptr : this;
        }
        return nullptr;
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
        _cur_child_id = id;
        is_set_probe = true;
    }

private:
    std::shared_ptr<SetSharedState> _set_state;
    int _cur_child_id;
    bool is_set_probe {false};
};

struct LocalExchangeSharedState {
public:
    ENABLE_FACTORY_CREATOR(LocalExchangeSharedState);
    std::vector<moodycamel::ConcurrentQueue<vectorized::Block>> data_queue;
    int num_partitions = 0;
    std::atomic<int> running_sink_operators = 0;
};

struct LocalExchangeDependency final : public WriteDependency {
public:
    using SharedState = LocalExchangeSharedState;
    LocalExchangeDependency(int id)
            : WriteDependency(id, "LocalExchangeDependency"),
              _local_exchange_shared_state(nullptr) {}
    ~LocalExchangeDependency() override = default;
    void* shared_state() override { return _local_exchange_shared_state.get(); }
    void set_shared_state(std::shared_ptr<LocalExchangeSharedState> state) {
        DCHECK(_local_exchange_shared_state == nullptr);
        _local_exchange_shared_state = state;
    }

    void set_channel_id(int channel_id) { _channel_id = channel_id; }

    Dependency* read_blocked_by() override {
        if (config::enable_fuzzy_mode && !_should_run() &&
            _read_dependency_watcher.elapsed_time() > SLOW_DEPENDENCY_THRESHOLD) {
            LOG(WARNING) << "========Dependency may be blocked by some reasons: " << name() << " "
                         << id();
        }
        return _should_run() ? nullptr : this;
    }

private:
    bool _should_run() const {
        DCHECK(_local_exchange_shared_state != nullptr);
        return _local_exchange_shared_state->data_queue[_channel_id].size_approx() > 0 ||
               _local_exchange_shared_state->running_sink_operators == 0;
    }

    std::shared_ptr<LocalExchangeSharedState> _local_exchange_shared_state;
    int _channel_id;
};

} // namespace pipeline
} // namespace doris
