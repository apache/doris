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
#include "vec/core/types.h"
#include "vec/exec/join/process_hash_table_probe.h"
#include "vec/exec/join/vhash_join_node.h"
#include "vec/exec/vaggregation_node.h"
#include "vec/exec/vanalytic_eval_node.h"
#include "vec/exec/vpartition_sort_node.h"
#include "vec/exec/vset_operation_node.h"
#include "vec/spill/spill_stream.h"

namespace doris::pipeline {

class Dependency;
class PipelineTask;
struct BasicSharedState;
using DependencySPtr = std::shared_ptr<Dependency>;
using DependencyMap = std::map<int, std::vector<DependencySPtr>>;

static constexpr auto SLOW_DEPENDENCY_THRESHOLD = 60 * 1000L * 1000L * 1000L;
static constexpr auto TIME_UNIT_DEPENDENCY_LOG = 30 * 1000L * 1000L * 1000L;
static_assert(TIME_UNIT_DEPENDENCY_LOG < SLOW_DEPENDENCY_THRESHOLD);

struct BasicSharedState {
    ENABLE_FACTORY_CREATOR(BasicSharedState)

    template <class TARGET>
    TARGET* cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET*>(this);
    }
    template <class TARGET>
    const TARGET* cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET*>(this);
    }
    std::vector<DependencySPtr> source_deps;
    std::vector<DependencySPtr> sink_deps;
    int id = 0;
    std::set<int> related_op_ids;

    virtual ~BasicSharedState() = default;

    Dependency* create_source_dependency(int operator_id, int node_id, std::string name,
                                         QueryContext* ctx);

    Dependency* create_sink_dependency(int dest_id, int node_id, std::string name,
                                       QueryContext* ctx);
};

class Dependency : public std::enable_shared_from_this<Dependency> {
public:
    ENABLE_FACTORY_CREATOR(Dependency);
    Dependency(int id, int node_id, std::string name, QueryContext* query_ctx)
            : _id(id),
              _node_id(node_id),
              _name(std::move(name)),
              _is_write_dependency(false),
              _ready(false),
              _query_ctx(query_ctx) {}
    Dependency(int id, int node_id, std::string name, bool ready, QueryContext* query_ctx)
            : _id(id),
              _node_id(node_id),
              _name(std::move(name)),
              _is_write_dependency(true),
              _ready(ready),
              _query_ctx(query_ctx) {}
    virtual ~Dependency() = default;

    bool is_write_dependency() const { return _is_write_dependency; }
    [[nodiscard]] int id() const { return _id; }
    [[nodiscard]] virtual std::string name() const { return _name; }
    BasicSharedState* shared_state() { return _shared_state; }
    void set_shared_state(BasicSharedState* shared_state) { _shared_state = shared_state; }
    virtual std::string debug_string(int indentation_level = 0);

    // Start the watcher. We use it to count how long this dependency block the current pipeline task.
    void start_watcher() { _watcher.start(); }
    [[nodiscard]] int64_t watcher_elapse_time() { return _watcher.elapsed_time(); }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] virtual Dependency* is_blocked_by(PipelineTask* task = nullptr);
    // Notify downstream pipeline tasks this dependency is ready.
    void set_ready();
    void set_ready_to_read() {
        DCHECK(_is_write_dependency) << debug_string();
        DCHECK(_shared_state->source_deps.size() == 1) << debug_string();
        _shared_state->source_deps.front()->set_ready();
    }
    void set_block_to_read() {
        DCHECK(_is_write_dependency) << debug_string();
        DCHECK(_shared_state->source_deps.size() == 1) << debug_string();
        _shared_state->source_deps.front()->block();
    }
    void set_ready_to_write() {
        DCHECK(_shared_state->sink_deps.size() == 1) << debug_string();
        _shared_state->sink_deps.front()->set_ready();
    }
    void set_block_to_write() {
        DCHECK(_shared_state->sink_deps.size() == 1) << debug_string();
        _shared_state->sink_deps.front()->block();
    }

    // Notify downstream pipeline tasks this dependency is blocked.
    void block() {
        if (_always_ready) {
            return;
        }
        std::unique_lock<std::mutex> lc(_always_ready_lock);
        if (_always_ready) {
            return;
        }
        _ready = false;
    }

    void set_always_ready() {
        if (_always_ready) {
            return;
        }
        std::unique_lock<std::mutex> lc(_always_ready_lock);
        if (_always_ready) {
            return;
        }
        _always_ready = true;
        set_ready();
    }

protected:
    void _add_block_task(PipelineTask* task);
    bool _is_cancelled() const { return _query_ctx->is_cancelled(); }

    const int _id;
    const int _node_id;
    const std::string _name;
    const bool _is_write_dependency;
    std::atomic<bool> _ready;
    const QueryContext* _query_ctx = nullptr;

    BasicSharedState* _shared_state = nullptr;
    MonotonicStopWatch _watcher;

    std::mutex _task_lock;
    std::vector<PipelineTask*> _blocked_task;

    // If `_always_ready` is true, `block()` will never block tasks.
    std::atomic<bool> _always_ready = false;
    std::mutex _always_ready_lock;
};

struct FakeSharedState final : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(FakeSharedState)
};

struct FakeDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    FakeDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "FakeDependency", query_ctx) {}

    [[nodiscard]] Dependency* is_blocked_by(PipelineTask* task) override { return nullptr; }
};

struct FinishDependency : public Dependency {
public:
    using SharedState = FakeSharedState;
    FinishDependency(int id, int node_id, std::string name, QueryContext* query_ctx)
            : Dependency(id, node_id, name, true, query_ctx) {}

    [[nodiscard]] Dependency* is_blocked_by(PipelineTask* task) override;
};

struct CountedFinishDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    CountedFinishDependency(int id, int node_id, std::string name, QueryContext* query_ctx)
            : Dependency(id, node_id, name, true, query_ctx) {}

    void add() {
        std::unique_lock<std::mutex> l(_mtx);
        if (!_counter) {
            block();
        }
        _counter++;
    }

    void sub() {
        std::unique_lock<std::mutex> l(_mtx);
        _counter--;
        if (!_counter) {
            set_ready();
        }
    }

    std::string debug_string(int indentation_level = 0) override;

private:
    std::mutex _mtx;
    uint32_t _counter = 0;
};

class RuntimeFilterDependency;
struct RuntimeFilterTimerQueue;
class RuntimeFilterTimer {
public:
    RuntimeFilterTimer(int64_t registration_time, int32_t wait_time_ms,
                       std::shared_ptr<RuntimeFilterDependency> parent)
            : _parent(std::move(parent)),
              _registration_time(registration_time),
              _wait_time_ms(wait_time_ms) {}

    // Called by runtime filter producer.
    void call_ready();

    // Called by RuntimeFilterTimerQueue which is responsible for checking if this rf is timeout.
    void call_timeout();

    int64_t registration_time() const { return _registration_time; }
    int32_t wait_time_ms() const { return _wait_time_ms; }

private:
    friend struct RuntimeFilterTimerQueue;
    std::shared_ptr<RuntimeFilterDependency> _parent = nullptr;
    std::mutex _lock;
    const int64_t _registration_time;
    const int32_t _wait_time_ms;
};

struct RuntimeFilterTimerQueue {
    constexpr static int64_t interval = 10;
    void run() { _thread.detach(); }
    void start();

    void stop() {
        _stop = true;
        cv.notify_all();
        wait_for_shutdown();
    }

    void wait_for_shutdown() const {
        while (!_shutdown) {
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }

    ~RuntimeFilterTimerQueue() = default;
    RuntimeFilterTimerQueue() { _thread = std::thread(&RuntimeFilterTimerQueue::start, this); }
    void push_filter_timer(std::shared_ptr<pipeline::RuntimeFilterTimer> filter) { push(filter); }

    void push(std::shared_ptr<pipeline::RuntimeFilterTimer> filter) {
        std::unique_lock<std::mutex> lc(_que_lock);
        _que.push_back(filter);
        cv.notify_all();
    }

    std::thread _thread;
    std::condition_variable cv;
    std::mutex cv_m;
    std::mutex _que_lock;
    std::atomic_bool _stop = false;
    std::atomic_bool _shutdown = false;
    std::list<std::shared_ptr<pipeline::RuntimeFilterTimer>> _que;
};

class RuntimeFilterDependency final : public Dependency {
public:
    RuntimeFilterDependency(int id, int node_id, std::string name, QueryContext* query_ctx,
                            IRuntimeFilter* runtime_filter)
            : Dependency(id, node_id, name, query_ctx), _runtime_filter(runtime_filter) {}
    std::string debug_string(int indentation_level = 0) override;

    Dependency* is_blocked_by(PipelineTask* task) override;

private:
    const IRuntimeFilter* _runtime_filter = nullptr;
};

struct AggSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(AggSharedState)
public:
    AggSharedState() {
        agg_data = std::make_unique<vectorized::AggregatedDataVariants>();
        agg_arena_pool = std::make_unique<vectorized::Arena>();
    }
    ~AggSharedState() override {
        if (!probe_expr_ctxs.empty()) {
            _close_with_serialized_key();
        }
    }

    Status reset_hash_table();

    // We should call this function only at 1st phase.
    // 1st phase: is_merge=true, only have one SlotRef.
    // 2nd phase: is_merge=false, maybe have multiple exprs.
    static int get_slot_column_id(const vectorized::AggFnEvaluator* evaluator) {
        auto ctxs = evaluator->input_exprs_ctxs();
        CHECK(ctxs.size() == 1 && ctxs[0]->root()->is_slot_ref())
                << "input_exprs_ctxs is invalid, input_exprs_ctx[0]="
                << ctxs[0]->root()->debug_string();
        return ((vectorized::VSlotRef*)ctxs[0]->root().get())->column_id();
    }

    vectorized::AggregatedDataVariantsUPtr agg_data = nullptr;
    std::unique_ptr<vectorized::AggregateDataContainer> aggregate_data_container;
    vectorized::ArenaUPtr agg_arena_pool;
    std::vector<vectorized::AggFnEvaluator*> aggregate_evaluators;
    // group by k1,k2
    vectorized::VExprContextSPtrs probe_expr_ctxs;
    size_t input_num_rows = 0;
    std::vector<vectorized::AggregateDataPtr> values;
    std::unique_ptr<vectorized::Arena> agg_profile_arena;
    /// The total size of the row from the aggregate functions.
    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    vectorized::Sizes offsets_of_aggregate_states;
    std::vector<size_t> make_nullable_keys;

    struct MemoryRecord {
        MemoryRecord() : used_in_arena(0), used_in_state(0) {}
        int64_t used_in_arena;
        int64_t used_in_state;
    };
    MemoryRecord mem_usage_record;
    bool enable_spill = false;

private:
    void _close_with_serialized_key() {
        std::visit(vectorized::Overload {[&](std::monostate& arg) -> void {
                                             // Do nothing
                                         },
                                         [&](auto& agg_method) -> void {
                                             auto& data = *agg_method.hash_table;
                                             data.for_each_mapped([&](auto& mapped) {
                                                 if (mapped) {
                                                     static_cast<void>(_destroy_agg_status(mapped));
                                                     mapped = nullptr;
                                                 }
                                             });
                                             if (data.has_null_key_data()) {
                                                 auto st = _destroy_agg_status(
                                                         data.template get_null_key_data<
                                                                 vectorized::AggregateDataPtr>());
                                                 if (!st) {
                                                     throw Exception(st.code(), st.to_string());
                                                 }
                                             }
                                         }},
                   agg_data->method_variant);
    }

    Status _destroy_agg_status(vectorized::AggregateDataPtr data) {
        for (int i = 0; i < aggregate_evaluators.size(); ++i) {
            aggregate_evaluators[i]->function()->destroy(data + offsets_of_aggregate_states[i]);
        }
        return Status::OK();
    }
};

struct AggSpillPartition;
struct PartitionedAggSharedState : public BasicSharedState,
                                   public std::enable_shared_from_this<PartitionedAggSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionedAggSharedState)

    PartitionedAggSharedState() = default;
    ~PartitionedAggSharedState() override = default;

    void init_spill_params(size_t spill_partition_count_bits);

    void close();

    AggSharedState* in_mem_shared_state = nullptr;
    std::shared_ptr<BasicSharedState> in_mem_shared_state_sptr;

    size_t partition_count_bits;
    size_t partition_count;
    size_t max_partition_index;
    Status sink_status;
    bool is_spilled = false;
    std::atomic_bool is_closed = false;
    std::deque<std::shared_ptr<AggSpillPartition>> spill_partitions;

    size_t get_partition_index(size_t hash_value) const {
        return (hash_value >> (32 - partition_count_bits)) & max_partition_index;
    }
};

struct AggSpillPartition {
    static constexpr int64_t AGG_SPILL_FILE_SIZE = 1024 * 1024 * 1024; // 1G

    AggSpillPartition() = default;

    void close();

    Status get_spill_stream(RuntimeState* state, int node_id, RuntimeProfile* profile,
                            vectorized::SpillStreamSPtr& spilling_stream);

    Status flush_if_full() {
        DCHECK(spilling_stream_);
        Status status;
        // avoid small spill files
        if (spilling_stream_->get_written_bytes() >= AGG_SPILL_FILE_SIZE) {
            status = spilling_stream_->spill_eof();
            spilling_stream_.reset();
        }
        return status;
    }

    Status finish_current_spilling(bool eos = false) {
        if (spilling_stream_) {
            if (eos || spilling_stream_->get_written_bytes() >= AGG_SPILL_FILE_SIZE) {
                auto status = spilling_stream_->spill_eof();
                spilling_stream_.reset();
                return status;
            }
        }
        return Status::OK();
    }

    std::deque<vectorized::SpillStreamSPtr> spill_streams_;
    vectorized::SpillStreamSPtr spilling_stream_;
};
using AggSpillPartitionSPtr = std::shared_ptr<AggSpillPartition>;
struct SortSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(SortSharedState)
public:
    std::unique_ptr<vectorized::Sorter> sorter;
};

struct SpillSortSharedState : public BasicSharedState,
                              public std::enable_shared_from_this<SpillSortSharedState> {
    ENABLE_FACTORY_CREATOR(SpillSortSharedState)

    SpillSortSharedState() = default;
    ~SpillSortSharedState() override = default;

    // This number specifies the maximum size of sub blocks
    static constexpr int SORT_BLOCK_SPILL_BATCH_BYTES = 8 * 1024 * 1024;
    void update_spill_block_batch_row_count(const vectorized::Block* block) {
        auto rows = block->rows();
        if (rows > 0 && 0 == avg_row_bytes) {
            avg_row_bytes = std::max((std::size_t)1, block->bytes() / rows);
            spill_block_batch_row_count =
                    (SORT_BLOCK_SPILL_BATCH_BYTES + avg_row_bytes - 1) / avg_row_bytes;
            LOG(INFO) << "spill sort block batch row count: " << spill_block_batch_row_count;
        }
    }
    void close();

    SortSharedState* in_mem_shared_state = nullptr;
    bool enable_spill = false;
    bool is_spilled = false;
    std::atomic_bool is_closed = false;
    Status sink_status;
    std::shared_ptr<BasicSharedState> in_mem_shared_state_sptr;

    std::deque<vectorized::SpillStreamSPtr> sorted_streams;
    size_t avg_row_bytes = 0;
    int spill_block_batch_row_count;
};

struct UnionSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(UnionSharedState)

public:
    UnionSharedState(int child_count = 1) : data_queue(child_count), _child_count(child_count) {};
    int child_count() const { return _child_count; }
    DataQueue data_queue;
    const int _child_count;
};

struct MultiCastSharedState : public BasicSharedState {
public:
    MultiCastSharedState(const RowDescriptor& row_desc, ObjectPool* pool, int cast_sender_count)
            : multi_cast_data_streamer(row_desc, pool, cast_sender_count, true) {}
    pipeline::MultiCastDataStreamer multi_cast_data_streamer;
};

struct AnalyticSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(AnalyticSharedState)

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

struct JoinSharedState : public BasicSharedState {
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
    ENABLE_FACTORY_CREATOR(HashJoinSharedState)
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
    std::shared_ptr<vectorized::Block> build_block;
    std::shared_ptr<std::vector<uint32_t>> build_indexes_null;
    bool probe_ignore_null = false;
};

struct PartitionedHashJoinSharedState
        : public HashJoinSharedState,
          public std::enable_shared_from_this<PartitionedHashJoinSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinSharedState)

    std::unique_ptr<RuntimeState> inner_runtime_state;
    std::shared_ptr<HashJoinSharedState> inner_shared_state;
    std::vector<std::unique_ptr<vectorized::MutableBlock>> partitioned_build_blocks;
    std::vector<vectorized::SpillStreamSPtr> spilled_streams;
    bool need_to_spill = false;
};

struct NestedLoopJoinSharedState : public JoinSharedState {
    ENABLE_FACTORY_CREATOR(NestedLoopJoinSharedState)
    // if true, left child has no more rows to process
    bool left_side_eos = false;
    // Visited flags for each row in build side.
    vectorized::MutableColumns build_side_visited_flags;
    // List of build blocks, constructed in prepare()
    vectorized::Blocks build_blocks;
};

struct PartitionSortNodeSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(PartitionSortNodeSharedState)
public:
    std::queue<vectorized::Block> blocks_buffer;
    std::mutex buffer_mutex;
    std::vector<std::unique_ptr<vectorized::PartitionSorter>> partition_sorts;
    bool sink_eos = false;
    std::mutex sink_eos_lock;
};

class AsyncWriterDependency final : public Dependency {
public:
    using SharedState = BasicSharedState;
    ENABLE_FACTORY_CREATOR(AsyncWriterDependency);
    AsyncWriterDependency(int id, int node_id, QueryContext* query_ctx)
            : Dependency(id, node_id, "AsyncWriterDependency", true, query_ctx) {}
    ~AsyncWriterDependency() override = default;
};

struct SetSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(SetSharedState)
public:
    /// default init
    vectorized::Block build_block; // build to source
    //record element size in hashtable
    int64_t valid_element_in_hash_tbl = 0;
    //first:column_id, could point to origin column or cast column
    //second:idx mapped to column types
    std::unordered_map<int, int> build_col_idx;

    //// shared static states (shared, decided in prepare/open...)

    /// init in setup_local_state
    std::unique_ptr<vectorized::SetHashTableVariants> hash_table_variants =
            nullptr; // the real data HERE.
    std::vector<bool> build_not_ignore_null;

    /// init in both upstream side.
    //The i-th result expr list refers to the i-th child.
    std::vector<vectorized::VExprContextSPtrs> child_exprs_lists;

    /// init in build side
    int child_quantity;
    vectorized::VExprContextSPtrs build_child_exprs;
    std::vector<Dependency*> probe_finished_children_dependency;

    /// init in probe side
    std::vector<vectorized::VExprContextSPtrs> probe_child_exprs_lists;

    std::atomic<bool> ready_for_read = false;

    /// called in setup_local_state
    void hash_table_init() {
        using namespace vectorized;
        if (child_exprs_lists[0].size() == 1 && (!build_not_ignore_null[0])) {
            // Single column optimization
            switch (child_exprs_lists[0][0]->root()->result_type()) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt8>>();
                break;
            case TYPE_SMALLINT:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt16>>();
                break;
            case TYPE_INT:
            case TYPE_FLOAT:
            case TYPE_DATEV2:
            case TYPE_DECIMAL32:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt32>>();
                break;
            case TYPE_BIGINT:
            case TYPE_DOUBLE:
            case TYPE_DATETIME:
            case TYPE_DATE:
            case TYPE_DECIMAL64:
            case TYPE_DATETIMEV2:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt64>>();
                break;
            case TYPE_LARGEINT:
            case TYPE_DECIMALV2:
            case TYPE_DECIMAL128I:
                hash_table_variants->emplace<SetPrimaryTypeHashTableContext<UInt128>>();
                break;
            default:
                hash_table_variants->emplace<SetSerializedHashTableContext>();
            }
            return;
        }
        if (!try_get_hash_map_context_fixed<NormalHashMap, HashCRC32, RowRefListWithFlags>(
                    *hash_table_variants, child_exprs_lists[0])) {
            hash_table_variants->emplace<SetSerializedHashTableContext>();
        }
    }
};

enum class ExchangeType : uint8_t {
    NOOP = 0,
    // Shuffle data by Crc32HashPartitioner<LocalExchangeChannelIds>.
    HASH_SHUFFLE = 1,
    // Round-robin passthrough data blocks.
    PASSTHROUGH = 2,
    // Shuffle data by Crc32HashPartitioner<ShuffleChannelIds> (e.g. same as storage engine).
    BUCKET_HASH_SHUFFLE = 3,
    // Passthrough data blocks to all channels.
    BROADCAST = 4,
    // Passthrough data to channels evenly in an adaptive way.
    ADAPTIVE_PASSTHROUGH = 5,
    // Send all data to the first channel.
    PASS_TO_ONE = 6,
};

inline std::string get_exchange_type_name(ExchangeType idx) {
    switch (idx) {
    case ExchangeType::NOOP:
        return "NOOP";
    case ExchangeType::HASH_SHUFFLE:
        return "HASH_SHUFFLE";
    case ExchangeType::PASSTHROUGH:
        return "PASSTHROUGH";
    case ExchangeType::BUCKET_HASH_SHUFFLE:
        return "BUCKET_HASH_SHUFFLE";
    case ExchangeType::BROADCAST:
        return "BROADCAST";
    case ExchangeType::ADAPTIVE_PASSTHROUGH:
        return "ADAPTIVE_PASSTHROUGH";
    case ExchangeType::PASS_TO_ONE:
        return "PASS_TO_ONE";
    }
    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}

struct DataDistribution {
    DataDistribution(ExchangeType type) : distribution_type(type) {}
    DataDistribution(ExchangeType type, const std::vector<TExpr>& partition_exprs_)
            : distribution_type(type), partition_exprs(partition_exprs_) {}
    DataDistribution(const DataDistribution& other)
            : distribution_type(other.distribution_type), partition_exprs(other.partition_exprs) {}
    bool need_local_exchange() const { return distribution_type != ExchangeType::NOOP; }
    DataDistribution& operator=(const DataDistribution& other) {
        distribution_type = other.distribution_type;
        partition_exprs = other.partition_exprs;
        return *this;
    }
    ExchangeType distribution_type;
    std::vector<TExpr> partition_exprs;
};

class Exchanger;

struct LocalExchangeSharedState : public BasicSharedState {
public:
    ENABLE_FACTORY_CREATOR(LocalExchangeSharedState);
    LocalExchangeSharedState(int num_instances);
    std::unique_ptr<Exchanger> exchanger {};
    std::vector<MemTracker*> mem_trackers;
    std::atomic<size_t> mem_usage = 0;
    std::mutex le_lock;
    void create_source_dependencies(int operator_id, int node_id, QueryContext* ctx) {
        for (size_t i = 0; i < source_deps.size(); i++) {
            source_deps[i] = std::make_shared<Dependency>(
                    operator_id, node_id, "LOCAL_EXCHANGE_OPERATOR_DEPENDENCY", ctx);
            source_deps[i]->set_shared_state(this);
        }
    };
    void sub_running_sink_operators();
    void _set_always_ready() {
        for (auto& dep : source_deps) {
            DCHECK(dep);
            dep->set_always_ready();
        }
    }

    Dependency* get_dep_by_channel_id(int channel_id) { return source_deps[channel_id].get(); }

    void set_ready_to_read(int channel_id) {
        auto& dep = source_deps[channel_id];
        DCHECK(dep) << channel_id;
        dep->set_ready();
    }

    void add_mem_usage(int channel_id, size_t delta, bool update_total_mem_usage = true) {
        mem_trackers[channel_id]->consume(delta);
        if (update_total_mem_usage) {
            add_total_mem_usage(delta);
        }
    }

    void sub_mem_usage(int channel_id, size_t delta, bool update_total_mem_usage = true) {
        mem_trackers[channel_id]->release(delta);
        if (update_total_mem_usage) {
            sub_total_mem_usage(delta);
        }
    }

    void add_total_mem_usage(size_t delta) {
        if (mem_usage.fetch_add(delta) > config::local_exchange_buffer_mem_limit) {
            sink_deps.front()->block();
        }
    }

    void sub_total_mem_usage(size_t delta) {
        if (mem_usage.fetch_sub(delta) <= config::local_exchange_buffer_mem_limit) {
            sink_deps.front()->set_ready();
        }
    }
};

} // namespace doris::pipeline
