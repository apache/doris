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

#ifdef __APPLE__
#include <netinet/in.h>
#include <sys/_types/_u_int.h>
#endif

#include <concurrentqueue.h>
#include <gen_cpp/internal_service.pb.h>
#include <sqltypes.h>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "core/block/block.h"
#include "core/types.h"
#include "exec/common/agg_utils.h"
#include "exec/common/join_utils.h"
#include "exec/common/set_utils.h"
#include "exec/operator/data_queue.h"
#include "exec/operator/join/process_hash_table_probe.h"
#include "exec/sort/partition_sorter.h"
#include "exec/sort/sorter.h"
#include "exec/spill/spill_file.h"
#include "runtime/runtime_profile_counter_names.h"
#include "util/brpc_closure.h"
#include "util/stack_util.h"

namespace doris {
class AggFnEvaluator;
class VSlotRef;
} // namespace doris

namespace doris {
class Dependency;
class PipelineTask;
struct BasicSharedState;
using DependencySPtr = std::shared_ptr<Dependency>;
class LocalExchangeSourceLocalState;

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

    void create_source_dependencies(int num_sources, int operator_id, int node_id,
                                    const std::string& name);
    Dependency* create_source_dependency(int operator_id, int node_id, const std::string& name);

    Dependency* create_sink_dependency(int dest_id, int node_id, const std::string& name);
    std::vector<DependencySPtr> get_dep_by_channel_id(int channel_id) {
        DCHECK_LT(channel_id, source_deps.size());
        return {source_deps[channel_id]};
    }
};

class Dependency : public std::enable_shared_from_this<Dependency> {
public:
    ENABLE_FACTORY_CREATOR(Dependency);
    Dependency(int id, int node_id, std::string name, bool ready = false)
            : _id(id), _node_id(node_id), _name(std::move(name)), _ready(ready) {}
    virtual ~Dependency() = default;

    [[nodiscard]] int id() const { return _id; }
    [[nodiscard]] virtual std::string name() const { return _name; }
    BasicSharedState* shared_state() { return _shared_state; }
    void set_shared_state(BasicSharedState* shared_state) { _shared_state = shared_state; }
    virtual std::string debug_string(int indentation_level = 0);
    bool ready() const { return _ready; }

    // Start the watcher. We use it to count how long this dependency block the current pipeline task.
    void start_watcher() { _watcher.start(); }
    [[nodiscard]] int64_t watcher_elapse_time() { return _watcher.elapsed_time(); }

    // Which dependency current pipeline task is blocked by. `nullptr` if this dependency is ready.
    [[nodiscard]] Dependency* is_blocked_by(std::shared_ptr<PipelineTask> task = nullptr);
    // Notify downstream pipeline tasks this dependency is ready.
    void set_ready();
    void set_ready_to_read(int channel_id = 0) {
        DCHECK_LT(channel_id, _shared_state->source_deps.size()) << debug_string();
        _shared_state->source_deps[channel_id]->set_ready();
    }
    void set_ready_to_write() {
        DCHECK_EQ(_shared_state->sink_deps.size(), 1) << debug_string();
        _shared_state->sink_deps.front()->set_ready();
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
    void _add_block_task(std::shared_ptr<PipelineTask> task);

    const int _id;
    const int _node_id;
    const std::string _name;
    std::atomic<bool> _ready;

    BasicSharedState* _shared_state = nullptr;
    MonotonicStopWatch _watcher;

    std::mutex _task_lock;
    std::vector<std::weak_ptr<PipelineTask>> _blocked_task;

    // If `_always_ready` is true, `block()` will never block tasks.
    std::atomic<bool> _always_ready = false;
    std::mutex _always_ready_lock;
};

struct FakeSharedState final : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(FakeSharedState)
};

class CountedFinishDependency final : public Dependency {
public:
    using SharedState = FakeSharedState;
    CountedFinishDependency(int id, int node_id, std::string name)
            : Dependency(id, node_id, std::move(name), true) {}

    void add(uint32_t count = 1) {
        std::unique_lock<std::mutex> l(_mtx);
        if (!_counter) {
            block();
        }
        _counter += count;
    }

    void sub() {
        std::unique_lock<std::mutex> l(_mtx);
        // _counter is unsigned: a stray sub() when counter is already 0 would
        // underflow to UINT32_MAX and the dependency would never become ready,
        // hanging the query forever. Fail loudly instead.
        if (_counter == 0) [[unlikely]] {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "CountedFinishDependency::sub() underflow on {}", debug_string());
        }
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

struct RuntimeFilterTimerQueue;
class RuntimeFilterTimer {
public:
    RuntimeFilterTimer(int64_t registration_time, int32_t wait_time_ms,
                       std::shared_ptr<Dependency> parent, bool force_wait_timeout = false)
            : _parent(std::move(parent)),
              _registration_time(registration_time),
              _wait_time_ms(wait_time_ms),
              _force_wait_timeout(force_wait_timeout) {}

    // Called by runtime filter producer.
    void call_ready();

    // Called by RuntimeFilterTimerQueue which is responsible for checking if this rf is timeout.
    void call_timeout();

    int64_t registration_time() const { return _registration_time; }
    int32_t wait_time_ms() const { return _wait_time_ms; }

    void set_local_runtime_filter_dependencies(
            const std::vector<std::shared_ptr<Dependency>>& deps) {
        _local_runtime_filter_dependencies = deps;
    }

    bool should_be_check_timeout();

    bool force_wait_timeout() { return _force_wait_timeout; }

private:
    friend struct RuntimeFilterTimerQueue;
    std::shared_ptr<Dependency> _parent = nullptr;
    std::vector<std::shared_ptr<Dependency>> _local_runtime_filter_dependencies;
    std::mutex _lock;
    int64_t _registration_time;
    const int32_t _wait_time_ms;
    // true only for group_commit_scan_operator
    bool _force_wait_timeout;
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
    void push_filter_timer(std::vector<std::shared_ptr<RuntimeFilterTimer>>&& filter) {
        std::unique_lock<std::mutex> lc(_que_lock);
        _que.insert(_que.end(), filter.begin(), filter.end());
        cv.notify_all();
    }

    std::thread _thread;
    std::condition_variable cv;
    std::mutex cv_m;
    std::mutex _que_lock;
    std::atomic_bool _stop = false;
    std::atomic_bool _shutdown = false;
    std::list<std::shared_ptr<RuntimeFilterTimer>> _que;
};

struct AggSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(AggSharedState)
public:
    AggSharedState() { agg_data = std::make_unique<AggregatedDataVariants>(); }
    ~AggSharedState() override {
        if (!probe_expr_ctxs.empty()) {
            _close_with_serialized_key();
        } else {
            _close_without_key();
        }
    }

    Status reset_hash_table();

    bool do_limit_filter(Block* block, size_t num_rows, const std::vector<int>* key_locs = nullptr);
    void build_limit_heap(size_t hash_table_size);

    // We should call this function only at 1st phase.
    // 1st phase: is_merge=true, only have one SlotRef.
    // 2nd phase: is_merge=false, maybe have multiple exprs.
    static int get_slot_column_id(const AggFnEvaluator* evaluator);

    AggregatedDataVariantsUPtr agg_data = nullptr;
    std::unique_ptr<AggregateDataContainer> aggregate_data_container;
    std::vector<AggFnEvaluator*> aggregate_evaluators;
    // group by k1,k2
    VExprContextSPtrs probe_expr_ctxs;
    size_t input_num_rows = 0;
    std::vector<AggregateDataPtr> values;
    /// The total size of the row from the aggregate functions.
    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    /// The offset to the n-th aggregate function in a row of aggregate functions.
    Sizes offsets_of_aggregate_states;
    std::vector<size_t> make_nullable_keys;

    bool agg_data_created_without_key = false;
    bool enable_spill = false;
    bool reach_limit = false;

    bool use_simple_count = false;
    int64_t limit = -1;
    bool do_sort_limit = false;
    MutableColumns limit_columns;
    int limit_columns_min = -1;
    PaddedPODArray<uint8_t> need_computes;
    std::vector<uint8_t> cmp_res;
    std::vector<int> order_directions;
    std::vector<int> null_directions;

    struct HeapLimitCursor {
        HeapLimitCursor(int row_id, MutableColumns& limit_columns,
                        std::vector<int>& order_directions, std::vector<int>& null_directions)
                : _row_id(row_id),
                  _limit_columns(limit_columns),
                  _order_directions(order_directions),
                  _null_directions(null_directions) {}

        HeapLimitCursor(const HeapLimitCursor& other) = default;

        HeapLimitCursor(HeapLimitCursor&& other) noexcept
                : _row_id(other._row_id),
                  _limit_columns(other._limit_columns),
                  _order_directions(other._order_directions),
                  _null_directions(other._null_directions) {}

        HeapLimitCursor& operator=(const HeapLimitCursor& other) noexcept {
            _row_id = other._row_id;
            return *this;
        }

        HeapLimitCursor& operator=(HeapLimitCursor&& other) noexcept {
            _row_id = other._row_id;
            return *this;
        }

        bool operator<(const HeapLimitCursor& rhs) const {
            for (int i = 0; i < _limit_columns.size(); ++i) {
                const auto& _limit_column = _limit_columns[i];
                auto res = _limit_column->compare_at(_row_id, rhs._row_id, *_limit_column,
                                                     _null_directions[i]) *
                           _order_directions[i];
                if (res < 0) {
                    return true;
                } else if (res > 0) {
                    return false;
                }
            }
            return false;
        }

        int _row_id;
        MutableColumns& _limit_columns;
        std::vector<int>& _order_directions;
        std::vector<int>& _null_directions;
    };

    std::priority_queue<HeapLimitCursor> limit_heap;

    // Refresh the top limit heap with a new row
    void refresh_top_limit(size_t row_id, const ColumnRawPtrs& key_columns);

    Arena agg_arena_pool;
    Arena agg_profile_arena;

private:
    MutableColumns _get_keys_hash_table();

    void _close_with_serialized_key() {
        std::visit(Overload {[&](std::monostate& arg) -> void {
                                 // Do nothing
                             },
                             [&](auto& agg_method) -> void {
                                 if (use_simple_count) {
                                     // Inline count: mapped slots hold UInt64,
                                     // not real agg state pointers. Skip destroy.
                                     return;
                                 }
                                 auto& data = *agg_method.hash_table;
                                 data.for_each_mapped([&](auto& mapped) {
                                     if (mapped) {
                                         _destroy_agg_status(mapped);
                                         mapped = nullptr;
                                     }
                                 });
                                 if (data.has_null_key_data()) {
                                     _destroy_agg_status(
                                             data.template get_null_key_data<AggregateDataPtr>());
                                 }
                             }},
                   agg_data->method_variant);
    }

    void _close_without_key() {
        //because prepare maybe failed, and couldn't create agg data.
        //but finally call close to destory agg data, if agg data has bitmapValue
        //will be core dump, it's not initialized
        if (agg_data_created_without_key) {
            _destroy_agg_status(agg_data->without_key);
            agg_data_created_without_key = false;
        }
    }
    void _destroy_agg_status(AggregateDataPtr data);
};

static constexpr int BUCKETED_AGG_NUM_BUCKETS = 256;

/// Shared state for BucketedAggSinkOperatorX / BucketedAggSourceOperatorX.
///
/// Each sink pipeline instance owns 256 per-bucket hash tables (two-level hash table
/// approach, inspired by ClickHouse). During sink, each row is routed to bucket
/// (hash >> 24) & 0xFF.
///
/// Source-side merge is pipelined with sink completion: as each sink instance finishes,
/// it unblocks all source dependencies. Source instances scan buckets and merge data
/// from finished sink instances into the merge target (the first sink to finish).
/// Each bucket has a CAS lock so only one source works on a bucket at a time.
/// After all sinks finish and all buckets are merged + output, one source handles
/// null key merge and the pipeline completes.
///
/// Thread safety model:
///  - Sink phase: each instance writes only to its own per_instance_data[task_idx]. No locking.
///  - Source phase: per-bucket CAS lock (merge_in_progress). Under the lock, a source
///    scans all finished sink instances and merges their bucket data into the merge
///    target's bucket. Already-merged entries are nulled out to prevent re-processing.
///    Output is only done when all sinks have finished and the bucket is fully merged.
struct BucketedAggSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(BucketedAggSharedState)
public:
    BucketedAggSharedState() = default;
    ~BucketedAggSharedState() override { _close(); }

    /// Per-instance data. One per sink pipeline instance.
    /// Each instance has 256 bucket hash tables + 1 shared arena.
    struct PerInstanceData {
        /// 256 per-bucket hash tables. Each bucket has its own BucketedAggDataVariants.
        /// Uses PHHashMap<StringRef> for string keys instead of StringHashMap.
        std::vector<BucketedAggDataVariantsUPtr> bucket_agg_data;
        ArenaUPtr arena;

        PerInstanceData() : arena(std::make_unique<Arena>()) {
            bucket_agg_data.resize(BUCKETED_AGG_NUM_BUCKETS);
            for (auto& p : bucket_agg_data) {
                p = std::make_unique<BucketedAggDataVariants>();
            }
        }
    };

    /// Per-bucket merge state for pipelined source-side processing.
    struct BucketMergeState {
        /// CAS lock: only one source instance can merge/output this bucket at a time.
        std::atomic<bool> merge_in_progress {false};
        /// Set to true once the bucket is fully merged and all rows have been output.
        std::atomic<bool> output_done {false};
        /// Tracks which sink instances have been merged into the merge target
        /// for this bucket. Accessed only under merge_in_progress CAS lock.
        /// Element i is true when instance i's data for this bucket has been merged.
        /// Sized to num_sink_instances in init_instances().
        std::vector<bool> merged_instances;
    };

    std::vector<PerInstanceData> per_instance_data;
    int num_sink_instances = 0;

    /// Tracks how many sinks have finished. Incremented by each sink on EOS.
    std::atomic<int> num_sinks_finished = 0;

    /// Per-sink completion flags. Set to true when each sink instance finishes.
    /// Source instances read these to know which sinks' data is safe to merge.
    std::unique_ptr<std::atomic<bool>[]> sink_finished;

    /// Index of the first sink instance to finish. Its bucket hash tables serve
    /// as the merge target — all other sinks' data is merged into it.
    /// Initialized to -1; the first sink to finish CAS-sets it to its instance idx.
    std::atomic<int> merge_target_instance = -1;

    /// Per-bucket merge state. Indexed by bucket id [0, 256).
    std::array<BucketMergeState, BUCKETED_AGG_NUM_BUCKETS> bucket_states;

    // Aggregate function metadata (shared, read-only after init).
    std::vector<AggFnEvaluator*> aggregate_evaluators;
    VExprContextSPtrs probe_expr_ctxs;
    size_t total_size_of_aggregate_states = 0;
    size_t align_aggregate_states = 1;
    Sizes offsets_of_aggregate_states;
    std::vector<size_t> make_nullable_keys;

    std::atomic<size_t> input_num_rows {0};

    /// When true, the aggregate has exactly one COUNT(*) function with no args.
    /// In this case, mapped values in the hash table store a UInt64 counter
    /// directly (reinterpret_cast<AggregateDataPtr>) instead of a pointer to
    /// allocated aggregate state. This eliminates create/merge/destroy overhead.
    bool use_simple_count = false;

    // ---- Source-side fields ----

    // Null key handling: null keys are stored separately (not in any bucket).
    // After all buckets are processed, one source instance merges and outputs
    // all null key data. This atomic ensures exactly one source instance does it.
    std::atomic<bool> null_key_output_claimed {false};

    /// Monotonically increasing counter bumped on every state change (bucket lock
    /// release, sink finish). Used by source instances to detect missed wakeups:
    /// if the generation changed between scan start and post-block() re-check,
    /// something happened and the source should unblock immediately.
    std::atomic<uint64_t> state_generation {0};

    /// Initialize per-instance data and optionally run a metadata init callback.
    /// The callback runs exactly once (under std::call_once), must return Status,
    /// and should populate shared metadata like probe_expr_ctxs, aggregate_evaluators, etc.
    /// All threads observe the same init status via _init_status.
    template <typename Func>
    Status init_instances(int num_instances, Func&& metadata_init) {
        std::call_once(_init_once, [&]() {
            num_sink_instances = num_instances;
            per_instance_data.resize(num_instances);
            sink_finished = std::make_unique<std::atomic<bool>[]>(num_instances);
            for (int i = 0; i < num_instances; ++i) {
                sink_finished[i].store(false, std::memory_order_relaxed);
            }
            for (auto& bs : bucket_states) {
                bs.merged_instances.resize(num_instances, false);
            }
            _init_status = std::forward<Func>(metadata_init)();
        });
        return _init_status;
    }

private:
    std::once_flag _init_once;
    Status _init_status;

    void _close() {
        for (auto& inst : per_instance_data) {
            for (auto& bucket_data : inst.bucket_agg_data) {
                _close_one_agg_data(*bucket_data);
            }
        }
    }

    void _close_one_agg_data(BucketedAggDataVariants& agg_data) {
        std::visit(
                Overload {[&](std::monostate& arg) -> void {
                              // Do nothing
                          },
                          [&](auto& agg_method) -> void {
                              if (use_simple_count) {
                                  // simple_count: mapped slots hold UInt64 counters,
                                  // not real agg state pointers. Skip destroy.
                                  return;
                              }
                              auto& data = *agg_method.hash_table;
                              data.for_each_mapped([&](auto& mapped) {
                                  if (mapped) {
                                      _destroy_agg_status(mapped);
                                      mapped = nullptr;
                                  }
                              });
                              if constexpr (std::is_assignable_v<decltype(data.has_null_key_data()),
                                                                 bool>) {
                                  if (data.has_null_key_data()) {
                                      _destroy_agg_status(
                                              data.template get_null_key_data<AggregateDataPtr>());
                                  }
                              }
                          }},
                agg_data.method_variant);
    }

    void _destroy_agg_status(AggregateDataPtr data);
};

struct PartitionedAggSharedState : public BasicSharedState,
                                   public std::enable_shared_from_this<PartitionedAggSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionedAggSharedState)

    PartitionedAggSharedState() = default;
    ~PartitionedAggSharedState() override = default;

    void close();

    AggSharedState* _in_mem_shared_state = nullptr;
    std::shared_ptr<BasicSharedState> _in_mem_shared_state_sptr;

    // partition count is no longer stored in shared state; operators maintain their own
    std::atomic<bool> _is_spilled = false;
    std::deque<SpillFileSPtr> _spill_partitions;
};

struct SortSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(SortSharedState)
public:
    std::shared_ptr<Sorter> sorter;
};

struct SpillSortSharedState : public BasicSharedState,
                              public std::enable_shared_from_this<SpillSortSharedState> {
    ENABLE_FACTORY_CREATOR(SpillSortSharedState)

    SpillSortSharedState() = default;
    ~SpillSortSharedState() override = default;

    void update_spill_block_batch_row_count(RuntimeState* state, const Block* block) {
        auto rows = block->rows();
        if (rows > 0 && 0 == avg_row_bytes) {
            avg_row_bytes = std::max((std::size_t)1, block->bytes() / rows);
            spill_block_batch_row_count =
                    (state->spill_buffer_size_bytes() + avg_row_bytes - 1) / avg_row_bytes;
            LOG(INFO) << "spill sort block batch row count: " << spill_block_batch_row_count;
        }
    }

    void close();

    SortSharedState* in_mem_shared_state = nullptr;
    bool enable_spill = false;
    bool is_spilled = false;
    int64_t limit = -1;
    int64_t offset = 0;
    std::atomic_bool is_closed = false;
    std::shared_ptr<BasicSharedState> in_mem_shared_state_sptr;

    std::deque<SpillFileSPtr> sorted_spill_groups;
    size_t avg_row_bytes = 0;
    size_t spill_block_batch_row_count;
};

struct UnionSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(UnionSharedState)

public:
    UnionSharedState(int child_count = 1) : data_queue(child_count), _child_count(child_count) {};
    int child_count() const { return _child_count; }
    DataQueue data_queue;
    const int _child_count;
};

struct DataQueueSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(DataQueueSharedState)
public:
    DataQueue data_queue;
};

class MultiCastDataStreamer;

struct MultiCastSharedState : public BasicSharedState,
                              public std::enable_shared_from_this<MultiCastSharedState> {
    MultiCastSharedState(ObjectPool* pool, int cast_sender_count, int node_id);

    std::unique_ptr<MultiCastDataStreamer> multi_cast_data_streamer;
};

struct AnalyticSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(AnalyticSharedState)

public:
    AnalyticSharedState() = default;
    std::queue<Block> blocks_buffer;
    std::mutex buffer_mutex;
    bool sink_eos = false;
    std::mutex sink_eos_lock;
    Arena agg_arena_pool;
};

struct JoinSharedState : public BasicSharedState {
    // For some join case, we can apply a short circuit strategy
    // 1. _has_null_in_build_side = true
    // 2. build side rows is empty, Join op is: inner join/right outer join/left semi/right semi/right anti
    bool _has_null_in_build_side = false;
    bool short_circuit_for_probe = false;
    // for some join, when build side rows is empty, we could return directly by add some additional null data in probe table.
    bool empty_right_table_need_probe_dispose = false;
    JoinOpVariants join_op_variants;
};

struct HashJoinSharedState : public JoinSharedState {
    ENABLE_FACTORY_CREATOR(HashJoinSharedState)
    HashJoinSharedState() {
        hash_table_variant_vector.push_back(std::make_shared<JoinDataVariants>());
    }
    HashJoinSharedState(int num_instances) {
        source_deps.resize(num_instances, nullptr);
        hash_table_variant_vector.resize(num_instances, nullptr);
        for (int i = 0; i < num_instances; i++) {
            hash_table_variant_vector[i] = std::make_shared<JoinDataVariants>();
        }
    }
    std::shared_ptr<Arena> arena = std::make_shared<Arena>();

    const std::vector<TupleDescriptor*> build_side_child_desc;
    size_t build_exprs_size = 0;
    std::shared_ptr<Block> build_block;
    std::shared_ptr<std::vector<uint32_t>> build_indexes_null;

    // Used by shared hash table
    // For probe operator, hash table in _hash_table_variants is read-only if visited flags is not
    // used. (visited flags will be used only in right / full outer join).
    //
    // For broadcast join, although hash table is read-only, some states in `_hash_table_variants`
    // are still could be written. For example, serialized keys will be written in a continuous
    // memory in `_hash_table_variants`. So before execution, we should use a local _hash_table_variants
    // which has a shared hash table in it.
    std::vector<std::shared_ptr<JoinDataVariants>> hash_table_variant_vector;

    // whether left semi join could directly return
    // if runtime filters contains local in filter, we can make sure all input rows are matched
    // local filter will always be applied, and in filter could guarantee precise filtering
    // ATTN: we should disable always_true logic for in filter when we set this flag
    bool left_semi_direct_return = false;

    // ASOF JOIN specific fields
    // Whether the inequality is >= or > (true) vs <= or < (false)
    bool asof_inequality_is_greater = true;
    // Whether the inequality is strict (> or <) vs non-strict (>= or <=)
    bool asof_inequality_is_strict = false;

    // ASOF JOIN pre-sorted index with inline values for O(log K) branchless lookup
    // Typed AsofIndexGroups stored in a variant (uint32_t for DateV2, uint64_t for DateTimeV2/TimestampTZ)
    AsofIndexVariant asof_index_groups;
    // build_row_index -> bucket_id for O(1) reverse lookup
    std::vector<uint32_t> asof_build_row_to_bucket;
};

struct PartitionedHashJoinSharedState
        : public HashJoinSharedState,
          public std::enable_shared_from_this<PartitionedHashJoinSharedState> {
    ENABLE_FACTORY_CREATOR(PartitionedHashJoinSharedState)

    std::unique_ptr<RuntimeState> _inner_runtime_state;
    std::shared_ptr<HashJoinSharedState> _inner_shared_state;
    std::vector<std::unique_ptr<MutableBlock>> _partitioned_build_blocks;
    std::vector<SpillFileSPtr> _spilled_build_groups;
    std::atomic<bool> _is_spilled = false;
};

struct NestedLoopJoinSharedState : public JoinSharedState {
    ENABLE_FACTORY_CREATOR(NestedLoopJoinSharedState)
    // if true, probe child has no more rows to process
    bool probe_side_eos = false;
    // Visited flags for each row in build side.
    MutableColumns build_side_visited_flags;
    // List of build blocks, constructed in prepare()
    Blocks build_blocks;
};

struct PartitionSortNodeSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(PartitionSortNodeSharedState)
public:
    std::queue<Block> blocks_buffer;
    std::mutex buffer_mutex;
    std::vector<std::unique_ptr<PartitionSorter>> partition_sorts;
    bool sink_eos = false;
    std::mutex sink_eos_lock;
    std::mutex prepared_finish_lock;
};

struct SetSharedState : public BasicSharedState {
    ENABLE_FACTORY_CREATOR(SetSharedState)
public:
    /// default init
    Block build_block; // build to source
    //record element size in hashtable
    int64_t valid_element_in_hash_tbl = 0;
    //first: idx mapped to column types
    //second: column_id, could point to origin column or cast column
    std::unordered_map<int, int> build_col_idx;

    //// shared static states (shared, decided in prepare/open...)

    /// init in setup_local_state
    std::unique_ptr<SetDataVariants> hash_table_variants =
            std::make_unique<SetDataVariants>(); // the real data HERE.
    std::vector<bool> build_not_ignore_null;

    // The SET operator's child might have different nullable attributes.
    // If a calculation involves both nullable and non-nullable columns, the final output should be a nullable column
    Status update_build_not_ignore_null(const VExprContextSPtrs& ctxs);

    size_t get_hash_table_size() const;
    /// init in both upstream side.
    //The i-th result expr list refers to the i-th child.
    std::vector<VExprContextSPtrs> child_exprs_lists;

    /// init in build side
    size_t child_quantity;
    VExprContextSPtrs build_child_exprs;
    std::vector<Dependency*> probe_finished_children_dependency;

    /// init in probe side
    std::vector<VExprContextSPtrs> probe_child_exprs_lists;

    std::atomic<bool> ready_for_read = false;

    Arena arena;

    /// called in setup_local_state
    Status hash_table_init();
};

enum class ExchangeType : uint8_t {
    NOOP = 0,
    // Shuffle data by Crc32CHashPartitioner
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
    throw Exception(Status::FatalError("__builtin_unreachable"));
}

struct DataDistribution {
    DataDistribution(ExchangeType type) : distribution_type(type) {}
    DataDistribution(ExchangeType type, const std::vector<TExpr>& partition_exprs_)
            : distribution_type(type), partition_exprs(partition_exprs_) {}
    DataDistribution(const DataDistribution& other) = default;
    bool need_local_exchange() const { return distribution_type != ExchangeType::NOOP; }
    DataDistribution& operator=(const DataDistribution& other) = default;
    ExchangeType distribution_type;
    std::vector<TExpr> partition_exprs;
};

class ExchangerBase;

struct LocalExchangeSharedState : public BasicSharedState {
public:
    ENABLE_FACTORY_CREATOR(LocalExchangeSharedState);
    LocalExchangeSharedState(int num_instances);
    ~LocalExchangeSharedState() override;
    std::unique_ptr<ExchangerBase> exchanger {};
    std::vector<RuntimeProfile::Counter*> mem_counters;
    std::atomic<int64_t> mem_usage = 0;
    std::atomic<size_t> _buffer_mem_limit = config::local_exchange_buffer_mem_limit;
    // We need to make sure to add mem_usage first and then enqueue, otherwise sub mem_usage may cause negative mem_usage during concurrent dequeue.
    std::mutex le_lock;
    void sub_running_sink_operators();
    void sub_running_source_operators();
    void _set_always_ready() {
        for (auto& dep : source_deps) {
            DCHECK(dep);
            dep->set_always_ready();
        }
        for (auto& dep : sink_deps) {
            DCHECK(dep);
            dep->set_always_ready();
        }
    }

    Dependency* get_sink_dep_by_channel_id(int channel_id) { return nullptr; }

    void set_ready_to_read(int channel_id) {
        auto& dep = source_deps[channel_id];
        DCHECK(dep) << channel_id;
        dep->set_ready();
    }

    void add_mem_usage(int channel_id, size_t delta) { mem_counters[channel_id]->update(delta); }

    void sub_mem_usage(int channel_id, size_t delta) {
        mem_counters[channel_id]->update(-(int64_t)delta);
    }

    void add_total_mem_usage(size_t delta) {
        if (cast_set<int64_t>(mem_usage.fetch_add(delta) + delta) > _buffer_mem_limit) {
            sink_deps.front()->block();
        }
    }

    void sub_total_mem_usage(size_t delta) {
        auto prev_usage = mem_usage.fetch_sub(delta);
        DCHECK_GE(prev_usage - delta, 0) << "prev_usage: " << prev_usage << " delta: " << delta;
        if (cast_set<int64_t>(prev_usage - delta) <= _buffer_mem_limit) {
            sink_deps.front()->set_ready();
        }
    }

    void set_low_memory_mode(RuntimeState* state) {
        _buffer_mem_limit = std::min<int64_t>(config::local_exchange_buffer_mem_limit,
                                              state->low_memory_mode_buffer_limit());
    }
};

} // namespace doris
