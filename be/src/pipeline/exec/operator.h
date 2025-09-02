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

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/be_mock_util.h"
#include "common/logging.h"
#include "common/status.h"
#include "pipeline/dependency.h"
#include "pipeline/exec/operator.h"
#include "pipeline/exec/spill_utils.h"
#include "pipeline/local_exchange/local_exchanger.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris {
#include "common/compile_check_begin.h"
class RowDescriptor;
class RuntimeState;
class TDataSink;
namespace vectorized {
class AsyncResultWriter;
class ScoreRuntime;
class AnnTopNRuntime;
} // namespace vectorized
} // namespace doris

namespace doris::pipeline {

class OperatorBase;
class OperatorXBase;
class DataSinkOperatorXBase;

using OperatorPtr = std::shared_ptr<OperatorXBase>;
using Operators = std::vector<OperatorPtr>;

using DataSinkOperatorPtr = std::shared_ptr<DataSinkOperatorXBase>;

// This suffix will be added back to the name of sink operator
// when we creating runtime profile.
const std::string exchange_sink_name_suffix = "(dest_id={})";

const std::string operator_name_suffix = "(id={})";

// This struct is used only for initializing local state.
struct LocalStateInfo {
    RuntimeProfile* parent_profile = nullptr;
    const std::vector<TScanRangeParams>& scan_ranges;
    BasicSharedState* shared_state;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>& shared_state_map;
    const int task_idx;
};

// This struct is used only for initializing local sink state.
struct LocalSinkStateInfo {
    const int task_idx;
    RuntimeProfile* parent_profile = nullptr;
    const int sender_id;
    BasicSharedState* shared_state;
    const std::map<int, std::pair<std::shared_ptr<BasicSharedState>,
                                  std::vector<std::shared_ptr<Dependency>>>>& shared_state_map;
    const TDataSink& tsink;
};

class OperatorBase {
public:
    explicit OperatorBase() : _child(nullptr), _is_closed(false) {}
    explicit OperatorBase(bool is_serial_operator)
            : _child(nullptr), _is_closed(false), _is_serial_operator(is_serial_operator) {}
    virtual ~OperatorBase() = default;

    virtual bool is_sink() const { return false; }

    virtual bool is_source() const { return false; }

    [[nodiscard]] virtual const RowDescriptor& row_desc() const;

    [[nodiscard]] virtual Status init(const TDataSink& tsink) { return Status::OK(); }

    [[nodiscard]] virtual std::string get_name() const = 0;
    [[nodiscard]] virtual Status prepare(RuntimeState* state) = 0;
    [[nodiscard]] virtual Status terminate(RuntimeState* state) = 0;
    [[nodiscard]] virtual Status close(RuntimeState* state);
    [[nodiscard]] virtual int node_id() const = 0;

    [[nodiscard]] virtual Status set_child(OperatorPtr child) {
        if (_child && child != nullptr) {
            return Status::InternalError("Child is already set in node name={}", get_name());
        }
        _child = child;
        return Status::OK();
    }

    // Operators need to be executed serially. (e.g. finalized agg without key)
    [[nodiscard]] virtual bool is_serial_operator() const { return _is_serial_operator; }

    [[nodiscard]] bool is_closed() const { return _is_closed; }

    virtual size_t revocable_mem_size(RuntimeState* state) const { return 0; }

    virtual Status revoke_memory(RuntimeState* state,
                                 const std::shared_ptr<SpillContext>& spill_context) {
        return Status::OK();
    }

    /**
     * Pipeline task is blockable means it will be blocked in the next run. So we should put the
     * pipeline task into the blocking task scheduler.
     */
    virtual bool is_blockable(RuntimeState* state) const = 0;

    virtual void set_low_memory_mode(RuntimeState* state) {}

    [[nodiscard]] virtual bool require_data_distribution() const { return false; }
    OperatorPtr child() { return _child; }
    [[nodiscard]] bool followed_by_shuffled_operator() const {
        return _followed_by_shuffled_operator;
    }
    void set_followed_by_shuffled_operator(bool followed_by_shuffled_operator) {
        _followed_by_shuffled_operator = followed_by_shuffled_operator;
    }
    [[nodiscard]] virtual bool is_shuffled_operator() const { return false; }
    [[nodiscard]] virtual DataDistribution required_data_distribution() const;
    [[nodiscard]] virtual bool require_shuffled_data_distribution() const;

protected:
    OperatorPtr _child = nullptr;

    bool _is_closed;
    bool _followed_by_shuffled_operator = false;
    bool _is_serial_operator = false;
};

class PipelineXLocalStateBase {
public:
    PipelineXLocalStateBase(RuntimeState* state, OperatorXBase* parent);
    virtual ~PipelineXLocalStateBase() = default;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    virtual Status init(RuntimeState* state, LocalStateInfo& info) = 0;
    // Make sure all resources are ready before execution. For example, remote tablets should be
    // loaded to local storage.
    // This is called by execution pthread and different from `Operator::prepare` which is called
    // by bthread.
    virtual Status prepare(RuntimeState* state) = 0;
    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state) = 0;
    virtual Status terminate(RuntimeState* state) = 0;

    // If use projection, we should clear `_origin_block`.
    void clear_origin_block();

    void reached_limit(vectorized::Block* block, bool* eos);
    RuntimeProfile* operator_profile() { return _operator_profile.get(); }
    RuntimeProfile* common_profile() { return _common_profile.get(); }
    RuntimeProfile* custom_profile() { return _custom_profile.get(); }

    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }
    OperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    vectorized::VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] int64_t num_rows_returned() const { return _num_rows_returned; }
    void add_num_rows_returned(int64_t delta) { _num_rows_returned += delta; }
    void set_num_rows_returned(int64_t value) { _num_rows_returned = value; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level = 0) const = 0;
    [[nodiscard]] virtual bool is_blockable() const;

    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in Scan
    virtual Dependency* finishdependency() { return nullptr; }
    virtual Dependency* spill_dependency() const { return nullptr; }
    //  override in Scan  MultiCastSink
    virtual std::vector<Dependency*> execution_dependencies() { return {}; }

    Status filter_block(const vectorized::VExprContextSPtrs& expr_contexts,
                        vectorized::Block* block, size_t column_to_keep);

    int64_t& estimate_memory_usage() { return _estimate_memory_usage; }

    void reset_estimate_memory_usage() { _estimate_memory_usage = 0; }

    bool low_memory_mode() {
#ifdef BE_TEST
        return false;
#else
        return _state->low_memory_mode();
#endif
    }

protected:
    friend class OperatorXBase;
    template <typename LocalStateType>
    friend class ScanOperatorX;

    ObjectPool* _pool = nullptr;
    int64_t _num_rows_returned {0};
    int64_t _estimate_memory_usage {0};

    /*
    Each operator has its profile like this:
    XXXX_OPERATOR:
        CommonCounters:
            ...
        CustomCounters:
            ...
    */
    // Profile of this operator.
    // Should not modify this profile usually.
    std::unique_ptr<RuntimeProfile> _operator_profile;
    // CommonCounters of this operator.
    // CommonCounters are counters that will be used by all operators.
    std::unique_ptr<RuntimeProfile> _common_profile;
    // CustomCounters of this operator.
    // CustomCounters are counters that will be used by this operator only.
    std::unique_ptr<RuntimeProfile> _custom_profile;

    RuntimeProfile::Counter* _rows_returned_counter = nullptr;
    RuntimeProfile::Counter* _blocks_returned_counter = nullptr;
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    // Account for current memory and peak memory used by this node
    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _projection_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;

    OperatorXBase* _parent = nullptr;
    RuntimeState* _state = nullptr;
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    std::shared_ptr<vectorized::ScoreRuntime> _score_runtime;
    std::shared_ptr<segment_v2::AnnTopNRuntime> _ann_topn_runtime;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<vectorized::VExprContextSPtrs> _intermediate_projections;

    bool _closed = false;
    std::atomic<bool> _terminated = false;
    vectorized::Block _origin_block;
    vectorized::Block _projection_block;
};

template <typename SharedStateArg = FakeSharedState>
class PipelineXLocalState : public PipelineXLocalStateBase {
public:
    using SharedStateType = SharedStateArg;
    PipelineXLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalStateBase(state, parent) {}
    ~PipelineXLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;
    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override;

    virtual std::string name_suffix() const;

    Status close(RuntimeState* state) override;
    Status terminate(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }
    Dependency* spill_dependency() const override { return _spill_dependency.get(); }

    virtual bool must_set_shared_state() const {
        return !std::is_same_v<SharedStateArg, FakeSharedState>;
    }

protected:
    Dependency* _dependency = nullptr;
    std::shared_ptr<Dependency> _spill_dependency;
    SharedStateArg* _shared_state = nullptr;
};

template <typename SharedStateArg>
class PipelineXSpillLocalState : public PipelineXLocalState<SharedStateArg> {
public:
    using Base = PipelineXLocalState<SharedStateArg>;
    PipelineXSpillLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<SharedStateArg>(state, parent) {}
    ~PipelineXSpillLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXLocalState<SharedStateArg>::init(state, info));

        init_spill_read_counters();

        return Status::OK();
    }

    void init_spill_write_counters() {
        _spill_write_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTime", 1);

        _spill_write_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteTaskWaitInQueueCount", TUnit::UNIT, 1);
        _spill_writing_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillWriteTaskCount", TUnit::UNIT, 1);
        _spill_write_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTaskWaitInQueueTime", 1);

        _spill_write_file_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteFileTime", 1);

        _spill_write_serialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteSerializeBlockTime", 1);
        _spill_write_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                          "SpillWriteBlockCount", TUnit::UNIT, 1);
        _spill_write_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
        _spill_write_file_total_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileBytes", TUnit::BYTES, 1);
        _spill_write_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillWriteRows", TUnit::UNIT, 1);
        _spill_file_total_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileTotalCount", TUnit::UNIT, 1);
    }

    void init_spill_read_counters() {
        _spill_total_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillTotalTime", 1);

        // Spill read counters
        _spill_recover_time = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillRecoverTime", 1);

        _spill_read_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillReadTaskWaitInQueueCount", TUnit::UNIT, 1);
        _spill_reading_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillReadTaskCount", TUnit::UNIT, 1);
        _spill_read_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadTaskWaitInQueueTime", 1);

        _spill_read_file_time =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileTime", 1);
        _spill_read_derialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillReadDerializeBlockTime", 1);

        _spill_read_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                         "SpillReadBlockCount", TUnit::UNIT, 1);
        _spill_read_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillReadBlockBytes", TUnit::BYTES, 1);
        _spill_read_file_size = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadFileBytes",
                                                       TUnit::BYTES, 1);
        _spill_read_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillReadRows", TUnit::UNIT, 1);
        _spill_read_file_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                        "SpillReadFileCount", TUnit::UNIT, 1);

        _spill_file_current_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileCurrentBytes", TUnit::BYTES, 1);
        _spill_file_current_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteFileCurrentCount", TUnit::UNIT, 1);
    }

    // These two counters are shared to spill source operators as the initial value
    // Initialize values of counters 'SpillWriteFileCurrentBytes' and 'SpillWriteFileCurrentCount'
    // from spill sink operators' "SpillWriteFileTotalCount" and "SpillWriteFileBytes"
    void copy_shared_spill_profile() {
        if (_copy_shared_spill_profile) {
            _copy_shared_spill_profile = false;
            const auto* spill_shared_state = (const BasicSpillSharedState*)Base::_shared_state;
            COUNTER_UPDATE(_spill_file_current_size,
                           spill_shared_state->_spill_write_file_total_size->value());
            COUNTER_UPDATE(_spill_file_current_count,
                           spill_shared_state->_spill_file_total_count->value());
            Base::_shared_state->update_spill_stream_profiles(Base::custom_profile());
        }
    }

    // Total time of spill, including spill task scheduling time,
    // serialize block time, write disk file time,
    // and read disk file time, deserialize block time etc.
    RuntimeProfile::Counter* _spill_total_timer = nullptr;

    // Spill write counters
    // Total time of spill write, including serialize block time, write disk file,
    // and wait in queue time, etc.
    RuntimeProfile::Counter* _spill_write_timer = nullptr;

    RuntimeProfile::Counter* _spill_write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _spill_writing_task_count = nullptr;
    RuntimeProfile::Counter* _spill_write_wait_in_queue_timer = nullptr;

    // Total time of writing file
    RuntimeProfile::Counter* _spill_write_file_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_serialize_block_timer = nullptr;
    // Original count of spilled Blocks
    // One Big Block maybe split into multiple small Blocks when actually written to disk file.
    RuntimeProfile::Counter* _spill_write_block_count = nullptr;
    // Total bytes of spill data in Block format(in memory format)
    RuntimeProfile::Counter* _spill_write_block_data_size = nullptr;
    // Total bytes of spill data written to disk file(after serialized)
    RuntimeProfile::Counter* _spill_write_file_total_size = nullptr;
    RuntimeProfile::Counter* _spill_write_rows_count = nullptr;
    RuntimeProfile::Counter* _spill_file_total_count = nullptr;
    RuntimeProfile::Counter* _spill_file_current_count = nullptr;
    // Spilled file total size
    RuntimeProfile::Counter* _spill_file_total_size = nullptr;
    // Current spilled file size
    RuntimeProfile::Counter* _spill_file_current_size = nullptr;

    // Spill read counters
    // Total time of recovring spilled data, including read file time, deserialize time, etc.
    RuntimeProfile::Counter* _spill_recover_time = nullptr;

    RuntimeProfile::Counter* _spill_read_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _spill_reading_task_count = nullptr;
    RuntimeProfile::Counter* _spill_read_wait_in_queue_timer = nullptr;

    RuntimeProfile::Counter* _spill_read_file_time = nullptr;
    RuntimeProfile::Counter* _spill_read_derialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_read_block_count = nullptr;
    // Total bytes of read data in Block format(in memory format)
    RuntimeProfile::Counter* _spill_read_block_data_size = nullptr;
    // Total bytes of spill data read from disk file
    RuntimeProfile::Counter* _spill_read_file_size = nullptr;
    RuntimeProfile::Counter* _spill_read_rows_count = nullptr;
    RuntimeProfile::Counter* _spill_read_file_count = nullptr;

    bool _copy_shared_spill_profile = true;
};

class DataSinkOperatorXBase;

class PipelineXSinkLocalStateBase {
public:
    PipelineXSinkLocalStateBase(DataSinkOperatorXBase* parent_, RuntimeState* state_);
    virtual ~PipelineXSinkLocalStateBase() = default;

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    virtual Status prepare(RuntimeState* state) = 0;
    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) = 0;
    virtual Status terminate(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state, Status exec_status) = 0;
    [[nodiscard]] virtual bool is_finished() const { return false; }
    [[nodiscard]] virtual bool is_blockable() const { return false; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    DataSinkOperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    RuntimeProfile* operator_profile() { return _operator_profile; }
    RuntimeProfile* common_profile() { return _common_profile; }
    RuntimeProfile* custom_profile() { return _custom_profile; }

    [[nodiscard]] RuntimeProfile* faker_runtime_profile() const {
        return _faker_runtime_profile.get();
    }

    RuntimeProfile::Counter* rows_input_counter() { return _rows_input_counter; }
    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }

    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in exchange sink , AsyncWriterSink
    virtual Dependency* finishdependency() { return nullptr; }
    virtual Dependency* spill_dependency() const { return nullptr; }

    bool low_memory_mode() { return _state->low_memory_mode(); }

protected:
    DataSinkOperatorXBase* _parent = nullptr;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _operator_profile = nullptr;
    RuntimeProfile* _common_profile = nullptr;
    RuntimeProfile* _custom_profile = nullptr;
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed = false;
    bool _terminated = false;
    //NOTICE: now add a faker profile, because sometimes the profile record is useless
    //so we want remove some counters and timers, eg: in join node, if it's broadcast_join
    //and shared hash table, some counter/timer about build hash table is useless,
    //so we could add those counter/timer in faker profile, and those will not display in web profile.
    std::unique_ptr<RuntimeProfile> _faker_runtime_profile =
            std::make_unique<RuntimeProfile>("faker profile");

    RuntimeProfile::Counter* _rows_input_counter = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    RuntimeProfile::Counter* _wait_for_finish_dependency_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    RuntimeProfile::HighWaterMarkCounter* _memory_used_counter = nullptr;
};

template <typename SharedStateArg = FakeSharedState>
class PipelineXSinkLocalState : public PipelineXSinkLocalStateBase {
public:
    using SharedStateType = SharedStateArg;
    PipelineXSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalStateBase(parent, state) {}
    ~PipelineXSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }

    Status terminate(RuntimeState* state) override;
    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] std::string debug_string(int indentation_level) const override;

    virtual std::string name_suffix();

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }
    Dependency* spill_dependency() const override { return _spill_dependency.get(); }

    virtual bool must_set_shared_state() const {
        return !std::is_same_v<SharedStateArg, FakeSharedState>;
    }

protected:
    Dependency* _dependency = nullptr;
    std::shared_ptr<Dependency> _spill_dependency;
    SharedStateType* _shared_state = nullptr;
};

class DataSinkOperatorXBase : public OperatorBase {
public:
    DataSinkOperatorXBase(const int operator_id, const int node_id, const int dest_id)
            : _operator_id(operator_id), _node_id(node_id), _dests_id({dest_id}) {}
    DataSinkOperatorXBase(const int operator_id, const TPlanNode& tnode, const int dest_id)
            : OperatorBase(tnode.__isset.is_serial_operator && tnode.is_serial_operator),
              _operator_id(operator_id),
              _node_id(tnode.node_id),
              _dests_id({dest_id}) {}

    DataSinkOperatorXBase(const int operator_id, const int node_id, std::vector<int>& dests)
            : _operator_id(operator_id), _node_id(node_id), _dests_id(dests) {}

#ifdef BE_TEST
    DataSinkOperatorXBase() : _operator_id(-1), _node_id(0), _dests_id({-1}) {};
#endif

    ~DataSinkOperatorXBase() override = default;

    // For agg/sort/join sink.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);

    Status init(const TDataSink& tsink) override;
    [[nodiscard]] virtual Status init(ExchangeType type, const int num_buckets,
                                      const bool use_global_hash_shuffle,
                                      const std::map<int, int>& shuffle_idx_to_instance_idx) {
        return Status::InternalError("init() is only implemented in local exchange!");
    }

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status terminate(RuntimeState* state) override;
    [[nodiscard]] bool is_finished(RuntimeState* state) const {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->is_finished();
    }

    [[nodiscard]] virtual Status sink(RuntimeState* state, vectorized::Block* block, bool eos) = 0;

    [[nodiscard]] virtual Status setup_local_state(RuntimeState* state,
                                                   LocalSinkStateInfo& info) = 0;

    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state, bool eos) {
        return state->minimum_operator_memory_required_bytes();
    }
    bool is_blockable(RuntimeState* state) const override {
        return state->get_sink_local_state()->is_blockable();
    }

    [[nodiscard]] bool is_spillable() const { return _spillable; }

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    [[nodiscard]] virtual std::shared_ptr<BasicSharedState> create_shared_state() const = 0;

    Status close(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const;

    [[nodiscard]] virtual std::string debug_string(RuntimeState* state,
                                                   int indentation_level) const;

    [[nodiscard]] bool is_sink() const override { return true; }

    static Status close(RuntimeState* state, Status exec_status) {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->close(state, exec_status);
    }

    [[nodiscard]] int operator_id() const { return _operator_id; }

    [[nodiscard]] const std::vector<int>& dests_id() const { return _dests_id; }

    [[nodiscard]] int nereids_id() const { return _nereids_id; }

    [[nodiscard]] int node_id() const override { return _node_id; }

    [[nodiscard]] std::string get_name() const override { return _name; }

    virtual bool should_dry_run(RuntimeState* state) { return false; }

    [[nodiscard]] virtual bool count_down_destination() { return true; }

protected:
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;
    // _operator_id : the current Operator's ID, which is not visible to the user.
    // _node_id : the plan node ID corresponding to the Operator, which is visible on the profile.
    // _dests_id : the target _operator_id of the sink, for example, in the case of a multi-sink, there are multiple targets.
    const int _operator_id;
    const int _node_id;
    int _nereids_id = -1;
    bool _spillable = false;
    std::vector<int> _dests_id;
    std::string _name;
};

template <typename LocalStateType>
class DataSinkOperatorX : public DataSinkOperatorXBase {
public:
    DataSinkOperatorX(const int id, const int node_id, const int dest_id)
            : DataSinkOperatorXBase(id, node_id, dest_id) {}
    DataSinkOperatorX(const int id, const TPlanNode& tnode, const int dest_id)
            : DataSinkOperatorXBase(id, tnode, dest_id) {}

    DataSinkOperatorX(const int id, const int node_id, std::vector<int> dest_ids)
            : DataSinkOperatorXBase(id, node_id, dest_ids) {}
#ifdef BE_TEST
    DataSinkOperatorX() = default;
#endif
    ~DataSinkOperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;
    std::shared_ptr<BasicSharedState> create_shared_state() const override;

    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_sink_local_state()->template cast<LocalState>();
    }
};

template <typename SharedStateArg>
class PipelineXSpillSinkLocalState : public PipelineXSinkLocalState<SharedStateArg> {
public:
    using Base = PipelineXSinkLocalState<SharedStateArg>;
    PipelineXSpillSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state) {}
    ~PipelineXSpillSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(Base::init(state, info));
        init_spill_counters();
        return Status::OK();
    }

    void init_spill_counters() {
        _spill_total_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillTotalTime", 1);

        _spill_write_timer = ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTime", 1);

        _spill_write_wait_in_queue_task_count = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteTaskWaitInQueueCount", TUnit::UNIT, 1);
        _spill_writing_task_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                           "SpillWriteTaskCount", TUnit::UNIT, 1);
        _spill_write_wait_in_queue_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteTaskWaitInQueueTime", 1);

        _spill_write_file_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteFileTime", 1);

        _spill_write_serialize_block_timer =
                ADD_TIMER_WITH_LEVEL(Base::custom_profile(), "SpillWriteSerializeBlockTime", 1);
        _spill_write_block_count = ADD_COUNTER_WITH_LEVEL(Base::custom_profile(),
                                                          "SpillWriteBlockCount", TUnit::UNIT, 1);
        _spill_write_block_data_size = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillWriteBlockBytes", TUnit::BYTES, 1);
        _spill_write_rows_count =
                ADD_COUNTER_WITH_LEVEL(Base::custom_profile(), "SpillWriteRows", TUnit::UNIT, 1);

        _spill_max_rows_of_partition = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillMaxRowsOfPartition", TUnit::UNIT, 1);
        _spill_min_rows_of_partition = ADD_COUNTER_WITH_LEVEL(
                Base::custom_profile(), "SpillMinRowsOfPartition", TUnit::UNIT, 1);
    }

    std::vector<Dependency*> dependencies() const override {
        auto dependencies = Base::dependencies();
        return dependencies;
    }

    void update_max_min_rows_counter() {
        int64_t max_rows = 0;
        int64_t min_rows = std::numeric_limits<int64_t>::max();

        for (auto rows : _rows_in_partitions) {
            if (rows > max_rows) {
                max_rows = rows;
            }
            if (rows < min_rows) {
                min_rows = rows;
            }
        }

        COUNTER_SET(_spill_max_rows_of_partition, max_rows);
        COUNTER_SET(_spill_min_rows_of_partition, min_rows);
    }

    std::vector<int64_t> _rows_in_partitions;

    // Total time of spill, including spill task scheduling time,
    // serialize block time, write disk file time,
    // and read disk file time, deserialize block time etc.
    RuntimeProfile::Counter* _spill_total_timer = nullptr;

    // Spill write counters
    // Total time of spill write, including serialize block time, write disk file,
    // and wait in queue time, etc.
    RuntimeProfile::Counter* _spill_write_timer = nullptr;

    RuntimeProfile::Counter* _spill_write_wait_in_queue_task_count = nullptr;
    RuntimeProfile::Counter* _spill_writing_task_count = nullptr;
    RuntimeProfile::Counter* _spill_write_wait_in_queue_timer = nullptr;

    // Total time of writing file
    RuntimeProfile::Counter* _spill_write_file_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_serialize_block_timer = nullptr;
    // Original count of spilled Blocks
    // One Big Block maybe split into multiple small Blocks when actually written to disk file.
    RuntimeProfile::Counter* _spill_write_block_count = nullptr;
    // Total bytes of spill data in Block format(in memory format)
    RuntimeProfile::Counter* _spill_write_block_data_size = nullptr;
    RuntimeProfile::Counter* _spill_write_rows_count = nullptr;
    // Spilled file total size
    RuntimeProfile::Counter* _spill_file_total_size = nullptr;

    RuntimeProfile::Counter* _spill_max_rows_of_partition = nullptr;
    RuntimeProfile::Counter* _spill_min_rows_of_partition = nullptr;
};

class OperatorXBase : public OperatorBase {
public:
    OperatorXBase(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                  const DescriptorTbl& descs)
            : OperatorBase(tnode.__isset.is_serial_operator && tnode.is_serial_operator),
              _operator_id(operator_id),
              _node_id(tnode.node_id),
              _type(tnode.node_type),
              _pool(pool),
              _tuple_ids(tnode.row_tuples),
              _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
              _resource_profile(tnode.resource_profile),
              _limit(tnode.limit) {
        if (tnode.__isset.output_tuple_id) {
            _output_row_descriptor.reset(new RowDescriptor(descs, {tnode.output_tuple_id}, {true}));
            _output_row_descriptor = std::make_unique<RowDescriptor>(
                    descs, std::vector {tnode.output_tuple_id}, std::vector {true});
        }
        if (!tnode.intermediate_output_tuple_id_list.empty()) {
            // common subexpression elimination
            _intermediate_output_row_descriptor.reserve(
                    tnode.intermediate_output_tuple_id_list.size());
            for (auto output_tuple_id : tnode.intermediate_output_tuple_id_list) {
                _intermediate_output_row_descriptor.push_back(
                        RowDescriptor(descs, std::vector {output_tuple_id}, std::vector {true}));
            }
        }
    }

    OperatorXBase(ObjectPool* pool, int node_id, int operator_id)
            : OperatorBase(),
              _operator_id(operator_id),
              _node_id(node_id),
              _pool(pool),
              _limit(-1) {}

#ifdef BE_TEST
    OperatorXBase() : _operator_id(-1), _node_id(0), _limit(-1) {};
#endif
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);
    Status init(const TDataSink& tsink) override {
        throw Exception(Status::FatalError("should not reach here!"));
    }
    virtual Status init(ExchangeType type) {
        throw Exception(Status::FatalError("should not reach here!"));
    }
    [[noreturn]] virtual const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, _op_name);
    }
    [[nodiscard]] std::string get_name() const override { return _op_name; }
    [[nodiscard]] virtual bool need_more_input_data(RuntimeState* state) const { return true; }
    bool is_blockable(RuntimeState* state) const override {
        return state->get_sink_local_state()->is_blockable();
    }

    Status prepare(RuntimeState* state) override;

    Status terminate(RuntimeState* state) override;
    [[nodiscard]] virtual Status get_block(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) = 0;

    Status close(RuntimeState* state) override;

    [[nodiscard]] virtual const RowDescriptor& intermediate_row_desc() const {
        return _row_descriptor;
    }

    [[nodiscard]] const RowDescriptor& intermediate_row_desc(int idx) {
        if (idx == 0) {
            return intermediate_row_desc();
        }
        DCHECK((idx - 1) < _intermediate_output_row_descriptor.size());
        return _intermediate_output_row_descriptor[idx - 1];
    }

    [[nodiscard]] const RowDescriptor& projections_row_desc() const {
        if (_intermediate_output_row_descriptor.empty()) {
            return intermediate_row_desc();
        } else {
            return _intermediate_output_row_descriptor.back();
        }
    }

    size_t revocable_mem_size(RuntimeState* state) const override {
        return (_child and !is_source()) ? _child->revocable_mem_size(state) : 0;
    }

    // If this method is not overwrite by child, its default value is 1MB
    [[nodiscard]] virtual size_t get_reserve_mem_size(RuntimeState* state) {
        return state->minimum_operator_memory_required_bytes();
    }

    virtual std::string debug_string(int indentation_level = 0) const;

    virtual std::string debug_string(RuntimeState* state, int indentation_level = 0) const;

    virtual Status setup_local_state(RuntimeState* state, LocalStateInfo& info) = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this))
                << " Mismatch type! Current type is " << typeid(*this).name()
                << " and expect type is" << typeid(TARGET).name();
        return reinterpret_cast<const TARGET&>(*this);
    }

    [[nodiscard]] OperatorPtr get_child() { return _child; }

    [[nodiscard]] vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    [[nodiscard]] vectorized::VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] virtual RowDescriptor& row_descriptor() { return _row_descriptor; }

    [[nodiscard]] int operator_id() const { return _operator_id; }
    [[nodiscard]] int node_id() const override { return _node_id; }
    [[nodiscard]] int nereids_id() const { return _nereids_id; }

    [[nodiscard]] int64_t limit() const { return _limit; }

    [[nodiscard]] const RowDescriptor& row_desc() const override {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }

    [[nodiscard]] const RowDescriptor* output_row_descriptor() {
        return _output_row_descriptor.get();
    }

    bool has_output_row_desc() const { return _output_row_descriptor != nullptr; }

    [[nodiscard]] virtual Status get_block_after_projects(RuntimeState* state,
                                                          vectorized::Block* block, bool* eos);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    Status do_projections(RuntimeState* state, vectorized::Block* origin_block,
                          vectorized::Block* output_block) const;
    void set_parallel_tasks(int parallel_tasks) { _parallel_tasks = parallel_tasks; }
    int parallel_tasks() const { return _parallel_tasks; }

    // To keep compatibility with older FE
    void set_serial_operator() { _is_serial_operator = true; }

    virtual void reset_reserve_mem_size(RuntimeState* state) {}

protected:
    template <typename Dependency>
    friend class PipelineXLocalState;
    friend class PipelineXLocalStateBase;
    friend class Scanner;
    const int _operator_id;
    const int _node_id; // unique w/in single plan tree
    int _nereids_id = -1;
    TPlanNodeType::type _type;
    ObjectPool* _pool = nullptr;
    std::vector<TupleId> _tuple_ids;

private:
    // The expr of operator set to private permissions, as cannot be executed concurrently,
    // should use local state's expr.
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<vectorized::VExprContextSPtrs> _intermediate_projections;

protected:
    RowDescriptor _row_descriptor;
    std::unique_ptr<RowDescriptor> _output_row_descriptor = nullptr;
    std::vector<RowDescriptor> _intermediate_output_row_descriptor;

    /// Resource information sent from the frontend.
    const TBackendResourceProfile _resource_profile;

    int64_t _limit; // -1: no limit

    uint32_t _debug_point_count = 0;
    std::atomic_uint32_t _bytes_per_row = 0;

    std::string _op_name;
    int _parallel_tasks = 0;

    //_keep_origin is used to avoid copying during projection,
    // currently set to false only in the nestloop join.
    bool _keep_origin = true;
};

template <typename LocalStateType>
class OperatorX : public OperatorXBase {
public:
    OperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
              const DescriptorTbl& descs)
            : OperatorXBase(pool, tnode, operator_id, descs) {}
    OperatorX(ObjectPool* pool, int node_id, int operator_id)
            : OperatorXBase(pool, node_id, operator_id) {};

#ifdef BE_TEST
    OperatorX() = default;
#endif

    ~OperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;
    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_local_state(operator_id())->template cast<LocalState>();
    }

    size_t get_reserve_mem_size(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        auto estimated_size = local_state.estimate_memory_usage();
        if (estimated_size < state->minimum_operator_memory_required_bytes()) {
            estimated_size = state->minimum_operator_memory_required_bytes();
        }
        if (!is_source() && _child) {
            auto child_reserve_size = _child->get_reserve_mem_size(state);
            estimated_size +=
                    std::max(state->minimum_operator_memory_required_bytes(), child_reserve_size);
        }
        return estimated_size;
    }

    void reset_reserve_mem_size(RuntimeState* state) override {
        auto& local_state = get_local_state(state);
        local_state.reset_estimate_memory_usage();

        if (!is_source() && _child) {
            _child->reset_reserve_mem_size(state);
        }
    }
};

/**
 * StreamingOperatorX indicates operators which always processes block in streaming way (one-in-one-out).
 */
template <typename LocalStateType>
class StreamingOperatorX : public OperatorX<LocalStateType> {
public:
    StreamingOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                       const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, operator_id, descs) {}

#ifdef BE_TEST
    StreamingOperatorX() = default;
#endif

    virtual ~StreamingOperatorX() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    virtual Status pull(RuntimeState* state, vectorized::Block* block, bool* eos) = 0;
};

/**
 * StatefulOperatorX indicates the operators with some states inside.
 *
 * Specifically, we called an operator stateful if an operator can determine its output by itself.
 * For example, hash join probe operator is a typical StatefulOperator. When it gets a block from probe side, it will hold this block inside (e.g. _child_block).
 * If there are still remain rows in probe block, we can get output block by calling `get_block` without any data from its child.
 * In a nutshell, it is a one-to-many relation between input blocks and output blocks for StatefulOperator.
 */
template <typename LocalStateType>
class StatefulOperatorX : public OperatorX<LocalStateType> {
public:
    StatefulOperatorX(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                      const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, operator_id, descs) {}
#ifdef BE_TEST
    StatefulOperatorX() = default;
#endif
    virtual ~StatefulOperatorX() = default;

    using OperatorX<LocalStateType>::get_local_state;

    [[nodiscard]] Status get_block(RuntimeState* state, vectorized::Block* block,
                                   bool* eos) override;

    [[nodiscard]] virtual Status pull(RuntimeState* state, vectorized::Block* block,
                                      bool* eos) const = 0;
    [[nodiscard]] virtual Status push(RuntimeState* state, vectorized::Block* input_block,
                                      bool eos) const = 0;
    bool need_more_input_data(RuntimeState* state) const override { return true; }
};

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
class AsyncWriterSink : public PipelineXSinkLocalState<BasicSharedState> {
public:
    using Base = PipelineXSinkLocalState<BasicSharedState>;
    AsyncWriterSink(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _async_writer_dependency(nullptr) {
        _finish_dependency =
                std::make_shared<Dependency>(parent->operator_id(), parent->node_id(),
                                             parent->get_name() + "_FINISH_DEPENDENCY", true);
    }

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* block, bool eos);

    std::vector<Dependency*> dependencies() const override {
        return {_async_writer_dependency.get()};
    }
    Status close(RuntimeState* state, Status exec_status) override;

    Dependency* finishdependency() override { return _finish_dependency.get(); }

protected:
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<Writer> _writer;

    std::shared_ptr<Dependency> _async_writer_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
};

#ifdef BE_TEST
class DummyOperatorLocalState final : public PipelineXLocalState<FakeSharedState> {
public:
    ENABLE_FACTORY_CREATOR(DummyOperatorLocalState);

    DummyOperatorLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<FakeSharedState>(state, parent) {
        _tmp_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                    "DummyOperatorDependency", true);
        _finish_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
        _filter_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
        _spill_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                      "DummyOperatorDependency", true);
    }
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    ~DummyOperatorLocalState() = default;

    std::vector<Dependency*> dependencies() const override { return {_tmp_dependency.get()}; }
    std::vector<Dependency*> execution_dependencies() override {
        return {_filter_dependency.get()};
    }
    Dependency* spill_dependency() const override { return _spill_dependency.get(); }

private:
    std::shared_ptr<Dependency> _tmp_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
    std::shared_ptr<Dependency> _filter_dependency;
};

class DummyOperator final : public OperatorX<DummyOperatorLocalState> {
public:
    DummyOperator() : OperatorX<DummyOperatorLocalState>(nullptr, 0, 0) {}

    [[nodiscard]] bool is_source() const override { return true; }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        *eos = _eos;
        return Status::OK();
    }
    void set_low_memory_mode(RuntimeState* state) override { _low_memory_mode = true; }
    Status terminate(RuntimeState* state) override {
        _terminated = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState* state) const override { return _revocable_mem_size; }
    size_t get_reserve_mem_size(RuntimeState* state) override {
        return _disable_reserve_mem
                       ? 0
                       : OperatorX<DummyOperatorLocalState>::get_reserve_mem_size(state);
    }

private:
    friend class AssertNumRowsLocalState;
    bool _eos = false;
    bool _low_memory_mode = false;
    bool _terminated = false;
    size_t _revocable_mem_size = 0;
    bool _disable_reserve_mem = false;
};

class DummySinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
public:
    using Base = PipelineXSinkLocalState<BasicSharedState>;
    ENABLE_FACTORY_CREATOR(DummySinkLocalState);
    DummySinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {
        _tmp_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                    "DummyOperatorDependency", true);
        _finish_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                       "DummyOperatorDependency", true);
        _spill_dependency = Dependency::create_shared(_parent->operator_id(), _parent->node_id(),
                                                      "DummyOperatorDependency", true);
    }

    std::vector<Dependency*> dependencies() const override { return {_tmp_dependency.get()}; }
    Dependency* finishdependency() override { return _finish_dependency.get(); }
    Dependency* spill_dependency() const override { return _spill_dependency.get(); }
    bool is_finished() const override { return _is_finished; }

private:
    std::shared_ptr<Dependency> _tmp_dependency;
    std::shared_ptr<Dependency> _finish_dependency;
    std::atomic_bool _is_finished = false;
};

class DummySinkOperatorX final : public DataSinkOperatorX<DummySinkLocalState> {
public:
    DummySinkOperatorX(int op_id, int node_id, int dest_id)
            : DataSinkOperatorX<DummySinkLocalState>(op_id, node_id, dest_id) {}
    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override {
        return _return_eof ? Status::Error<ErrorCode::END_OF_FILE>("source have closed")
                           : Status::OK();
    }
    void set_low_memory_mode(RuntimeState* state) override { _low_memory_mode = true; }
    Status terminate(RuntimeState* state) override {
        _terminated = true;
        return Status::OK();
    }
    size_t revocable_mem_size(RuntimeState* state) const override { return _revocable_mem_size; }
    size_t get_reserve_mem_size(RuntimeState* state, bool eos) override {
        return _disable_reserve_mem
                       ? 0
                       : DataSinkOperatorX<DummySinkLocalState>::get_reserve_mem_size(state, eos);
    }

private:
    bool _low_memory_mode = false;
    bool _terminated = false;
    std::atomic_bool _return_eof = false;
    size_t _revocable_mem_size = 0;
    bool _disable_reserve_mem = false;
};
#endif

#include "common/compile_check_end.h"
} // namespace doris::pipeline
