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

#include "common/logging.h"
#include "pipeline/exec/operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "pipeline/pipeline_x/local_exchange/local_exchanger.h"

namespace doris::vectorized {
class AsyncResultWriter;
}
namespace doris::pipeline {

// This struct is used only for initializing local state.
struct LocalStateInfo {
    RuntimeProfile* parent_profile = nullptr;
    const std::vector<TScanRangeParams> scan_ranges;
    BasicSharedState* shared_state;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            le_state_map;
    const int task_idx;
};

// This struct is used only for initializing local sink state.
struct LocalSinkStateInfo {
    const int task_idx;
    RuntimeProfile* parent_profile = nullptr;
    const int sender_id;
    BasicSharedState* shared_state;
    std::map<int, std::pair<std::shared_ptr<LocalExchangeSharedState>, std::shared_ptr<Dependency>>>
            le_state_map;
    const TDataSink& tsink;
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
    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) { return Status::OK(); }
    virtual Status close(RuntimeState* state) = 0;

    // If use projection, we should clear `_origin_block`.
    void clear_origin_block();

    [[nodiscard]] bool reached_limit() const;
    void reached_limit(vectorized::Block* block, bool* eos);
    RuntimeProfile* profile() { return _runtime_profile.get(); }

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    RuntimeProfile::Counter* rows_returned_counter() { return _rows_returned_counter; }
    RuntimeProfile::Counter* blocks_returned_counter() { return _blocks_returned_counter; }
    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    OperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    vectorized::VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] int64_t num_rows_returned() const { return _num_rows_returned; }
    void add_num_rows_returned(int64_t delta) { _num_rows_returned += delta; }
    void set_num_rows_returned(int64_t value) { _num_rows_returned = value; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level = 0) const = 0;

    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in Scan
    virtual Dependency* finishdependency() { return nullptr; }
    //  override in Scan  MultiCastSink
    virtual std::vector<Dependency*> filter_dependencies() { return {}; }

    std::shared_ptr<QueryStatistics> get_query_statistics_ptr() { return _query_statistics; }

protected:
    friend class OperatorXBase;

    ObjectPool* _pool = nullptr;
    int64_t _num_rows_returned {0};

    std::unique_ptr<RuntimeProfile> _runtime_profile;

    // Record this node memory size. it is expected that artificial guarantees are accurate,
    // which will providea reference for operator memory.
    std::unique_ptr<MemTracker> _mem_tracker;

    std::shared_ptr<QueryStatistics> _query_statistics = nullptr;

    RuntimeProfile::Counter* _rows_returned_counter = nullptr;
    RuntimeProfile::Counter* _blocks_returned_counter = nullptr;
    RuntimeProfile::Counter* _wait_for_dependency_timer = nullptr;
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _projection_timer = nullptr;
    RuntimeProfile::Counter* _exec_timer = nullptr;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _peak_memory_usage_counter = nullptr;
    RuntimeProfile::Counter* _init_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;

    OperatorXBase* _parent = nullptr;
    RuntimeState* _state = nullptr;
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<vectorized::VExprContextSPtrs> _intermediate_projections;

    bool _closed = false;
    vectorized::Block _origin_block;
};

class OperatorXBase : public OperatorBase {
public:
    OperatorXBase(ObjectPool* pool, const TPlanNode& tnode, const int operator_id,
                  const DescriptorTbl& descs)
            : OperatorBase(nullptr),
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
        }
        if (tnode.__isset.output_tuple_id) {
            _output_row_descriptor = std::make_unique<RowDescriptor>(
                    descs, std::vector {tnode.output_tuple_id}, std::vector {true});
        }
        if (!tnode.intermediate_output_tuple_id_list.empty()) {
            DCHECK(tnode.__isset.output_tuple_id) << " no final output tuple id";
            // common subexpression elimination
            DCHECK_EQ(tnode.intermediate_output_tuple_id_list.size(),
                      tnode.intermediate_projections_list.size());
            _intermediate_output_row_descriptor.reserve(
                    tnode.intermediate_output_tuple_id_list.size());
            for (auto output_tuple_id : tnode.intermediate_output_tuple_id_list) {
                _intermediate_output_row_descriptor.push_back(
                        RowDescriptor(descs, std::vector {output_tuple_id}, std::vector {true}));
            }
        }
    }

    OperatorXBase(ObjectPool* pool, int node_id, int operator_id)
            : OperatorBase(nullptr),
              _operator_id(operator_id),
              _node_id(node_id),
              _pool(pool),
              _limit(-1) {}
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);
    Status init(const TDataSink& tsink) override {
        LOG(FATAL) << "should not reach here!";
        return Status::OK();
    }
    virtual Status init(ExchangeType type) {
        LOG(FATAL) << "should not reach here!";
        return Status::OK();
    }
    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Runtime Profile is not owned by operator");
        return nullptr;
    }
    [[noreturn]] virtual const std::vector<TRuntimeFilterDesc>& runtime_filter_descs() {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, _op_name);
    }
    [[nodiscard]] std::string get_name() const override { return _op_name; }
    [[nodiscard]] virtual DataDistribution required_data_distribution() const {
        return _child_x && _child_x->ignore_data_distribution() && !is_source()
                       ? DataDistribution(ExchangeType::PASSTHROUGH)
                       : DataDistribution(ExchangeType::NOOP);
    }
    [[nodiscard]] virtual bool need_data_from_children(RuntimeState* state) const {
        return is_source() ? true : _child_x == nullptr || _child_x->need_data_from_children(state);
    }
    [[nodiscard]] virtual bool ignore_data_distribution() const {
        return _child_x ? _child_x->ignore_data_distribution() : _ignore_data_distribution;
    }
    [[nodiscard]] bool ignore_data_hash_distribution() const {
        return _child_x ? _child_x->ignore_data_hash_distribution() : _ignore_data_distribution;
    }
    void set_ignore_data_distribution() { _ignore_data_distribution = true; }

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        LOG(FATAL) << "should not be called in pipelineX";
        return Status::OK();
    }

    [[nodiscard]] virtual Status get_block(RuntimeState* state, vectorized::Block* block,
                                           bool* eos) = 0;

    [[nodiscard]] bool can_terminate_early() override { return false; }

    [[nodiscard]] virtual bool can_terminate_early(RuntimeState* state) { return false; }

    [[nodiscard]] virtual bool is_shuffled_hash_join() const { return false; }

    bool can_read() override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    bool can_write() override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    [[nodiscard]] bool is_pending_finish() const override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override {
        LOG(FATAL) << "should not reach here!";
        return Status::OK();
    }

    bool runtime_filters_are_ready_or_timeout() override {
        LOG(FATAL) << "should not reach here!";
        return true;
    }

    Status close(RuntimeState* state) override;

    [[nodiscard]] virtual const RowDescriptor& intermediate_row_desc() const {
        return _row_descriptor;
    }

    //  input expr -> intermediate_projections[0] -> intermediate_projections[1] -> intermediate_projections[2]    ... ->     final projections         ->         output expr
    //  prepare        _row_descriptor          intermediate_row_desc[0]             intermediate_row_desc[1]            intermediate_row_desc.end()          _output_row_descriptor

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

    [[nodiscard]] std::string debug_string() const override { return ""; }

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

    [[nodiscard]] OperatorXPtr get_child() { return _child_x; }

    [[nodiscard]] vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    [[nodiscard]] virtual RowDescriptor& row_descriptor() { return _row_descriptor; }

    [[nodiscard]] int id() const override { return node_id(); }
    [[nodiscard]] int operator_id() const { return _operator_id; }
    [[nodiscard]] int node_id() const { return _node_id; }

    [[nodiscard]] int64_t limit() const { return _limit; }

    [[nodiscard]] const RowDescriptor& row_desc() const override {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }

    [[nodiscard]] const RowDescriptor* output_row_descriptor() {
        return _output_row_descriptor.get();
    }

    bool has_output_row_desc() const { return _output_row_descriptor != nullptr; }

    [[nodiscard]] bool is_source() const override { return false; }

    [[nodiscard]] virtual Status get_block_after_projects(RuntimeState* state,
                                                          vectorized::Block* block, bool* eos);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    Status do_projections(RuntimeState* state, vectorized::Block* origin_block,
                          vectorized::Block* output_block) const;
    void set_parallel_tasks(int parallel_tasks) { _parallel_tasks = parallel_tasks; }
    int parallel_tasks() const { return _parallel_tasks; }

protected:
    template <typename Dependency>
    friend class PipelineXLocalState;
    friend class PipelineXLocalStateBase;
    friend class VScanner;
    const int _operator_id;
    const int _node_id; // unique w/in single plan tree
    TPlanNodeType::type _type;
    ObjectPool* _pool = nullptr;
    std::vector<TupleId> _tuple_ids;

    vectorized::VExprContextSPtrs _conjuncts;

    RowDescriptor _row_descriptor;

    std::unique_ptr<RowDescriptor> _output_row_descriptor = nullptr;
    vectorized::VExprContextSPtrs _projections;

    std::vector<RowDescriptor> _intermediate_output_row_descriptor;
    // Used in common subexpression elimination to compute intermediate results.
    std::vector<vectorized::VExprContextSPtrs> _intermediate_projections;

    /// Resource information sent from the frontend.
    const TBackendResourceProfile _resource_profile;

    int64_t _limit; // -1: no limit

    std::string _op_name;
    bool _ignore_data_distribution = false;
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
    ~OperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;
    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_local_state(operator_id())->template cast<LocalState>();
    }
};

template <typename SharedStateArg = FakeSharedState>
class PipelineXLocalState : public PipelineXLocalStateBase {
public:
    using SharedStateType = SharedStateArg;
    PipelineXLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalStateBase(state, parent) {}
    ~PipelineXLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override;

    virtual std::string name_suffix() const {
        return " (id=" + std::to_string(_parent->node_id()) + ")";
    }

    Status close(RuntimeState* state) override;

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }

    void inc_running_big_mem_op_num(RuntimeState* state) {
        if (!_big_mem_op_num_added) {
            state->get_query_ctx()->inc_running_big_mem_op_num();
            _big_mem_op_num_added = true;
        }
    }

    void dec_running_big_mem_op_num(RuntimeState* state) {
        if (_big_mem_op_num_added && !_big_mem_op_num_deced) {
            state->get_query_ctx()->dec_running_big_mem_op_num();
            _big_mem_op_num_deced = true;
        }
    }

protected:
    Dependency* _dependency = nullptr;
    SharedStateArg* _shared_state = nullptr;

private:
    bool _big_mem_op_num_added = false;
    bool _big_mem_op_num_deced = false;
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
        _spill_counters = ADD_LABEL_COUNTER_WITH_LEVEL(Base::profile(), "Spill", 1);
        _spill_recover_time =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillRecoverTime", "Spill", 1);
        _spill_read_data_time =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillReadDataTime", "Spill", 1);
        _spill_deserialize_time =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillDeserializeTime", "Spill", 1);
        _spill_read_bytes = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillReadDataSize",
                                                         TUnit::BYTES, "Spill", 1);
        _spill_wait_in_queue_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWaitInQueueTime", "Spill", 1);
        _spill_write_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWriteWaitIOTime", "Spill", 1);
        _spill_read_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillReadWaitIOTime", "Spill", 1);
        return Status::OK();
    }

    RuntimeProfile::Counter* _spill_counters = nullptr;
    RuntimeProfile::Counter* _spill_recover_time;
    RuntimeProfile::Counter* _spill_read_data_time;
    RuntimeProfile::Counter* _spill_deserialize_time;
    RuntimeProfile::Counter* _spill_read_bytes;
    RuntimeProfile::Counter* _spill_write_wait_io_timer = nullptr;
    RuntimeProfile::Counter* _spill_read_wait_io_timer = nullptr;
    RuntimeProfile::Counter* _spill_wait_in_queue_timer = nullptr;
};

class DataSinkOperatorXBase;

class PipelineXSinkLocalStateBase {
public:
    PipelineXSinkLocalStateBase(DataSinkOperatorXBase* parent_, RuntimeState* state_);
    virtual ~PipelineXSinkLocalStateBase() = default;

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state, Status exec_status) = 0;

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
    RuntimeProfile* profile() { return _profile; }
    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    [[nodiscard]] RuntimeProfile* faker_runtime_profile() const {
        return _faker_runtime_profile.get();
    }

    RuntimeProfile::Counter* rows_input_counter() { return _rows_input_counter; }
    RuntimeProfile::Counter* exec_time_counter() { return _exec_timer; }
    virtual std::vector<Dependency*> dependencies() const { return {nullptr}; }

    // override in exchange sink , AsyncWriterSink
    virtual Dependency* finishdependency() { return nullptr; }

    std::shared_ptr<QueryStatistics> get_query_statistics_ptr() { return _query_statistics; }

protected:
    DataSinkOperatorXBase* _parent = nullptr;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    std::unique_ptr<MemTracker> _mem_tracker;
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed = false;
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
    RuntimeProfile::Counter* _memory_used_counter = nullptr;
    RuntimeProfile::Counter* _peak_memory_usage_counter = nullptr;

    std::shared_ptr<QueryStatistics> _query_statistics = nullptr;
};

class DataSinkOperatorXBase : public OperatorBase {
public:
    DataSinkOperatorXBase(const int operator_id, const int node_id)
            : OperatorBase(nullptr), _operator_id(operator_id), _node_id(node_id), _dests_id({1}) {}

    DataSinkOperatorXBase(const int operator_id, const int node_id, const int dest_id)
            : OperatorBase(nullptr),
              _operator_id(operator_id),
              _node_id(node_id),
              _dests_id({dest_id}) {}

    DataSinkOperatorXBase(const int operator_id, const int node_id, std::vector<int>& sources)
            : OperatorBase(nullptr),
              _operator_id(operator_id),
              _node_id(node_id),
              _dests_id(sources) {}

    ~DataSinkOperatorXBase() override = default;

    // For agg/sort/join sink.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);

    Status init(const TDataSink& tsink) override;
    [[nodiscard]] virtual Status init(ExchangeType type, const int num_buckets,
                                      const bool is_shuffled_hash_join,
                                      const std::map<int, int>& shuffle_idx_to_instance_idx) {
        return Status::InternalError("init() is only implemented in local exchange!");
    }

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override {
        LOG(FATAL) << "should not reach here!";
        return Status::OK();
    }

    [[nodiscard]] virtual Status sink(RuntimeState* state, vectorized::Block* block, bool eos) = 0;

    [[nodiscard]] virtual Status setup_local_state(RuntimeState* state,
                                                   LocalSinkStateInfo& info) = 0;

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
    [[nodiscard]] virtual DataDistribution required_data_distribution() const {
        return _child_x && _child_x->ignore_data_distribution()
                       ? DataDistribution(ExchangeType::PASSTHROUGH)
                       : DataDistribution(ExchangeType::NOOP);
    }

    [[nodiscard]] virtual bool is_shuffled_hash_join() const { return false; }

    Status close(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    bool can_read() override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    bool can_write() override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    [[nodiscard]] bool is_pending_finish() const override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    [[nodiscard]] std::string debug_string() const override { return ""; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const;

    [[nodiscard]] virtual std::string debug_string(RuntimeState* state,
                                                   int indentation_level) const;

    [[nodiscard]] bool is_sink() const override { return true; }

    [[nodiscard]] bool is_source() const override { return false; }

    static Status close(RuntimeState* state, Status exec_status) {
        auto result = state->get_sink_local_state_result();
        if (!result) {
            return result.error();
        }
        return result.value()->close(state, exec_status);
    }

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Runtime Profile is not owned by operator");
        return nullptr;
    }

    [[nodiscard]] int id() const override { return node_id(); }

    [[nodiscard]] int operator_id() const { return _operator_id; }

    [[nodiscard]] const std::vector<int>& dests_id() const { return _dests_id; }

    void set_dests_id(const std::vector<int>& dest_id) { _dests_id = dest_id; }

    [[nodiscard]] int node_id() const { return _node_id; }

    [[nodiscard]] std::string get_name() const override { return _name; }

    virtual bool should_dry_run(RuntimeState* state) { return false; }

protected:
    template <typename Writer, typename Parent>
        requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
    friend class AsyncWriterSink;
    // _operator_id : the current Operator's ID, which is not visible to the user.
    // _node_id : the plan node ID corresponding to the Operator, which is visible on the profile.
    // _dests_id : the target _operator_id of the sink, for example, in the case of a multi-sink, there are multiple targets.
    const int _operator_id;
    const int _node_id;
    std::vector<int> _dests_id;
    std::string _name;

    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;
};

template <typename LocalStateType>
class DataSinkOperatorX : public DataSinkOperatorXBase {
public:
    DataSinkOperatorX(int operator_id, const int node_id)
            : DataSinkOperatorXBase(operator_id, node_id) {}

    DataSinkOperatorX(const int id, const int node_id, const int source_id)
            : DataSinkOperatorXBase(id, node_id, source_id) {}

    DataSinkOperatorX(const int id, const int node_id, std::vector<int> sources)
            : DataSinkOperatorXBase(id, node_id, sources) {}
    ~DataSinkOperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;
    std::shared_ptr<BasicSharedState> create_shared_state() const override;

    using LocalState = LocalStateType;
    [[nodiscard]] LocalState& get_local_state(RuntimeState* state) const {
        return state->get_sink_local_state()->template cast<LocalState>();
    }
};

template <typename SharedStateArg = FakeSharedState>
class PipelineXSinkLocalState : public PipelineXSinkLocalStateBase {
public:
    using SharedStateType = SharedStateArg;
    PipelineXSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalStateBase(parent, state) {}
    ~PipelineXSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status open(RuntimeState* state) override { return Status::OK(); }

    Status close(RuntimeState* state, Status exec_status) override;

    [[nodiscard]] std::string debug_string(int indentation_level) const override;

    virtual std::string name_suffix() { return " (id=" + std::to_string(_parent->node_id()) + ")"; }

    std::vector<Dependency*> dependencies() const override {
        return _dependency ? std::vector<Dependency*> {_dependency} : std::vector<Dependency*> {};
    }

    void inc_running_big_mem_op_num(RuntimeState* state) {
        if (!_big_mem_op_num_added) {
            state->get_query_ctx()->inc_running_big_mem_op_num();
            _big_mem_op_num_added = true;
        }
    }

    void dec_running_big_mem_op_num(RuntimeState* state) {
        if (_big_mem_op_num_added && !_big_mem_op_num_deced) {
            state->get_query_ctx()->dec_running_big_mem_op_num();
            _big_mem_op_num_deced = true;
        }
    }

protected:
    Dependency* _dependency = nullptr;
    SharedStateType* _shared_state = nullptr;

private:
    bool _big_mem_op_num_added = false;
    bool _big_mem_op_num_deced = false;
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

        _spill_counters = ADD_LABEL_COUNTER_WITH_LEVEL(Base::profile(), "Spill", 1);
        _spill_timer = ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillTime", "Spill", 1);
        _spill_serialize_block_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillSerializeBlockTime", "Spill", 1);
        _spill_write_disk_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWriteDiskTime", "Spill", 1);
        _spill_data_size = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillWriteDataSize",
                                                        TUnit::BYTES, "Spill", 1);
        _spill_block_count = ADD_CHILD_COUNTER_WITH_LEVEL(Base::profile(), "SpillWriteBlockCount",
                                                          TUnit::UNIT, "Spill", 1);
        _spill_wait_in_queue_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWaitInQueueTime", "Spill", 1);
        _spill_write_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillWriteWaitIOTime", "Spill", 1);
        _spill_read_wait_io_timer =
                ADD_CHILD_TIMER_WITH_LEVEL(Base::profile(), "SpillReadWaitIOTime", "Spill", 1);
        return Status::OK();
    }

    RuntimeProfile::Counter* _spill_counters = nullptr;
    RuntimeProfile::Counter* _spill_timer = nullptr;
    RuntimeProfile::Counter* _spill_serialize_block_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_disk_timer = nullptr;
    RuntimeProfile::Counter* _spill_data_size = nullptr;
    RuntimeProfile::Counter* _spill_block_count = nullptr;
    RuntimeProfile::Counter* _spill_wait_in_queue_timer = nullptr;
    RuntimeProfile::Counter* _spill_write_wait_io_timer = nullptr;
    RuntimeProfile::Counter* _spill_read_wait_io_timer = nullptr;
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
    virtual ~StatefulOperatorX() = default;

    using OperatorX<LocalStateType>::get_local_state;

    [[nodiscard]] Status get_block(RuntimeState* state, vectorized::Block* block,
                                   bool* eos) override;

    [[nodiscard]] virtual Status pull(RuntimeState* state, vectorized::Block* block,
                                      bool* eos) const = 0;
    [[nodiscard]] virtual Status push(RuntimeState* state, vectorized::Block* input_block,
                                      bool eos) const = 0;

    [[nodiscard]] virtual bool need_more_input_data(RuntimeState* state) const = 0;

    bool need_data_from_children(RuntimeState* state) const override {
        if (need_more_input_data(state)) {
            return OperatorX<LocalStateType>::_child_x->need_data_from_children(state);
        } else {
            return false;
        }
    }
};

template <typename Writer, typename Parent>
    requires(std::is_base_of_v<vectorized::AsyncResultWriter, Writer>)
class AsyncWriterSink : public PipelineXSinkLocalState<FakeSharedState> {
public:
    using Base = PipelineXSinkLocalState<FakeSharedState>;
    AsyncWriterSink(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _async_writer_dependency(nullptr) {
        _finish_dependency = std::make_shared<FinishDependency>(
                parent->operator_id(), parent->node_id(), parent->get_name() + "_FINISH_DEPENDENCY",
                state->get_query_ctx());
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

    std::shared_ptr<AsyncWriterDependency> _async_writer_dependency;

    std::shared_ptr<Dependency> _finish_dependency;
};

} // namespace doris::pipeline
