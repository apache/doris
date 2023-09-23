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

#include "pipeline/exec/operator.h"

namespace doris::pipeline {

#define CREATE_LOCAL_STATE_RETURN_IF_ERROR(local_state)                                 \
    auto _sptr = state->get_local_state(id());                                          \
    if (!_sptr) return Status::InternalError("could not find local state id {}", id()); \
    auto& local_state = _sptr->template cast<LocalState>();

#define CREATE_SINK_LOCAL_STATE_RETURN_IF_ERROR(local_state)                            \
    auto _sptr = state->get_sink_local_state(id());                                     \
    if (!_sptr) return Status::InternalError("could not find local state id {}", id()); \
    auto& local_state = _sptr->template cast<LocalState>();

#define CREATE_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state) \
    auto _sptr = state->get_local_state(id());               \
    if (!_sptr) return nullptr;                              \
    auto& local_state = _sptr->template cast<LocalState>();

#define CREATE_SINK_LOCAL_STATE_RETURN_NULL_IF_ERROR(local_state) \
    auto _sptr = state->get_sink_local_state(id());               \
    if (!_sptr) return nullptr;                                   \
    auto& local_state = _sptr->template cast<LocalState>();

// This struct is used only for initializing local state.
struct LocalStateInfo {
    RuntimeProfile* parent_profile;
    const std::vector<TScanRangeParams> scan_ranges;
    Dependency* dependency;
};

// This struct is used only for initializing local sink state.
struct LocalSinkStateInfo {
    RuntimeProfile* parent_profile;
    const int sender_id;
    Dependency* dependency;
    const TDataSink& tsink;
};

class PipelineXLocalStateBase {
public:
    PipelineXLocalStateBase(RuntimeState* state, OperatorXBase* parent)
            : _num_rows_returned(0),
              _rows_returned_counter(nullptr),
              _rows_returned_rate(nullptr),
              _memory_used_counter(nullptr),
              _peak_memory_usage_counter(nullptr),
              _parent(parent),
              _state(state) {}
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
    void reached_limit(vectorized::Block* block, SourceState& source_state);
    RuntimeProfile* profile() { return _runtime_profile.get(); }

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    RuntimeProfile::Counter* rows_returned_counter() { return _rows_returned_counter; }
    RuntimeProfile::Counter* rows_returned_rate() { return _rows_returned_rate; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }
    RuntimeProfile::Counter* projection_timer() { return _projection_timer; }
    RuntimeProfile::Counter* wait_for_dependency_timer() { return _wait_for_dependency_timer; }
    RuntimeProfile::Counter* blocks_returned_counter() { return _rows_returned_counter; }

    OperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    vectorized::VExprContextSPtrs& projections() { return _projections; }
    [[nodiscard]] int64_t num_rows_returned() const { return _num_rows_returned; }
    void add_num_rows_returned(int64_t delta) { _num_rows_returned += delta; }
    void set_num_rows_returned(int64_t value) { _num_rows_returned = value; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level = 0) const;

protected:
    friend class OperatorXBase;

    ObjectPool* _pool;
    int64_t _num_rows_returned;

    std::unique_ptr<RuntimeProfile> _runtime_profile;

    // Record this node memory size. it is expected that artificial guarantees are accurate,
    // which will providea reference for operator memory.
    std::unique_ptr<MemTracker> _mem_tracker;

    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _blocks_returned_counter;
    RuntimeProfile::Counter* _rows_returned_rate;
    RuntimeProfile::Counter* _wait_for_dependency_timer;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _memory_used_counter;
    RuntimeProfile::Counter* _projection_timer;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _peak_memory_usage_counter;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;

    OpentelemetrySpan _span;
    OperatorXBase* _parent;
    RuntimeState* _state;
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    bool _closed = false;
    vectorized::Block _origin_block;
};

class OperatorXBase : public OperatorBase {
public:
    OperatorXBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : OperatorBase(nullptr),
              _id(tnode.node_id),
              _type(tnode.node_type),
              _pool(pool),
              _tuple_ids(tnode.row_tuples),
              _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
              _resource_profile(tnode.resource_profile),
              _limit(tnode.limit) {
        if (tnode.__isset.output_tuple_id) {
            _output_row_descriptor.reset(new RowDescriptor(descs, {tnode.output_tuple_id}, {true}));
        }
    }

    OperatorXBase(ObjectPool* pool, int id) : OperatorBase(nullptr), _id(id), _pool(pool) {};
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);
    Status init(const TDataSink& tsink) override {
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

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status finalize(RuntimeState* state) override { return Status::OK(); }

    [[nodiscard]] bool can_terminate_early() override { return false; }

    [[nodiscard]] virtual bool can_terminate_early(RuntimeState* state) { return false; }

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

    virtual bool runtime_filters_are_ready_or_timeout(RuntimeState* state) const { return true; }

    Status close(RuntimeState* state) override;

    virtual Dependency* wait_for_dependency(RuntimeState* state) { return nullptr; }

    virtual bool is_pending_finish(RuntimeState* state) const { return false; }

    [[nodiscard]] virtual const RowDescriptor& intermediate_row_desc() const {
        return _row_descriptor;
    }

    [[nodiscard]] std::string debug_string() const override { return ""; }

    virtual std::string debug_string(int indentation_level = 0) const;

    virtual std::string debug_string(RuntimeState* state, int indentation_level = 0) const;

    virtual Status setup_local_state(RuntimeState* state, LocalStateInfo& info) = 0;

    virtual Status setup_local_states(RuntimeState* state, std::vector<LocalStateInfo>& infos) = 0;

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
    [[nodiscard]] RowDescriptor& row_descriptor() { return _row_descriptor; }

    [[nodiscard]] int id() const override { return _id; }

    [[nodiscard]] int64_t limit() const { return _limit; }

    [[nodiscard]] const RowDescriptor& row_desc() override {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }

    [[nodiscard]] bool is_source() const override { return false; }

    Status get_next_after_projects(RuntimeState* state, vectorized::Block* block,
                                   SourceState& source_state);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    Status do_projections(RuntimeState* state, vectorized::Block* origin_block,
                          vectorized::Block* output_block) const;

protected:
    template <typename Dependency>
    friend class PipelineXLocalState;
    friend class PipelineXLocalStateBase;
    int _id; // unique w/in single plan tree
    TPlanNodeType::type _type;
    ObjectPool* _pool;
    std::vector<TupleId> _tuple_ids;

    vectorized::VExprContextSPtrs _conjuncts;

    RowDescriptor _row_descriptor;

    std::unique_ptr<RowDescriptor> _output_row_descriptor;
    vectorized::VExprContextSPtrs _projections;

    /// Resource information sent from the frontend.
    const TBackendResourceProfile _resource_profile;

    int64_t _limit; // -1: no limit

    std::string _op_name;
};

template <typename LocalStateType>
class OperatorX : public OperatorXBase {
public:
    OperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : OperatorXBase(pool, tnode, descs) {}
    OperatorX(ObjectPool* pool, int id) : OperatorXBase(pool, id) {};
    ~OperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override;
    Status setup_local_states(RuntimeState* state, std::vector<LocalStateInfo>& info) override;
    using LocalState = LocalStateType;
};

template <typename DependencyType = FakeDependency>
class PipelineXLocalState : public PipelineXLocalStateBase {
public:
    PipelineXLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalStateBase(state, parent) {}
    ~PipelineXLocalState() override = default;

    Status init(RuntimeState* state, LocalStateInfo& info) override {
        _runtime_profile.reset(new RuntimeProfile(_parent->get_name() +
                                                  " (id=" + std::to_string(_parent->id()) + ")"));
        _runtime_profile->set_metadata(_parent->id());
        info.parent_profile->add_child(_runtime_profile.get(), true, nullptr);
        if constexpr (!std::is_same_v<FakeDependency, Dependency>) {
            _dependency = (DependencyType*)info.dependency;
            if (_dependency) {
                _shared_state = (typename DependencyType::SharedState*)_dependency->shared_state();
                _wait_for_dependency_timer = ADD_TIMER(
                        _runtime_profile, "WaitForDependency[" + _dependency->name() + "]Time");
            }
        }

        _conjuncts.resize(_parent->_conjuncts.size());
        _projections.resize(_parent->_projections.size());
        for (size_t i = 0; i < _conjuncts.size(); i++) {
            RETURN_IF_ERROR(_parent->_conjuncts[i]->clone(state, _conjuncts[i]));
        }
        for (size_t i = 0; i < _projections.size(); i++) {
            RETURN_IF_ERROR(_parent->_projections[i]->clone(state, _projections[i]));
        }
        _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
        _blocks_returned_counter = ADD_COUNTER(_runtime_profile, "BlocksReturned", TUnit::UNIT);
        _projection_timer = ADD_TIMER(_runtime_profile, "ProjectionTime");
        _open_timer = ADD_TIMER(_runtime_profile, "OpenTime");
        _close_timer = ADD_TIMER(_runtime_profile, "CloseTime");
        _rows_returned_rate = profile()->add_derived_counter(
                doris::ExecNode::ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
                std::bind<int64_t>(&RuntimeProfile::units_per_second, _rows_returned_counter,
                                   profile()->total_time_counter()),
                "");
        _mem_tracker =
                std::make_unique<MemTracker>("PipelineXLocalState:" + _runtime_profile->name());
        _memory_used_counter = ADD_LABEL_COUNTER(_runtime_profile, "MemoryUsage");
        _peak_memory_usage_counter = _runtime_profile->AddHighWaterMarkCounter(
                "PeakMemoryUsage", TUnit::BYTES, "MemoryUsage");
        return Status::OK();
    }

    Status close(RuntimeState* state) override {
        if (_closed) {
            return Status::OK();
        }
        if (_dependency) {
            COUNTER_SET(_wait_for_dependency_timer, _dependency->read_watcher_elapse_time());
        }
        if (_rows_returned_counter != nullptr) {
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        }
        profile()->add_to_span(_span);
        _closed = true;
        return Status::OK();
    }

    [[nodiscard]] std::string debug_string(int indentation_level = 0) const override;

protected:
    DependencyType* _dependency;
    typename DependencyType::SharedState* _shared_state;
};

class DataSinkOperatorXBase;

class PipelineXSinkLocalStateBase {
public:
    PipelineXSinkLocalStateBase(DataSinkOperatorXBase* parent_, RuntimeState* state_)
            : _parent(parent_), _state(state_) {}
    virtual ~PipelineXSinkLocalStateBase() = default;

    // Do initialization. This step should be executed only once and in bthread, so we can do some
    // lightweight or non-idempotent operations (e.g. init profile, clone expr ctx from operatorX)
    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    // Do initialization. This step can be executed multiple times, so we should make sure it is
    // idempotent (e.g. wait for runtime filters).
    virtual Status open(RuntimeState* state) = 0;
    virtual Status close(RuntimeState* state, Status exec_status) = 0;
    virtual Status try_close(RuntimeState* state, Status exec_status) = 0;

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const;

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
    QueryStatistics* query_statistics() { return _query_statistics.get(); }
    [[nodiscard]] RuntimeProfile* faker_runtime_profile() const {
        return _faker_runtime_profile.get();
    }

    RuntimeProfile::Counter* rows_input_counter() { return _rows_input_counter; }

protected:
    DataSinkOperatorXBase* _parent;
    RuntimeState* _state;
    RuntimeProfile* _profile;
    std::unique_ptr<MemTracker> _mem_tracker;
    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed = false;
    //NOTICE: now add a faker profile, because sometimes the profile record is useless
    //so we want remove some counters and timers, eg: in join node, if it's broadcast_join
    //and shared hash table, some counter/timer about build hash table is useless,
    //so we could add those counter/timer in faker profile, and those will not display in web profile.
    std::unique_ptr<RuntimeProfile> _faker_runtime_profile =
            std::make_unique<RuntimeProfile>("faker profile");

    RuntimeProfile::Counter* _rows_input_counter;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _wait_for_dependency_timer;
};

class DataSinkOperatorXBase : public OperatorBase {
public:
    DataSinkOperatorXBase(const int id) : OperatorBase(nullptr), _id(id), _dests_id({id}) {}

    DataSinkOperatorXBase(const int id, const int dest_id)
            : OperatorBase(nullptr), _id(id), _dests_id({dest_id}) {}

    DataSinkOperatorXBase(const int id, std::vector<int>& sources)
            : OperatorBase(nullptr), _id(id), _dests_id(sources) {}

    ~DataSinkOperatorXBase() override = default;

    // For agg/sort/join sink.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state);

    Status init(const TDataSink& tsink) override;

    Status prepare(RuntimeState* state) override { return Status::OK(); }
    Status open(RuntimeState* state) override { return Status::OK(); }

    virtual Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    virtual Status setup_local_states(RuntimeState* state,
                                      std::vector<LocalSinkStateInfo>& infos) = 0;

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

    virtual void get_dependency(std::vector<DependencySPtr>& dependency) = 0;

    Status close(RuntimeState* state) override {
        return Status::InternalError("Should not reach here!");
    }

    Status try_close(RuntimeState* state) override {
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

    virtual WriteDependency* wait_for_dependency(RuntimeState* state) { return nullptr; }

    virtual bool is_pending_finish(RuntimeState* state) const { return false; }

    [[nodiscard]] std::string debug_string() const override { return ""; }

    [[nodiscard]] virtual std::string debug_string(int indentation_level) const;

    [[nodiscard]] virtual std::string debug_string(RuntimeState* state,
                                                   int indentation_level) const;

    [[nodiscard]] bool is_sink() const override { return true; }

    [[nodiscard]] bool is_source() const override { return false; }

    virtual Status close(RuntimeState* state, Status exec_status) {
        return state->get_sink_local_state(id())->close(state, exec_status);
    }

    [[nodiscard]] virtual Status try_close(RuntimeState* state, Status exec_status) {
        return state->get_sink_local_state(id())->try_close(state, exec_status);
    }

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Runtime Profile is not owned by operator");
        return nullptr;
    }

    [[nodiscard]] int id() const override { return _id; }

    [[nodiscard]] const std::vector<int>& dests_id() const { return _dests_id; }

    [[nodiscard]] std::string get_name() const override { return _name; }

    Status finalize(RuntimeState* state) override { return Status::OK(); }

    virtual bool should_dry_run(RuntimeState* state) { return false; }

protected:
    template <typename Writer, typename Parent>
    friend class AsyncWriterSink;

    const int _id;

    std::vector<int> _dests_id;
    std::string _name;

    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;

    OpentelemetrySpan _span {};
};

template <typename LocalStateType>
class DataSinkOperatorX : public DataSinkOperatorXBase {
public:
    DataSinkOperatorX(const int id) : DataSinkOperatorXBase(id) {}

    DataSinkOperatorX(const int id, const int source_id) : DataSinkOperatorXBase(id, source_id) {}

    DataSinkOperatorX(const int id, std::vector<int> sources)
            : DataSinkOperatorXBase(id, sources) {}
    ~DataSinkOperatorX() override = default;

    Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) override;

    Status setup_local_states(RuntimeState* state, std::vector<LocalSinkStateInfo>& infos) override;
    void get_dependency(std::vector<DependencySPtr>& dependency) override;

    using LocalState = LocalStateType;
};

template <typename DependencyType = FakeDependency>
class PipelineXSinkLocalState : public PipelineXSinkLocalStateBase {
public:
    using Dependency = DependencyType;
    PipelineXSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalStateBase(parent, state) {}
    ~PipelineXSinkLocalState() override = default;

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        // create profile
        _profile = state->obj_pool()->add(new RuntimeProfile(
                _parent->get_name() + " (id=" + std::to_string(_parent->id()) + ")"));
        if constexpr (!std::is_same_v<FakeDependency, Dependency>) {
            _dependency = (DependencyType*)info.dependency;
            if (_dependency) {
                _shared_state = (typename DependencyType::SharedState*)_dependency->shared_state();
                _wait_for_dependency_timer =
                        ADD_TIMER(_profile, "WaitForDependency[" + _dependency->name() + "]Time");
            }
        }
        _rows_input_counter = ADD_COUNTER(_profile, "InputRows", TUnit::UNIT);
        _open_timer = ADD_TIMER(_profile, "OpenTime");
        _close_timer = ADD_TIMER(_profile, "CloseTime");
        info.parent_profile->add_child(_profile, true, nullptr);
        _mem_tracker = std::make_unique<MemTracker>(_parent->get_name());
        return Status::OK();
    }

    Status open(RuntimeState* state) override { return Status::OK(); }

    Status try_close(RuntimeState* state, Status exec_status) override { return Status::OK(); }

    Status close(RuntimeState* state, Status exec_status) override {
        if (_closed) {
            return Status::OK();
        }
        if (_dependency) {
            COUNTER_SET(_wait_for_dependency_timer, _dependency->write_watcher_elapse_time());
        }
        _closed = true;
        return Status::OK();
    }

    [[nodiscard]] std::string debug_string(int indentation_level) const override;
    typename DependencyType::SharedState*& get_shared_state() { return _shared_state; }

protected:
    DependencyType* _dependency = nullptr;
    typename DependencyType::SharedState* _shared_state;
};

/**
 * StreamingOperatorX indicates operators which always processes block in streaming way (one-in-one-out).
 */
template <typename LocalStateType>
class StreamingOperatorX : public OperatorX<LocalStateType> {
public:
    StreamingOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, descs) {}
    virtual ~StreamingOperatorX() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override;

    virtual Status pull(RuntimeState* state, vectorized::Block* block,
                        SourceState& source_state) = 0;
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
    StatefulOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : OperatorX<LocalStateType>(pool, tnode, descs) {}
    virtual ~StatefulOperatorX() = default;

    [[nodiscard]] Status get_block(RuntimeState* state, vectorized::Block* block,
                                   SourceState& source_state) override;

    [[nodiscard]] virtual Status pull(RuntimeState* state, vectorized::Block* block,
                                      SourceState& source_state) const = 0;
    [[nodiscard]] virtual Status push(RuntimeState* state, vectorized::Block* input_block,
                                      SourceState source_state) const = 0;
    [[nodiscard]] virtual bool need_more_input_data(RuntimeState* state) const = 0;
};

template <typename Writer, typename Parent>
class AsyncWriterSink : public PipelineXSinkLocalState<> {
public:
    AsyncWriterSink(DataSinkOperatorXBase* parent, RuntimeState* state)
            : PipelineXSinkLocalState<>(parent, state), _async_writer_dependency(nullptr) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXSinkLocalState<>::init(state, info));
        _output_vexpr_ctxs.resize(_parent->cast<Parent>()._output_vexpr_ctxs.size());
        for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
            RETURN_IF_ERROR(_parent->cast<Parent>()._output_vexpr_ctxs[i]->clone(
                    state, _output_vexpr_ctxs[i]));
        }

        _writer.reset(new Writer(info.tsink, _output_vexpr_ctxs));
        _async_writer_dependency = AsyncWriterDependency::create_shared(_parent->id());
        _writer->set_dependency(_async_writer_dependency.get());

        _wait_for_dependency_timer = ADD_TIMER(
                _profile, "WaitForDependency[" + _async_writer_dependency->name() + "]Time");
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(PipelineXSinkLocalState<>::open(state));
        _writer->start_writer(state, _profile);
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) {
        return _writer->sink(block, source_state == SourceState::FINISHED);
    }

    WriteDependency* write_blocked_by() { return _writer->write_blocked_by(); }

    Status close(RuntimeState* state, Status exec_status) override {
        if (_closed) {
            return Status::OK();
        }
        COUNTER_SET(_wait_for_dependency_timer,
                    _async_writer_dependency->write_watcher_elapse_time());
        if (_writer->need_normal_close()) {
            if (exec_status.ok() && !state->is_cancelled()) {
                RETURN_IF_ERROR(_writer->commit_trans());
            }
            RETURN_IF_ERROR(_writer->close(exec_status));
        }
        return PipelineXSinkLocalState<>::close(state, exec_status);
    }

    Status try_close(RuntimeState* state, Status exec_status) override {
        if (state->is_cancelled() || !exec_status.ok()) {
            _writer->force_close(!exec_status.ok() ? exec_status : Status::Cancelled("Cancelled"));
        }
        return Status::OK();
    }

    bool is_pending_finish() { return _writer->is_pending_finish(); }

protected:
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    std::unique_ptr<Writer> _writer;

    std::shared_ptr<AsyncWriterDependency> _async_writer_dependency;
};

} // namespace doris::pipeline
