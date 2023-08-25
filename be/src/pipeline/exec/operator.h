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
#include <stdint.h>

#include <functional>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exec/exec_node.h"
#include "pipeline/pipeline_x/dependency.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris {
class DataSink;
class RowDescriptor;
class RuntimeState;
class TDataSink;
} // namespace doris

#define OPERATOR_CODE_GENERATOR(NAME, SUBCLASS)                       \
    NAME##Builder::NAME##Builder(int32_t id, ExecNode* exec_node)     \
            : OperatorBuilder(id, #NAME, exec_node) {}                \
                                                                      \
    OperatorPtr NAME##Builder::build_operator() {                     \
        return std::make_shared<NAME>(this, _node);                   \
    }                                                                 \
                                                                      \
    NAME::NAME(OperatorBuilderBase* operator_builder, ExecNode* node) \
            : SUBCLASS(operator_builder, node) {};

namespace doris::pipeline {

/**
 * State of source operator.
 *                      |------> MORE_DATA ------|
 *                      |         ^    |         |
 * DEPEND_ON_SOURCE ----|         |----|         |----> FINISHED
 *    ^       |         |------------------------|
 *    |-------|
 */
enum class SourceState : uint8_t {
    DEPEND_ON_SOURCE = 0, // Need more data from source.
    MORE_DATA = 1,        // Has more data to output. (e.g. RepeatNode)
    FINISHED = 2
};

/**
 * State of sink operator.
 *                     |------> SINK_BUSY ------|
 *                     |         ^    |         |
 *   SINK_IDLE --------|         |----|         |----> FINISHED
 *   ^       |         |------------------------|
 *   |-------|
 */
enum class SinkState : uint8_t {
    SINK_IDLE = 0, // Can send block to sink.
    SINK_BUSY = 1, // Sink buffer is full, sink operator is blocked until buffer is freed.
    FINISHED = 2
};

class OperatorBuilderBase;
class OperatorBase;
class OperatorXBase;
class DataSinkOperatorX;

using OperatorPtr = std::shared_ptr<OperatorBase>;
using Operators = std::vector<OperatorPtr>;

using OperatorXPtr = std::shared_ptr<OperatorXBase>;
using OperatorXs = std::vector<OperatorXPtr>;

using DataSinkOperatorXPtr = std::shared_ptr<DataSinkOperatorX>;

using OperatorBuilderPtr = std::shared_ptr<OperatorBuilderBase>;
using OperatorBuilders = std::vector<OperatorBuilderPtr>;

class OperatorBuilderBase {
public:
    OperatorBuilderBase(int32_t id, const std::string& name) : _id(id), _name(name) {}

    virtual ~OperatorBuilderBase() = default;

    virtual OperatorPtr build_operator() = 0;

    virtual bool is_sink() const { return false; }
    virtual bool is_source() const { return false; }

    std::string get_name() const { return _name; }

    virtual const RowDescriptor& row_desc() = 0;

    int32_t id() const { return _id; }

protected:
    // Exec node id.
    const int32_t _id;
    const std::string _name;

    RuntimeState* _state = nullptr;
    bool _is_closed = false;
};

template <typename NodeType>
class OperatorBuilder : public OperatorBuilderBase {
public:
    OperatorBuilder(int32_t id, const std::string& name, ExecNode* exec_node = nullptr)
            : OperatorBuilderBase(id, name), _node(reinterpret_cast<NodeType*>(exec_node)) {}

    ~OperatorBuilder() override = default;

    const RowDescriptor& row_desc() override { return _node->row_desc(); }

    NodeType* exec_node() const { return _node; }

protected:
    NodeType* _node;
};

template <typename SinkType>
class DataSinkOperatorBuilder : public OperatorBuilderBase {
public:
    DataSinkOperatorBuilder(int32_t id, const std::string& name, DataSink* sink = nullptr)
            : OperatorBuilderBase(id, name), _sink(reinterpret_cast<SinkType*>(sink)) {}

    ~DataSinkOperatorBuilder() override = default;

    bool is_sink() const override { return true; }

    const RowDescriptor& row_desc() override { return _sink->row_desc(); }

    SinkType* exec_node() const { return _sink; }

protected:
    SinkType* _sink;
};

class OperatorBase {
public:
    explicit OperatorBase(OperatorBuilderBase* operator_builder);
    virtual ~OperatorBase() = default;

    virtual std::string get_name() const { return _operator_builder->get_name(); }

    virtual bool is_sink() const;

    virtual bool is_source() const;

    virtual Status init(const TDataSink& tsink) { return Status::OK(); }

    // Prepare for running. (e.g. resource allocation, etc.)
    virtual Status prepare(RuntimeState* state) = 0;

    /**
     * Allocate resources needed by this operator.
     *
     * This is called when current pipeline is scheduled first time.
     * e.g. If we got three pipeline and dependencies are A -> B, B-> C, all operators' `open`
     * method in pipeline C will be called once pipeline A and B finished.
     *
     * Now we have only one task per pipeline, so it has no problem，
     * But if one pipeline have multi task running in parallel, we need to rethink this logic.
     */
    virtual Status open(RuntimeState* state) = 0;

    /**
     * Release all resources once this operator done its work.
     */
    virtual Status close(RuntimeState* state);

    Status set_child(OperatorPtr child) {
        if (is_source()) {
            return Status::InternalError("source can not has child.");
        }
        _child = std::move(child);
        return Status::OK();
    }

    Status set_child(OperatorXPtr child) {
        _child_x = std::move(child);
        return Status::OK();
    }

    virtual bool can_read() { return false; } // for source

    virtual bool runtime_filters_are_ready_or_timeout() { return true; } // for source

    virtual bool can_write() { return false; } // for sink

    [[nodiscard]] virtual bool can_terminate_early() { return false; }

    /**
     * The main method to execute a pipeline task.
     * Now it is a pull-based pipeline and operators pull data from its child by this method.
     */
    virtual Status get_block(RuntimeState* runtime_state, vectorized::Block* block,
                             SourceState& result_state) {
        return Status::OK();
    }

    /**
     * Push data to the sink operator.
     * Data in this block will be sent by RPC or written to somewhere finally.
     */
    virtual Status sink(RuntimeState* state, vectorized::Block* block,
                        SourceState source_state) = 0;

    virtual Status finalize(RuntimeState* state) {
        std::stringstream error_msg;
        error_msg << " not a sink, can not finalize";
        return Status::NotSupported(error_msg.str());
    }

    /**
     * pending_finish means we have called `close` and there are still some work to do before finishing.
     * Now it is a pull-based pipeline and operators pull data from its child by this method.
     *
     * For source operator, it is pending_finish iff scan threads have not been released yet
     * For sink operator, it is pending_finish iff RPC resources have not been released yet
     * Otherwise, it will return false.
     */
    virtual bool is_pending_finish() const { return false; }

    virtual Status try_close(RuntimeState* state) { return Status::OK(); }

    bool is_closed() const { return _is_closed; }

    const OperatorBuilderBase* operator_builder() const { return _operator_builder; }

    virtual const RowDescriptor& row_desc();

    virtual std::string debug_string() const;
    virtual int32_t id() const { return _operator_builder->id(); }

    [[nodiscard]] virtual RuntimeProfile* get_runtime_profile() const = 0;

protected:
    OperatorBuilderBase* _operator_builder;
    OperatorPtr _child;

    // Used on pipeline X
    OperatorXPtr _child_x;

    bool _is_closed;
};

/**
 * All operators inherited from DataSinkOperator will hold a SinkNode inside. Namely, it is a one-to-one relation between DataSinkOperator and DataSink.
 *
 * It should be mentioned that, not all SinkOperators are inherited from this (e.g. SortSinkOperator which holds a sort node inside instead of a DataSink).
 */
template <typename OperatorBuilderType>
class DataSinkOperator : public OperatorBase {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    DataSinkOperator(OperatorBuilderBase* builder, DataSink* sink)
            : OperatorBase(builder), _sink(reinterpret_cast<NodeType*>(sink)) {}

    ~DataSinkOperator() override = default;

    Status prepare(RuntimeState* state) override { return Status::OK(); }

    Status open(RuntimeState* state) override { return _sink->open(state); }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        if (in_block->rows() > 0 || source_state == SourceState::FINISHED) {
            auto st = _sink->sink(state, in_block, source_state == SourceState::FINISHED);
            // TODO: improvement: if sink returned END_OF_FILE, pipeline task can be finished
            if (st.template is<ErrorCode::END_OF_FILE>()) {
                return Status::OK();
            }
            return st;
        }
        return Status::OK();
    }

    Status try_close(RuntimeState* state) override {
        return _sink->try_close(state, state->query_status());
    }

    [[nodiscard]] bool is_pending_finish() const override { return !_sink->is_close_done(); }

    Status close(RuntimeState* state) override {
        if (is_closed()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(_sink->close(state, state->query_status()));
        _is_closed = true;
        return Status::OK();
    }

    Status finalize(RuntimeState* state) override { return Status::OK(); }

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override { return _sink->profile(); }

protected:
    NodeType* _sink;
};

/**
 * All operators inherited from Operator will hold a ExecNode inside.
 */
template <typename OperatorBuilderType>
class StreamingOperator : public OperatorBase {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    StreamingOperator(OperatorBuilderBase* builder, ExecNode* node)
            : OperatorBase(builder), _node(reinterpret_cast<NodeType*>(node)) {}

    ~StreamingOperator() override = default;

    [[nodiscard]] bool can_terminate_early() override { return _node->can_terminate_early(); }

    Status prepare(RuntimeState* state) override {
        _node->increase_ref();
        _use_projection = _node->has_output_row_descriptor();
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(_node->alloc_resource(state));
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        return _node->sink(state, in_block, source_state == SourceState::FINISHED);
    }

    Status close(RuntimeState* state) override {
        if (is_closed()) {
            return Status::OK();
        }
        if (!_node->decrease_ref()) {
            _node->release_resource(state);
        }
        _is_closed = true;
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        DCHECK(_child);
        auto input_block = _use_projection ? _node->get_clear_input_block() : block;
        RETURN_IF_ERROR(_child->get_block(state, input_block, source_state));
        bool eos = false;
        RETURN_IF_ERROR(_node->get_next_after_projects(
                state, block, &eos,
                std::bind(&ExecNode::pull, _node, std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3),
                false));
        return Status::OK();
    }

    Status finalize(RuntimeState* state) override { return Status::OK(); }

    bool can_read() override { return _node->can_read(); }

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override {
        return _node->runtime_profile();
    }

protected:
    NodeType* _node;
    bool _use_projection;
};

template <typename OperatorBuilderType>
class SourceOperator : public StreamingOperator<OperatorBuilderType> {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    SourceOperator(OperatorBuilderBase* builder, ExecNode* node)
            : StreamingOperator<OperatorBuilderType>(builder, node) {}

    ~SourceOperator() override = default;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        auto& node = StreamingOperator<OperatorBuilderType>::_node;
        bool eos = false;
        RETURN_IF_ERROR(node->get_next_after_projects(
                state, block, &eos,
                std::bind(&ExecNode::pull, node, std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
        source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;
        return Status::OK();
    }
};

/**
 * StatefulOperator indicates the operators with some states inside.
 *
 * Specifically, we called an operator stateful if an operator can determine its output by itself.
 * For example, hash join probe operator is a typical StatefulOperator. When it gets a block from probe side, it will hold this block inside (e.g. _child_block).
 * If there are still remain rows in probe block, we can get output block by calling `get_block` without any data from its child.
 * In a nutshell, it is a one-to-many relation between input blocks and output blocks for StatefulOperator.
 */
template <typename OperatorBuilderType>
class StatefulOperator : public StreamingOperator<OperatorBuilderType> {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    StatefulOperator(OperatorBuilderBase* builder, ExecNode* node)
            : StreamingOperator<OperatorBuilderType>(builder, node),
              _child_block(vectorized::Block::create_unique()),
              _child_source_state(SourceState::DEPEND_ON_SOURCE) {}

    virtual ~StatefulOperator() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        auto& node = StreamingOperator<OperatorBuilderType>::_node;
        auto& child = StreamingOperator<OperatorBuilderType>::_child;

        if (node->need_more_input_data()) {
            _child_block->clear_column_data();
            RETURN_IF_ERROR(child->get_block(state, _child_block.get(), _child_source_state));
            source_state = _child_source_state;
            if (_child_block->rows() == 0 && _child_source_state != SourceState::FINISHED) {
                return Status::OK();
            }
            node->prepare_for_next();
            RETURN_IF_ERROR(node->push(state, _child_block.get(),
                                       _child_source_state == SourceState::FINISHED));
        }

        if (!node->need_more_input_data()) {
            bool eos = false;
            RETURN_IF_ERROR(node->get_next_after_projects(
                    state, block, &eos,
                    std::bind(&ExecNode::pull, node, std::placeholders::_1, std::placeholders::_2,
                              std::placeholders::_3)));
            if (eos) {
                source_state = SourceState::FINISHED;
            } else if (!node->need_more_input_data()) {
                source_state = SourceState::MORE_DATA;
            } else if (source_state == SourceState::MORE_DATA) {
                source_state = _child_source_state;
            }
        }
        return Status::OK();
    }

protected:
    std::unique_ptr<vectorized::Block> _child_block;
    SourceState _child_source_state;
};

// This struct is used only for initializing local state.
struct LocalStateInfo {
    const std::vector<TScanRangeParams> scan_ranges;
    Dependency* dependency;
};

// This struct is used only for initializing local sink state.
struct LocalSinkStateInfo {
    const int sender_id;
    Dependency* dependency;
};

class PipelineXLocalState {
public:
    PipelineXLocalState(RuntimeState* state, OperatorXBase* parent)
            : _num_rows_returned(0),
              _rows_returned_counter(nullptr),
              _rows_returned_rate(nullptr),
              _memory_used_counter(nullptr),
              _peak_memory_usage_counter(nullptr),
              _parent(parent),
              _state(state) {}
    virtual ~PipelineXLocalState() {}

    virtual Status init(RuntimeState* state, LocalStateInfo& info);
    template <class TARGET>
    TARGET& cast() {
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        return reinterpret_cast<const TARGET&>(*this);
    }

    // If use projection, we should clear `_origin_block`.
    void clear_origin_block();

    bool reached_limit() const;
    void reached_limit(vectorized::Block* block, bool* eos);
    void reached_limit(vectorized::Block* block, SourceState& source_state);
    RuntimeProfile* profile() { return _runtime_profile.get(); }

    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    RuntimeProfile::Counter* rows_returned_counter() { return _rows_returned_counter; }
    RuntimeProfile::Counter* rows_returned_rate() { return _rows_returned_rate; }
    RuntimeProfile::Counter* memory_used_counter() { return _memory_used_counter; }
    RuntimeProfile::Counter* projection_timer() { return _projection_timer; }

    OperatorXBase* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    vectorized::VExprContextSPtrs& projections() { return _projections; }
    int64_t num_rows_returned() const { return _num_rows_returned; }
    void add_num_rows_returned(int64_t delta) { _num_rows_returned += delta; }
    void set_num_rows_returned(int64_t value) { _num_rows_returned = value; }

protected:
    friend class OperatorXBase;

    ObjectPool* _pool;
    int64_t _num_rows_returned;

    std::unique_ptr<RuntimeProfile> _runtime_profile;

    // Record this node memory size. it is expected that artificial guarantees are accurate,
    // which will providea reference for operator memory.
    std::unique_ptr<MemTracker> _mem_tracker;

    RuntimeProfile::Counter* _rows_returned_counter;
    RuntimeProfile::Counter* _rows_returned_rate;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _memory_used_counter;
    RuntimeProfile::Counter* _projection_timer;
    // Account for peak memory used by this node
    RuntimeProfile::Counter* _peak_memory_usage_counter;

    OpentelemetrySpan _span;
    OperatorXBase* _parent;
    RuntimeState* _state;
    vectorized::VExprContextSPtrs _conjuncts;
    vectorized::VExprContextSPtrs _projections;
    bool _init = false;
    vectorized::Block _origin_block;
};

class OperatorXBase : public OperatorBase {
public:
    OperatorXBase(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                  std::string op_name)
            : OperatorBase(nullptr),
              _id(tnode.node_id),
              _type(tnode.node_type),
              _pool(pool),
              _tuple_ids(tnode.row_tuples),
              _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
              _resource_profile(tnode.resource_profile),
              _limit(tnode.limit),
              _op_name(op_name) {
        if (tnode.__isset.output_tuple_id) {
            _output_row_descriptor.reset(new RowDescriptor(descs, {tnode.output_tuple_id}, {true}));
        }
    }

    virtual Status init(const TPlanNode& tnode, RuntimeState* state);
    Status init(const TDataSink& tsink) override {
        LOG(FATAL) << "should not reach here!";
        return Status::OK();
    }
    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override {
        return _runtime_profile.get();
    }
    [[nodiscard]] std::string get_name() const override { return _op_name; }

    virtual Status prepare(RuntimeState* state) override;

    virtual Status open(RuntimeState* state) override;

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

    bool is_pending_finish() const override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) override {
        LOG(FATAL) << "should not reach here!";
        return Status::OK();
    }

    virtual Status close(RuntimeState* state) override;

    virtual bool can_read(RuntimeState* state) { return false; }

    virtual bool is_pending_finish(RuntimeState* state) const { return false; }

    [[nodiscard]] virtual const RowDescriptor& intermediate_row_desc() const {
        return _row_descriptor;
    }

    virtual std::string debug_string() const override;

    virtual Status setup_local_state(RuntimeState* state, LocalStateInfo& info) = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this));
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this));
        return reinterpret_cast<const TARGET&>(*this);
    }

    OperatorXPtr get_child() { return _child_x; }

    vectorized::VExprContextSPtrs& conjuncts() { return _conjuncts; }
    RowDescriptor& row_descriptor() { return _row_descriptor; }

    [[nodiscard]] int id() const override { return _id; }

    [[nodiscard]] int64_t limit() const { return _limit; }

    const RowDescriptor& row_desc() override {
        return _output_row_descriptor ? *_output_row_descriptor : _row_descriptor;
    }

    virtual bool is_source() const override { return false; }

    Status get_next_after_projects(RuntimeState* state, vectorized::Block* block,
                                   SourceState& source_state);

    /// Only use in vectorized exec engine try to do projections to trans _row_desc -> _output_row_desc
    Status do_projections(RuntimeState* state, vectorized::Block* origin_block,
                          vectorized::Block* output_block);

protected:
    friend class PipelineXLocalState;
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
    std::unique_ptr<RuntimeProfile> _runtime_profile;

private:
    void _init_runtime_profile();

    std::string _op_name;
};

class DataSinkOperatorX;

class PipelineXSinkLocalState {
public:
    PipelineXSinkLocalState(DataSinkOperatorX* parent_, RuntimeState* state_)
            : _parent(parent_), _state(state_) {}
    virtual ~PipelineXSinkLocalState() {}

    virtual Status init(RuntimeState* state, LocalSinkStateInfo& info);
    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this));
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this));
        return reinterpret_cast<const TARGET&>(*this);
    }

    DataSinkOperatorX* parent() { return _parent; }
    RuntimeState* state() { return _state; }
    RuntimeProfile* profile() { return _profile; }
    MemTracker* mem_tracker() { return _mem_tracker.get(); }
    QueryStatistics* query_statistics() { return _query_statistics.get(); }

protected:
    DataSinkOperatorX* _parent;
    RuntimeState* _state;
    RuntimeProfile* _profile;
    std::unique_ptr<MemTracker> _mem_tracker;
    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;
};

class DataSinkOperatorX : public OperatorBase {
public:
    DataSinkOperatorX(const int id) : OperatorBase(nullptr), _id(id) {}

    ~DataSinkOperatorX() override = default;

    // For agg/sort/join sink.
    virtual Status init(const TPlanNode& tnode, RuntimeState* state) {
        return Status::InternalError("{} should not init with TPlanNode", _name);
    }

    virtual Status init(const TDataSink& tsink) override { return Status::OK(); }

    virtual Status setup_local_state(RuntimeState* state, LocalSinkStateInfo& info) = 0;

    template <class TARGET>
    TARGET& cast() {
        DCHECK(dynamic_cast<TARGET*>(this));
        return reinterpret_cast<TARGET&>(*this);
    }
    template <class TARGET>
    const TARGET& cast() const {
        DCHECK(dynamic_cast<const TARGET*>(this));
        return reinterpret_cast<const TARGET&>(*this);
    }

    virtual void get_dependency(DependencySPtr& dependency) {
        dependency.reset((Dependency*)nullptr);
    }

    Status close(RuntimeState* state) override { return Status::OK(); }

    bool can_read() override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    bool can_write() override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    bool is_pending_finish() const override {
        LOG(FATAL) << "should not reach here!";
        return false;
    }

    virtual bool can_write(RuntimeState* state) { return false; }

    virtual bool is_pending_finish(RuntimeState* state) const { return false; }

    virtual std::string debug_string() const override;

    bool is_sink() const override { return true; }

    bool is_source() const override { return false; }

    virtual Status close(RuntimeState* state, Status exec_status) { return Status::OK(); }

    [[nodiscard]] RuntimeProfile* get_runtime_profile() const override { return _profile; }

    [[nodiscard]] int id() const override { return _id; }

    [[nodiscard]] std::string get_name() const override { return _name; }

    Status finalize(RuntimeState* state) override { return Status::OK(); }

protected:
    const int _id;
    // Set to true after close() has been called. subclasses should check and set this in
    // close().
    bool _closed;
    std::string _name;

    // Maybe this will be transferred to BufferControlBlock.
    std::shared_ptr<QueryStatistics> _query_statistics;

    OpentelemetrySpan _span {};

    RuntimeProfile* _profile = nullptr;
};

} // namespace doris::pipeline
