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

#include <utility>

#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/exec/vdata_gen_scan_node.h"

#define OPERATOR_CODE_GENERATOR(NAME, SUBCLASS)                                                 \
    NAME##Builder::NAME##Builder(int32_t id, ExecNode* exec_node)                               \
            : OperatorBuilder(id, #NAME, exec_node) {}                                          \
                                                                                                \
    OperatorPtr NAME##Builder::build_operator() { return std::make_shared<NAME>(this, _node); } \
                                                                                                \
    NAME::NAME(OperatorBuilderBase* operator_builder, ExecNode* node)                           \
            : SUBCLASS(operator_builder, node) {};

namespace doris::pipeline {

// Result of source pull data, init state is DEPEND_ON_SOURCE
enum class SourceState : uint8_t {
    DEPEND_ON_SOURCE = 0, // Operator has no more data in itself, needs to read from source.
    MORE_DATA = 1,        // Still have data can read
    FINISHED = 2
};

enum class SinkState : uint8_t {
    SINK_IDLE = 0, // can send block to sink
    SINK_BUSY = 1, // sink buffer is full， should wait sink to send some block
    FINISHED = 2
};
////////////////       DO NOT USE THE UP State     ////////////////

class OperatorBuilderBase;
class OperatorBase;

using OperatorPtr = std::shared_ptr<OperatorBase>;
using Operators = std::vector<OperatorPtr>;

using OperatorBuilderPtr = std::shared_ptr<OperatorBuilderBase>;
using OperatorBuilders = std::vector<OperatorBuilderPtr>;

class OperatorBuilderBase {
public:
    OperatorBuilderBase(int32_t id, const std::string& name) : _id(id), _name(name) {}

    virtual ~OperatorBuilderBase() = default;

    virtual OperatorPtr build_operator() = 0;

    virtual bool is_sink() const { return false; }
    virtual bool is_source() const { return false; }

    // create the object used by all operator
    virtual Status prepare(RuntimeState* state);

    // destory the object used by all operator
    virtual void close(RuntimeState* state);

    std::string get_name() const { return _name; }

    RuntimeState* runtime_state() { return _state; }

    virtual const RowDescriptor& row_desc() = 0;

    int32_t id() const { return _id; }

protected:
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

    // After both sink and source need to know the cancel state.
    // do cancel work
    bool is_sink() const;

    bool is_source() const;

    // Only result sink and data stream sink need to impl the virtual function
    virtual Status init(const TDataSink& tsink) { return Status::OK(); };

    // Do prepare some state of Operator
    virtual Status prepare(RuntimeState* state) = 0;

    // Like ExecNode，when pipeline task first time be scheduled， can't block
    // the pipeline should be open after dependencies is finish
    // Eg a -> c, b-> c, after a, b pipeline finish, c pipeline should call open
    // Now the pipeline only have one task, so the there is no performance bottleneck for the mechanism，
    // but if one pipeline have multi task to parallel work, need to rethink the logic
    //
    // Each operator should call alloc_resource() to prepare resource to do data compute.
    // if ExecNode split to sink and source operator, alloc_resource() should be called in sink operator
    virtual Status open(RuntimeState* state) = 0;

    // Release the resource, should not block the thread
    //
    // Each operator should call close_self() to release resource
    // if ExecNode split to sink and source operator, close_self() should be called in source operator
    virtual Status close(RuntimeState* state) = 0;

    Status set_child(OperatorPtr child) {
        if (is_source()) {
            return Status::InternalError("source can not has child.");
        }
        _child = std::move(child);
        return Status::OK();
    }

    virtual bool can_read() { return false; } // for source

    virtual bool can_write() { return false; } // for sink

    // for pipeline
    virtual Status get_block(RuntimeState* runtime_state, vectorized::Block* block,
                             SourceState& result_state) {
        return Status::OK();
    };

    // return can write continue
    virtual Status sink(RuntimeState* state, vectorized::Block* block,
                        SourceState source_state) = 0;

    virtual Status finalize(RuntimeState* state) {
        std::stringstream error_msg;
        error_msg << " not a sink, can not finalize";
        return Status::NotSupported(error_msg.str());
    }

    // close be called
    // - Source: scan thread do not exist
    // - Sink: RPC do not be disposed
    // - else return false
    virtual bool is_pending_finish() const { return false; }

    // TODO: should we keep the function
    // virtual bool is_finished() = 0;

    bool is_closed() const { return _is_closed; }

    MemTracker* mem_tracker() const { return _mem_tracker.get(); }

    const OperatorBuilderBase* operator_builder() const { return _operator_builder; }

    const RowDescriptor& row_desc();

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    std::string debug_string() const;

protected:
    std::unique_ptr<MemTracker> _mem_tracker;

    OperatorBuilderBase* _operator_builder;
    // source has no child
    // if an operator is not source, it will get data from its child.
    OperatorPtr _child;

    std::unique_ptr<RuntimeProfile> _runtime_profile;
    // TODO pipeline Account for peak memory used by this operator
    RuntimeProfile::Counter* _memory_used_counter = nullptr;

private:
    bool _is_closed = false;
};

template <typename OperatorBuilderType>
class DataSinkOperator : public OperatorBase {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    DataSinkOperator(OperatorBuilderBase* builder, DataSink* sink)
            : OperatorBase(builder), _sink(reinterpret_cast<NodeType*>(sink)) {};

    ~DataSinkOperator() override = default;

    Status prepare(RuntimeState* state) override {
        RETURN_IF_ERROR(_sink->prepare(state));
        _runtime_profile.reset(new RuntimeProfile(_operator_builder->get_name()));
        _sink->profile()->insert_child_head(_runtime_profile.get(), true);
        _mem_tracker = std::make_unique<MemTracker>("Operator:" + _runtime_profile->name(),
                                                    _runtime_profile.get());
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        return _sink->open(state);
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        if (!UNLIKELY(in_block)) {
            DCHECK(source_state == SourceState::FINISHED)
                    << "block is null, eos should invoke in finalize.";
            return Status::OK();
        }
        return _sink->send(state, in_block, source_state == SourceState::FINISHED);
    }

    Status close(RuntimeState* state) override {
        _fresh_exec_timer(_sink);
        return _sink->close(state, Status::OK());
    }

    Status finalize(RuntimeState* state) override { return Status::OK(); }

protected:
    void _fresh_exec_timer(NodeType* node) {
        node->profile()->total_time_counter()->update(
                _runtime_profile->total_time_counter()->value());
    }

    NodeType* _sink;
};

template <typename OperatorBuilderType>
class Operator : public OperatorBase {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    Operator(OperatorBuilderBase* builder, ExecNode* node)
            : OperatorBase(builder), _node(reinterpret_cast<NodeType*>(node)) {};

    ~Operator() override = default;

    Status prepare(RuntimeState* state) override {
        _runtime_profile.reset(new RuntimeProfile(_operator_builder->get_name()));
        _node->runtime_profile()->insert_child_head(_runtime_profile.get(), true);
        _mem_tracker = std::make_unique<MemTracker>("Operator:" + _runtime_profile->name(),
                                                    _runtime_profile.get());
        _node->increase_ref();
        return Status::OK();
    }

    Status open(RuntimeState* state) override {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        RETURN_IF_ERROR(_node->alloc_resource(state));
        return Status::OK();
    }

    Status sink(RuntimeState* state, vectorized::Block* in_block,
                SourceState source_state) override {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        return _node->sink(state, in_block, source_state == SourceState::FINISHED);
    }

    Status close(RuntimeState* state) override {
        _fresh_exec_timer(_node);
        if (!_node->decrease_ref()) {
            _node->release_resource(state);
        }
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        SCOPED_TIMER(_runtime_profile->total_time_counter());
        bool eos = false;
        RETURN_IF_ERROR(_node->pull(state, block, &eos));
        source_state = eos ? SourceState::FINISHED : SourceState::DEPEND_ON_SOURCE;
        return Status::OK();
    }

    Status finalize(RuntimeState* state) override { return Status::OK(); }

    bool can_read() override { return _node->can_read(); }

protected:
    void _fresh_exec_timer(NodeType* node) {
        node->runtime_profile()->total_time_counter()->update(
                _runtime_profile->total_time_counter()->value());
    }

    NodeType* _node;
};

template <typename OperatorBuilderType>
class DataStateOperator : public Operator<OperatorBuilderType> {
public:
    using NodeType =
            std::remove_pointer_t<decltype(std::declval<OperatorBuilderType>().exec_node())>;

    DataStateOperator(OperatorBuilderBase* builder, ExecNode* node)
            : Operator<OperatorBuilderType>(builder, node),
              _child_block(new vectorized::Block),
              _child_source_state(SourceState::DEPEND_ON_SOURCE) {};

    virtual ~DataStateOperator() = default;

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        auto& node = Operator<OperatorBuilderType>::_node;
        auto& child = Operator<OperatorBuilderType>::_child;

        if (node->need_more_input_data()) {
            RETURN_IF_ERROR(child->get_block(state, _child_block.get(), _child_source_state));
            source_state = _child_source_state;
            if (_child_block->rows() == 0) {
                return Status::OK();
            }
            node->push(state, _child_block.get(), source_state == SourceState::FINISHED);
        }

        bool eos = false;
        RETURN_IF_ERROR(node->pull(state, block, &eos));
        if (eos) {
            source_state = SourceState::FINISHED;
            _child_block->clear_column_data();
        } else if (!node->need_more_input_data()) {
            source_state = SourceState::MORE_DATA;
        } else {
            _child_block->clear_column_data();
        }
        return Status::OK();
    }

protected:
    std::unique_ptr<vectorized::Block> _child_block;
    SourceState _child_source_state;
};

} // namespace doris::pipeline
