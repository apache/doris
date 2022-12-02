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

namespace doris::pipeline {

// Result of source pull data, init state is DEPEND_ON_SOURCE
enum class SourceState : uint8_t {
    DEPEND_ON_SOURCE = 0, // Operator has no more data in itself, needs to read from source.
    MORE_DATA = 1,        // Still have data can read
    FINISHED = 2
};

//
enum class SinkState : uint8_t {
    SINK_IDLE = 0, // can send block to sink
    SINK_BUSY = 1, // sink buffer is full， should wait sink to send some block
    FINISHED = 2
};
////////////////       DO NOT USE THE UP State     ////////////////

class OperatorBuilder;
class Operator;

using OperatorPtr = std::shared_ptr<Operator>;
using Operators = std::vector<OperatorPtr>;

class Operator {
public:
    explicit Operator(OperatorBuilder* operator_builder);
    virtual ~Operator() = default;

    // After both sink and source need to know the cancel state.
    // do cancel work
    bool is_sink() const;

    bool is_source() const;

    // Should be call after ExecNode is constructed
    virtual Status init(ExecNode* exec_node, RuntimeState* state = nullptr);

    // Only result sink and data stream sink need to impl the virtual function
    virtual Status init(const TDataSink& tsink) { return Status::OK(); };

    // Do prepare some state of Operator
    virtual Status prepare(RuntimeState* state);

    // Like ExecNode，when pipeline task first time be scheduled， can't block
    // the pipeline should be open after dependencies is finish
    // Eg a -> c, b-> c, after a, b pipeline finish, c pipeline should call open
    // Now the pipeline only have one task, so the there is no performance bottleneck for the mechanism，
    // but if one pipeline have multi task to parallel work, need to rethink the logic
    //
    // Each operator should call open_self() to prepare resource to do data compute.
    // if ExecNode split to sink and source operator, open_self() should be called in sink operator
    virtual Status open(RuntimeState* state);

    // Release the resource, should not block the thread
    //
    // Each operator should call close_self() to release resource
    // if ExecNode split to sink and source operator, close_self() should be called in source operator
    virtual Status close(RuntimeState* state);

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
    virtual Status get_block([[maybe_unused]] RuntimeState* runtime_state,
                             [[maybe_unused]] vectorized::Block* block,
                             [[maybe_unused]] SourceState& result_state) {
        std::stringstream error_msg;
        error_msg << " has not implements get_block";
        return Status::NotSupported(error_msg.str());
    }

    // return can write continue
    virtual Status sink(RuntimeState* state, vectorized::Block* block, SourceState source_state) {
        std::stringstream error_msg;
        error_msg << " not a sink ";
        return Status::NotSupported(error_msg.str());
    }

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

    const OperatorBuilder* operator_builder() const { return _operator_builder; }

    const RowDescriptor& row_desc();

    RuntimeProfile* runtime_profile() { return _runtime_profile.get(); }
    std::string debug_string() const;

protected:
    void _fresh_exec_timer(ExecNode* node);

    std::unique_ptr<MemTracker> _mem_tracker;

    OperatorBuilder* _operator_builder;
    // source has no child
    // if an operator is not source, it will get data from its child.
    OperatorPtr _child;

    std::unique_ptr<RuntimeProfile> _runtime_profile;
    // TODO pipeline Account for peak memory used by this operator
    RuntimeProfile::Counter* _memory_used_counter = nullptr;

private:
    bool _is_closed = false;
};

class OperatorBuilder {
public:
    OperatorBuilder(int32_t id, const std::string& name, ExecNode* exec_node = nullptr)
            : _id(id), _name(name), _related_exec_node(exec_node) {}

    virtual ~OperatorBuilder() = default;

    virtual OperatorPtr build_operator() = 0;

    virtual bool is_sink() const { return false; }
    virtual bool is_source() const { return false; }

    // create the object used by all operator
    virtual Status prepare(RuntimeState* state);

    // destory the object used by all operator
    virtual void close(RuntimeState* state);

    std::string get_name() const { return _name; }

    RuntimeState* runtime_state() { return _state; }

    const RowDescriptor& row_desc() { return _related_exec_node->row_desc(); }

    ExecNode* exec_node() const { return _related_exec_node; }

    int32_t id() const { return _id; }

protected:
    const int32_t _id;
    const std::string _name;
    ExecNode* _related_exec_node;

    RuntimeState* _state = nullptr;
    bool _is_closed = false;
};

using OperatorBuilderPtr = std::shared_ptr<OperatorBuilder>;
using OperatorBuilders = std::vector<OperatorBuilderPtr>;

} // namespace doris::pipeline
