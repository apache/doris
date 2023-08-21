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

#include "operator.h"

#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

OperatorBase::OperatorBase(OperatorBuilderBase* operator_builder)
        : _operator_builder(operator_builder), _is_closed(false) {}

bool OperatorBase::is_sink() const {
    return _operator_builder->is_sink();
}

bool OperatorBase::is_source() const {
    return _operator_builder->is_source();
}

Status OperatorBase::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    return Status::OK();
}

const RowDescriptor& OperatorBase::row_desc() {
    return _operator_builder->row_desc();
}

std::string OperatorBase::debug_string() const {
    std::stringstream ss;
    ss << _operator_builder->get_name() << ": is_source: " << is_source();
    ss << ", is_sink: " << is_sink() << ", is_closed: " << _is_closed;
    ss << ", is_pending_finish: " << is_pending_finish();
    return ss.str();
}

std::string OperatorXBase::debug_string() const {
    std::stringstream ss;
    ss << _op_name << ": is_source: " << is_source();
    ss << ", is_closed: " << _is_closed;
    return ss.str();
}

Status OperatorXBase::init(const TPlanNode& tnode, RuntimeState* /*state*/) {
    _init_runtime_profile();

    if (tnode.__isset.vconjunct) {
        vectorized::VExprContextSPtr context;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(tnode.vconjunct, context));
        _conjuncts.emplace_back(context);
    } else if (tnode.__isset.conjuncts) {
        for (auto& conjunct : tnode.conjuncts) {
            vectorized::VExprContextSPtr context;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(conjunct, context));
            _conjuncts.emplace_back(context);
        }
    }

    // create the projections expr
    if (tnode.__isset.projections) {
        DCHECK(tnode.__isset.output_tuple_id);
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(tnode.projections, _projections));
    }
    return Status::OK();
}

Status OperatorXBase::prepare(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->prepare(state, intermediate_row_desc()));
    }

    RETURN_IF_ERROR(vectorized::VExpr::prepare(_projections, state, intermediate_row_desc()));

    if (_children) {
        RETURN_IF_ERROR(_children->prepare(state));
    }

    return Status::OK();
}

Status OperatorXBase::open(RuntimeState* state) {
    for (auto& conjunct : _conjuncts) {
        RETURN_IF_ERROR(conjunct->open(state));
    }
    RETURN_IF_ERROR(vectorized::VExpr::open(_projections, state));
    return Status::OK();
}

void OperatorXBase::_init_runtime_profile() {
    std::stringstream ss;
    ss << get_name() << " (id=" << _id << ")";
    _runtime_profile.reset(new RuntimeProfile(ss.str()));
    _runtime_profile->set_metadata(_id);
}

Status OperatorXBase::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    auto local_state = state->get_local_state(id());
    Status result;
    _children->close(state);
    if (local_state->_rows_returned_counter != nullptr) {
        COUNTER_SET(local_state->_rows_returned_counter, local_state->_num_rows_returned);
    }

    local_state->profile()->add_to_span(local_state->_span);
    _is_closed = true;
    return Status::OK();
}

Status PipelineXLocalState::init(RuntimeState* state, LocalStateInfo& /*info*/) {
    _runtime_profile.reset(new RuntimeProfile("LocalState " + _parent->get_name()));
    _parent->get_runtime_profile()->add_child(_runtime_profile.get(), true, nullptr);
    _conjuncts.resize(_parent->_conjuncts.size());
    _projections.resize(_parent->_projections.size());
    for (size_t i = 0; i < _conjuncts.size(); i++) {
        RETURN_IF_ERROR(_parent->_conjuncts[i]->clone(state, _conjuncts[i]));
    }
    for (size_t i = 0; i < _projections.size(); i++) {
        RETURN_IF_ERROR(_parent->_projections[i]->clone(state, _projections[i]));
    }
    DCHECK(_runtime_profile.get() != nullptr);
    _rows_returned_counter = ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _projection_timer = ADD_TIMER(_runtime_profile, "ProjectionTime");
    _rows_returned_rate = profile()->add_derived_counter(
            doris::ExecNode::ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
            std::bind<int64_t>(&RuntimeProfile::units_per_second, _rows_returned_counter,
                               profile()->total_time_counter()),
            "");
    _mem_tracker = std::make_unique<MemTracker>("PipelineXLocalState:" + _runtime_profile->name());
    return Status::OK();
}

bool PipelineXLocalState::reached_limit() const {
    return _parent->_limit != -1 && _num_rows_returned >= _parent->_limit;
}

void PipelineXLocalState::reached_limit(vectorized::Block* block, bool* eos) {
    if (_parent->_limit != -1 and _num_rows_returned + block->rows() >= _parent->_limit) {
        block->set_num_rows(_parent->_limit - _num_rows_returned);
        *eos = true;
    }

    _num_rows_returned += block->rows();
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
}

std::string DataSinkOperatorX::debug_string() const {
    std::stringstream ss;
    ss << _name << ", is_closed: " << _is_closed;
    return ss.str();
}
} // namespace doris::pipeline
