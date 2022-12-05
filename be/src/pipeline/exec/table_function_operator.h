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

#include "operator.h"
#include "vec/exec/vtable_function_node.h"

namespace doris {

namespace pipeline {
class TableFunctionOperator;

class TableFunctionOperatorBuilder : public OperatorBuilder {
public:
    TableFunctionOperatorBuilder(int32_t id, vectorized::VTableFunctionNode* node)
            : OperatorBuilder(id, "TableFunctionOperatorBuilder", node), _node(node) {}

    OperatorPtr build_operator() override;

private:
    vectorized::VTableFunctionNode* _node;
};

class TableFunctionOperator : public Operator {
public:
    TableFunctionOperator(TableFunctionOperatorBuilder* operator_builder,
                          vectorized::VTableFunctionNode* node)
            : Operator(operator_builder), _node(node) {}

    Status open(RuntimeState* state) override {
        RETURN_IF_ERROR(Operator::open(state));
        _child_block.reset(new vectorized::Block);
        return _node->alloc_resource(state);
    }

    Status close(RuntimeState* state) override {
        _node->release_resource(state);
        _fresh_exec_timer(_node);
        return Operator::close(state);
    }

    Status get_block(RuntimeState* state, vectorized::Block* block,
                     SourceState& source_state) override {
        if (_node->need_more_input_data()) {
            RETURN_IF_ERROR(_child->get_block(state, _child_block.get(), _child_source_state));
            source_state = _child_source_state;
            if (_child_block->rows() == 0) {
                return Status::OK();
            }
            _node->push(state, _child_block.get(), source_state == SourceState::FINISHED);
        }

        bool eos = false;
        RETURN_IF_ERROR(_node->pull(state, block, &eos));
        if (eos) {
            source_state = SourceState::FINISHED;
            _child_block->clear_column_data();
        } else if (!_node->need_more_input_data()) {
            source_state = SourceState::MORE_DATA;
        } else {
            _child_block->clear_column_data();
        }
        return Status::OK();
    }

private:
    vectorized::VTableFunctionNode* _node;
    std::unique_ptr<vectorized::Block> _child_block;
    SourceState _child_source_state;
};

OperatorPtr TableFunctionOperatorBuilder::build_operator() {
    return std::make_shared<TableFunctionOperator>(this, _node);
}

} // namespace pipeline
} // namespace doris
