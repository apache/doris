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

#include "vec/exprs/vectorized_agg_fn.h"

#include "fmt/format.h"
#include "fmt/ranges.h"
#include "runtime/descriptors.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/materialize_block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

AggFnEvaluator::AggFnEvaluator(const TExprNode& desc)
        : _fn(desc.fn),
          _is_merge(desc.agg_expr.is_merge_agg),
          _return_type(TypeDescriptor::from_thrift(desc.fn.ret_type)),
          _intermediate_type(TypeDescriptor::from_thrift(desc.fn.aggregate_fn.intermediate_type)),
          _intermediate_slot_desc(nullptr),
          _output_slot_desc(nullptr),
          _exec_timer(nullptr),
          _merge_timer(nullptr),
          _expr_timer(nullptr) {
    bool nullable = true;
    if (desc.__isset.is_nullable) {
        nullable = desc.is_nullable;
    }
    _data_type = DataTypeFactory::instance().create_data_type(_return_type, nullable);
}

Status AggFnEvaluator::create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result) {
    *result = pool->add(new AggFnEvaluator(desc.nodes[0]));
    auto& agg_fn_evaluator = *result;
    int node_idx = 0;
    for (int i = 0; i < desc.nodes[0].num_children; ++i) {
        ++node_idx;
        VExpr* expr = nullptr;
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(
                VExpr::create_tree_from_thrift(pool, desc.nodes, NULL, &node_idx, &expr, &ctx));
        agg_fn_evaluator->_input_exprs_ctxs.push_back(ctx);
    }
    return Status::OK();
}

Status AggFnEvaluator::prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                               const SlotDescriptor* intermediate_slot_desc,
                               const SlotDescriptor* output_slot_desc,
                               const std::shared_ptr<MemTracker>& mem_tracker) {
    DCHECK(pool != NULL);
    DCHECK(intermediate_slot_desc != NULL);
    DCHECK(_intermediate_slot_desc == NULL);
    _output_slot_desc = output_slot_desc;
    _intermediate_slot_desc = intermediate_slot_desc;

    Status status = VExpr::prepare(_input_exprs_ctxs, state, desc, mem_tracker);
    RETURN_IF_ERROR(status);

    DataTypes argument_types;
    argument_types.reserve(_input_exprs_ctxs.size());

    std::vector<std::string_view> child_expr_name;

    doris::vectorized::Array params;
    // prepare for argument
    for (int i = 0; i < _input_exprs_ctxs.size(); ++i) {
        auto data_type = _input_exprs_ctxs[i]->root()->data_type();
        argument_types.emplace_back(data_type);
        child_expr_name.emplace_back(_input_exprs_ctxs[i]->root()->expr_name());
    }

    _function = AggregateFunctionSimpleFactory::instance().get(
            _fn.name.function_name, argument_types, params, _data_type->is_nullable());
    if (_function == nullptr) {
        return Status::InternalError(
                fmt::format("Agg Function {} is not implemented", _fn.name.function_name));
    }

    _expr_name = fmt::format("{}({})", _fn.name.function_name, child_expr_name);
    return Status::OK();
}

Status AggFnEvaluator::open(RuntimeState* state) {
    return VExpr::open(_input_exprs_ctxs, state);
}

void AggFnEvaluator::close(RuntimeState* state) {
    VExpr::close(_input_exprs_ctxs, state);
}
void AggFnEvaluator::create(AggregateDataPtr place) {
    _function->create(place);
}
void AggFnEvaluator::destroy(AggregateDataPtr place) {
    _function->destroy(place);
}

void AggFnEvaluator::execute_single_add(Block* block, AggregateDataPtr place, Arena* arena) {
    _calc_argment_columns(block);
    SCOPED_TIMER(_exec_timer);
    _function->add_batch_single_place(block->rows(), place, _agg_columns.data(), arena);
}

void AggFnEvaluator::execute_batch_add(Block* block, size_t offset, AggregateDataPtr* places,
                                       Arena* arena) {
    _calc_argment_columns(block);
    SCOPED_TIMER(_exec_timer);
    _function->add_batch(block->rows(), places, offset, _agg_columns.data(), arena);
}

void AggFnEvaluator::insert_result_info(AggregateDataPtr place, IColumn* column) {
    _function->insert_result_into(place, *column);
}

void AggFnEvaluator::reset(AggregateDataPtr place) {
    _function->reset(place);
}

std::string AggFnEvaluator::debug_string(const std::vector<AggFnEvaluator*>& exprs) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < exprs.size(); ++i) {
        out << (i == 0 ? "" : " ") << exprs[i]->debug_string();
    }

    out << "]";
    return out.str();
}

std::string AggFnEvaluator::debug_string() const {
    std::stringstream out;
    out << "AggFnEvaluator(";
    out << ")";
    return out.str();
}

void AggFnEvaluator::_calc_argment_columns(Block* block) {
    SCOPED_TIMER(_expr_timer);
    _agg_columns.resize(_input_exprs_ctxs.size());
    int column_ids[_input_exprs_ctxs.size()];
    for (int i = 0; i < _input_exprs_ctxs.size(); ++i) {
        int column_id = -1;
        _input_exprs_ctxs[i]->execute(block, &column_id);
        column_ids[i] = column_id;
    }
    materialize_block_inplace(*block, column_ids, column_ids + _input_exprs_ctxs.size());
    for (int i = 0; i < _input_exprs_ctxs.size(); ++i) {
        _agg_columns[i] = block->get_by_position(column_ids[i]).column.get();
    }
}

} // namespace doris::vectorized
