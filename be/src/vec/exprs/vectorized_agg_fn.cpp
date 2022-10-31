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
#include "vec/aggregate_functions/aggregate_function_java_udaf.h"
#include "vec/aggregate_functions/aggregate_function_rpc.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/aggregate_function_sort.h"
#include "vec/core/materialize_block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

AggFnEvaluator::AggFnEvaluator(const TExprNode& desc)
        : _fn(desc.fn),
          _is_merge(desc.agg_expr.is_merge_agg),
          _return_type(TypeDescriptor::from_thrift(desc.fn.ret_type)),
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

    if (desc.agg_expr.__isset.param_types) {
        auto& param_types = desc.agg_expr.param_types;
        for (int i = 0; i < param_types.size(); i++) {
            _argument_types_with_sort.push_back(
                    DataTypeFactory::instance().create_data_type(param_types[i]));
        }
    }
}

Status AggFnEvaluator::create(ObjectPool* pool, const TExpr& desc, const TSortInfo& sort_info,
                              AggFnEvaluator** result) {
    *result = pool->add(new AggFnEvaluator(desc.nodes[0]));
    auto& agg_fn_evaluator = *result;
    int node_idx = 0;
    for (int i = 0; i < desc.nodes[0].num_children; ++i) {
        ++node_idx;
        VExpr* expr = nullptr;
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(
                VExpr::create_tree_from_thrift(pool, desc.nodes, nullptr, &node_idx, &expr, &ctx));
        agg_fn_evaluator->_input_exprs_ctxs.push_back(ctx);
    }

    auto sort_size = sort_info.ordering_exprs.size();
    auto real_arguments_size = agg_fn_evaluator->_argument_types_with_sort.size() - sort_size;
    // Child arguments conatins [real arguments, order by arguments], we pass the arguments
    // to the order by functions
    for (int i = 0; i < sort_size; ++i) {
        agg_fn_evaluator->_sort_description.emplace_back(real_arguments_size + i,
                                                         sort_info.is_asc_order[i] == true,
                                                         sort_info.nulls_first[i] == true);
    }

    // Pass the real arguments to get functions
    for (int i = 0; i < real_arguments_size; ++i) {
        agg_fn_evaluator->_real_argument_types.emplace_back(
                agg_fn_evaluator->_argument_types_with_sort[i]);
    }
    return Status::OK();
}

Status AggFnEvaluator::prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                               const SlotDescriptor* intermediate_slot_desc,
                               const SlotDescriptor* output_slot_desc) {
    DCHECK(pool != nullptr);
    DCHECK(intermediate_slot_desc != nullptr);
    DCHECK(_intermediate_slot_desc == nullptr);
    _output_slot_desc = output_slot_desc;
    _intermediate_slot_desc = intermediate_slot_desc;

    Status status = VExpr::prepare(_input_exprs_ctxs, state, desc);
    RETURN_IF_ERROR(status);

    DataTypes tmp_argument_types;
    tmp_argument_types.reserve(_input_exprs_ctxs.size());

    std::vector<std::string_view> child_expr_name;

    // prepare for argument
    for (int i = 0; i < _input_exprs_ctxs.size(); ++i) {
        auto data_type = _input_exprs_ctxs[i]->root()->data_type();
        tmp_argument_types.emplace_back(data_type);
        child_expr_name.emplace_back(_input_exprs_ctxs[i]->root()->expr_name());
    }

    const DataTypes& argument_types =
            _real_argument_types.empty() ? tmp_argument_types : _real_argument_types;

    if (_fn.binary_type == TFunctionBinaryType::JAVA_UDF) {
#ifdef LIBJVM
        _function = AggregateJavaUdaf::create(_fn, argument_types, {}, _data_type);
#else
        return Status::InternalError("Java UDAF is disabled since no libjvm is found!");
#endif
    } else if (_fn.binary_type == TFunctionBinaryType::RPC) {
        _function = AggregateRpcUdaf::create(_fn, argument_types, {}, _data_type);
    } else {
        _function = AggregateFunctionSimpleFactory::instance().get(
                _fn.name.function_name, argument_types, {}, _data_type->is_nullable());
    }
    if (_function == nullptr) {
        return Status::InternalError("Agg Function {} is not implemented", _fn.name.function_name);
    }

    if (!_sort_description.empty()) {
        _function = transform_to_sort_agg_function(_function, _argument_types_with_sort,
                                                   _sort_description, state);
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
                                       Arena* arena, bool agg_many) {
    _calc_argment_columns(block);
    SCOPED_TIMER(_exec_timer);
    _function->add_batch(block->rows(), places, offset, _agg_columns.data(), arena, agg_many);
}

void AggFnEvaluator::execute_batch_add_selected(Block* block, size_t offset,
                                                AggregateDataPtr* places, Arena* arena) {
    _calc_argment_columns(block);
    SCOPED_TIMER(_exec_timer);
    _function->add_batch_selected(block->rows(), places, offset, _agg_columns.data(), arena);
}

void AggFnEvaluator::streaming_agg_serialize(Block* block, BufferWritable& buf,
                                             const size_t num_rows, Arena* arena) {
    _calc_argment_columns(block);
    SCOPED_TIMER(_exec_timer);
    _function->streaming_agg_serialize(_agg_columns.data(), buf, num_rows, arena);
}

void AggFnEvaluator::streaming_agg_serialize_to_column(Block* block, MutableColumnPtr& dst,
                                                       const size_t num_rows, Arena* arena) {
    _calc_argment_columns(block);
    SCOPED_TIMER(_exec_timer);
    _function->streaming_agg_serialize_to_column(_agg_columns.data(), dst, num_rows, arena);
}

void AggFnEvaluator::insert_result_info(AggregateDataPtr place, IColumn* column) {
    _function->insert_result_into(place, *column);
}

void AggFnEvaluator::insert_result_info_vec(const std::vector<AggregateDataPtr>& places,
                                            size_t offset, IColumn* column, const size_t num_rows) {
    _function->insert_result_into_vec(places, offset, *column, num_rows);
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
