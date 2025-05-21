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

#include "vec/exprs/vectorized_fn_call.h"

#include <fmt/format.h>
#include <fmt/ranges.h> // IWYU pragma: keep
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/Types_types.h>

#include <memory>
#include <ostream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "common/utils.h"
#include "olap/rowset/segment_v2/ann_index_iterator.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/index_reader.h"
#include "olap/rowset/segment_v2/virtual_column_iterator.h"
#include "pipeline/pipeline_task.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/virtual_slot_ref.h"
#include "vec/exprs/vliteral.h"
#include "vec/functions/array/function_array_distance.h"
#include "vec/functions/function_agg_state.h"
#include "vec/functions/function_fake.h"
#include "vec/functions/function_java_udf.h"
#include "vec/functions/function_rpc.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

const std::string AGG_STATE_SUFFIX = "_state";

VectorizedFnCall::VectorizedFnCall(const TExprNode& node) : VExpr(node) {}

Status VectorizedFnCall::prepare(RuntimeState* state, const RowDescriptor& desc,
                                 VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    ColumnsWithTypeAndName argument_template;
    argument_template.reserve(_children.size());
    for (auto child : _children) {
        argument_template.emplace_back(nullptr, child->data_type(), child->expr_name());
    }

    _expr_name = fmt::format("VectorizedFnCall[{}](arguments={},return={})", _fn.name.function_name,
                             get_child_names(), _data_type->get_name());
    if (_fn.binary_type == TFunctionBinaryType::RPC) {
        _function = FunctionRPC::create(_fn, argument_template, _data_type);
    } else if (_fn.binary_type == TFunctionBinaryType::JAVA_UDF) {
        if (config::enable_java_support) {
            if (_fn.is_udtf_function) {
                // fake function. it's no use and can't execute.
                auto builder =
                        std::make_shared<DefaultFunctionBuilder>(FunctionFake<UDTFImpl>::create());
                _function = builder->build(argument_template, std::make_shared<DataTypeUInt8>());
            } else {
                _function = JavaFunctionCall::create(_fn, argument_template, _data_type);
            }
        } else {
            return Status::InternalError(
                    "Java UDF is not enabled, you can change be config enable_java_support to true "
                    "and restart be.");
        }
    } else if (_fn.binary_type == TFunctionBinaryType::AGG_STATE) {
        DataTypes argument_types;
        for (auto column : argument_template) {
            argument_types.emplace_back(column.type);
        }

        if (match_suffix(_fn.name.function_name, AGG_STATE_SUFFIX)) {
            if (_data_type->is_nullable()) {
                return Status::InternalError("State function's return type must be not nullable");
            }
            if (_data_type->get_primitive_type() != PrimitiveType::TYPE_AGG_STATE) {
                return Status::InternalError(
                        "State function's return type must be agg_state but get {}",
                        _data_type->get_family_name());
            }
            _function = FunctionAggState::create(
                    argument_types, _data_type,
                    assert_cast<const DataTypeAggState*>(_data_type.get())->get_nested_function());
        } else {
            return Status::InternalError("Function {} is not endwith '_state'", _fn.signature);
        }
    } else {
        // get the function. won't prepare function.
        _function = SimpleFunctionFactory::instance().get_function(
                _fn.name.function_name, argument_template, _data_type,
                {.enable_decimal256 = state->enable_decimal256()}, state->be_exec_version());
    }
    if (_function == nullptr) {
        return Status::InternalError("Could not find function {}, arg {} return {} ",
                                     _fn.name.function_name, get_child_names(),
                                     _data_type->get_name());
    }
    VExpr::register_function_context(state, context);
    _function_name = _fn.name.function_name;
    _prepare_finished = true;

    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    if (fn().__isset.dict_function) {
        fn_ctx->set_dict_function(fn().dict_function);
    }
    return Status::OK();
}

Status VectorizedFnCall::open(RuntimeState* state, VExprContext* context,
                              FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(state, context, scope, _function));
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }

    _open_finished = true;
    return Status::OK();
}

void VectorizedFnCall::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    VExpr::close_function_context(context, scope, _function);
    VExpr::close(context, scope);
}

Status VectorizedFnCall::evaluate_inverted_index(VExprContext* context, uint32_t segment_num_rows) {
    DCHECK_GE(get_num_children(), 1);
    return _evaluate_inverted_index(context, _function, segment_num_rows);
}

Status VectorizedFnCall::_do_execute(doris::vectorized::VExprContext* context,
                                     doris::vectorized::Block* block, int* result_column_id,
                                     ColumnNumbers& args) {
    if (is_const_and_have_executed()) { // const have executed in open function
        return get_result_from_const(block, _expr_name, result_column_id);
    }
    if (fast_execute(context, block, result_column_id)) {
        return Status::OK();
    }
    DBUG_EXECUTE_IF("VectorizedFnCall.must_in_slow_path", {
        if (get_child(0)->is_slot_ref()) {
            auto debug_col_name = DebugPoints::instance()->get_debug_param_or_default<std::string>(
                    "VectorizedFnCall.must_in_slow_path", "column_name", "");

            std::vector<std::string> column_names;
            boost::split(column_names, debug_col_name, boost::algorithm::is_any_of(","));

            auto* column_slot_ref = assert_cast<VSlotRef*>(get_child(0).get());
            std::string column_name = column_slot_ref->expr_name();
            auto it = std::find(column_names.begin(), column_names.end(), column_name);
            if (it == column_names.end()) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "column {} should in slow path while VectorizedFnCall::execute.",
                        column_name);
            }
        }
    })
    DCHECK(_open_finished || _getting_const_col) << debug_string();
    // TODO: not execute const expr again, but use the const column in function context
    args.resize(_children.size());
    for (int i = 0; i < _children.size(); ++i) {
        int column_id = -1;
        RETURN_IF_ERROR(_children[i]->execute(context, block, &column_id));
        args[i] = column_id;
    }

    RETURN_IF_ERROR(check_constant(*block, args));
    // call function
    uint32_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});

    DBUG_EXECUTE_IF("VectorizedFnCall.wait_before_execute", {
        auto possibility = DebugPoints::instance()->get_debug_param_or_default<double>(
                "VectorizedFnCall.wait_before_execute", "possibility", 0);
        if (random_bool_slow(possibility)) {
            LOG(WARNING) << "VectorizedFnCall::execute sleep 30s";
            sleep(30);
        }
    });

    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), *block, args,
                                       num_columns_without_result, block->rows(), false));
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

size_t VectorizedFnCall::estimate_memory(const size_t rows) {
    if (is_const_and_have_executed()) { // const have execute in open function
        return 0;
    }

    size_t estimate_size = 0;
    for (auto& child : _children) {
        estimate_size += child->estimate_memory(rows);
    }

    if (_data_type->have_maximum_size_of_value()) {
        estimate_size += rows * _data_type->get_size_of_value_in_memory();
    } else {
        estimate_size += rows * 512; /// FIXME: estimated value...
    }
    return estimate_size;
}

Status VectorizedFnCall::execute_runtime_fitler(doris::vectorized::VExprContext* context,
                                                doris::vectorized::Block* block,
                                                int* result_column_id, ColumnNumbers& args) {
    return _do_execute(context, block, result_column_id, args);
}

Status VectorizedFnCall::execute(VExprContext* context, vectorized::Block* block,
                                 int* result_column_id) {
    ColumnNumbers arguments;
    return _do_execute(context, block, result_column_id, arguments);
}

const std::string& VectorizedFnCall::expr_name() const {
    return _expr_name;
}

std::string VectorizedFnCall::debug_string() const {
    std::stringstream out;
    out << "VectorizedFn[";
    out << _expr_name;
    out << "]{";
    bool first = true;
    for (const auto& input_expr : children()) {
        if (first) {
            first = false;
        } else {
            out << ",";
        }
        out << "\n" << input_expr->debug_string();
    }
    out << "}";
    return out.str();
}

std::string VectorizedFnCall::debug_string(const std::vector<VectorizedFnCall*>& agg_fns) {
    std::stringstream out;
    out << "[";
    for (int i = 0; i < agg_fns.size(); ++i) {
        out << (i == 0 ? "" : " ") << agg_fns[i]->debug_string();
    }
    out << "]";
    return out.str();
}

bool VectorizedFnCall::can_push_down_to_index() const {
    return _function->can_push_down_to_index();
}

bool VectorizedFnCall::equals(const VExpr& other) {
    const auto* other_ptr = dynamic_cast<const VectorizedFnCall*>(&other);
    if (!other_ptr) {
        return false;
    }
    if (this->_function_name != other_ptr->_function_name) {
        return false;
    }
    if (get_num_children() != other_ptr->get_num_children()) {
        return false;
    }
    for (uint16_t i = 0; i < get_num_children(); i++) {
        if (!this->get_child(i)->equals(*other_ptr->get_child(i))) {
            return false;
        }
    }
    return true;
}

/*
    FuncationCall(LE/LT/GE/GT)
    |----------------
    |               |
    |               |
    VirtualSlotRef  Float64Literal 
    |
    |
    FuncationCall
    |----------------
    |               |
    |               |
    CastToArray     ArrayLiteral
    |
    |
    SlotRef
*/

Status VectorizedFnCall::prepare_ann_range_search() {
    std::set<TExprOpcode::type> ops = {TExprOpcode::GE, TExprOpcode::LE, TExprOpcode::LE,
                                       TExprOpcode::GT, TExprOpcode::LT};
    if (ops.find(this->op()) == ops.end()) {
        LOG_INFO("Not a range search function.");
        return Status::OK();
    }

    _ann_range_search_params.is_le_or_lt =
            (this->op() == TExprOpcode::LE || this->op() == TExprOpcode::LT);

    DCHECK(_children.size() == 2);

    auto left_child = get_child(0);
    auto right_child = get_child(1);

    // Return type of L2Distance is always double.
    auto right_literal = std::dynamic_pointer_cast<VLiteral>(right_child);
    if (right_literal == nullptr) {
        LOG_INFO("Right child is not a literal.");
        return Status::OK();
    }

    auto right_col = right_literal->get_column_ptr()->convert_to_full_column_if_const();
    auto right_type = right_literal->get_data_type();
    if (right_type->get_type_id() != vectorized::TypeIndex::Float64) {
        LOG_INFO("Right child is not a Float64Literal.");
        return Status::OK();
    }

    const ColumnFloat64* cf64_right = assert_cast<const ColumnFloat64*>(right_col.get());
    _ann_range_search_params.radius = cf64_right->get_data()[0];

    std::shared_ptr<VectorizedFnCall> function_call;
    auto vir_slot_ref = std::dynamic_pointer_cast<VirtualSlotRef>(left_child);
    if (vir_slot_ref != nullptr) {
        DCHECK(vir_slot_ref->get_virtual_column_expr() != nullptr);
        function_call = std::dynamic_pointer_cast<VectorizedFnCall>(
                vir_slot_ref->get_virtual_column_expr());
    } else {
        function_call = std::dynamic_pointer_cast<VectorizedFnCall>(left_child);
    }

    if (function_call == nullptr) {
        LOG_INFO("Left child is not a function call.");
        return Status::OK();
    }

    // Now left child is a function call, we need to check if it is a distance function
    if (function_call->_function_name != L2Distance::name) {
        LOG_INFO("Left child is not a distance function. Got {}", function_call->_function_name);
        return Status::OK();
    }

    if (function_call->get_num_children() != 2) {
        return Status::OK();
    }

    UInt16 idx_of_cast_to_array = 0;
    UInt16 idx_of_array_literal = 0;
    for (UInt16 i = 0; i < function_call->get_num_children(); ++i) {
        auto child = function_call->get_child(i);
        if (std::dynamic_pointer_cast<VCastExpr>(child) != nullptr) {
            idx_of_cast_to_array = i;
        } else if (std::dynamic_pointer_cast<VArrayLiteral>(child) != nullptr) {
            idx_of_array_literal = i;
        }
    }

    std::shared_ptr<VCastExpr> cast_to_array_expr =
            std::dynamic_pointer_cast<VCastExpr>(function_call->get_child(idx_of_cast_to_array));
    std::shared_ptr<VArrayLiteral> array_literal = std::dynamic_pointer_cast<VArrayLiteral>(
            function_call->get_child(idx_of_array_literal));

    if (cast_to_array_expr == nullptr || array_literal == nullptr) {
        LOG_INFO("Cast to array expr or array literal is null.");
        return Status::OK();
    }

    // One of the children is a slot ref, and the other is an array literal, now begin to create search params.
    std::shared_ptr<VSlotRef> slot_ref =
            std::dynamic_pointer_cast<VSlotRef>(cast_to_array_expr->get_child(0));
    if (slot_ref == nullptr) {
        LOG_INFO("Cast to array expr's child is not a slot ref.");
        return Status::OK();
    }

    _ann_range_search_params.src_col_idx = slot_ref->column_id();
    _ann_range_search_params.dst_col_idx = vir_slot_ref == nullptr ? -1 : vir_slot_ref->column_id();
    auto col_const = array_literal->get_column_ptr();
    auto col_array = col_const->convert_to_full_column_if_const();
    const ColumnArray* array_col = assert_cast<const ColumnArray*>(col_array.get());
    DCHECK(array_col->size() == 1);
    size_t dim = array_col->get_offsets()[0];
    _ann_range_search_params.query_value = std::make_unique<float[]>(dim);

    const ColumnNullable* cn = assert_cast<const ColumnNullable*>(array_col->get_data_ptr().get());
    const ColumnFloat64* cf64 =
            assert_cast<const ColumnFloat64*>(cn->get_nested_column_ptr().get());
    for (size_t i = 0; i < dim; ++i) {
        _ann_range_search_params.query_value[i] = static_cast<Float32>(cf64->get_data()[i]);
    }
    _ann_range_search_params.is_ann_range_search = true;
    LOG_INFO("Ann range search params: {}", _ann_range_search_params.to_string());
    return Status::OK();
}

Status VectorizedFnCall::evaluate_ann_range_search(
        const std::vector<std::unique_ptr<segment_v2::IndexIterator>>& cid_to_index_iterators,
        const std::vector<ColumnId>& idx_to_cid,
        const std::vector<std::unique_ptr<segment_v2::ColumnIterator>>& column_iterators,
        roaring::Roaring& row_bitmap) {
    if (_ann_range_search_params.is_ann_range_search == false) {
        return Status::OK();
    }
    LOG_INFO("Try apply ann range search. Local search params: {}",
             _ann_range_search_params.to_string());
    size_t origin_num = row_bitmap.cardinality();

    int idx_in_block = static_cast<int>(_ann_range_search_params.src_col_idx);
    DCHECK(idx_in_block < idx_to_cid.size())
            << "idx_in_block: " << idx_in_block << ", idx_to_cid.size(): " << idx_to_cid.size();

    ColumnId src_col_cid = idx_to_cid[idx_in_block];
    DCHECK(src_col_cid < cid_to_index_iterators.size());
    segment_v2::IndexIterator* index_iterators = cid_to_index_iterators[src_col_cid].get();
    if (index_iterators == nullptr) {
        LOG_INFO("No index iterator for column cid {}", src_col_cid);
        return Status::OK();
    }

    segment_v2::AnnIndexIterator* ann_index_iterators =
            dynamic_cast<segment_v2::AnnIndexIterator*>(index_iterators);
    if (ann_index_iterators == nullptr) {
        LOG_INFO("No index iterator for column cid {}", src_col_cid);
        return Status::OK();
    }

    RangeSearchParams params = _ann_range_search_params.toRangeSearchParams();
    CustomSearchParams custom_params = _ann_range_search_params.toCustomSearchParams();

    params.roaring = &row_bitmap;
    DCHECK(params.roaring != nullptr);
    RangeSearchResult result;
    RETURN_IF_ERROR(ann_index_iterators->range_search(params, custom_params, &result));

#ifndef NDEBUG
    if (this->_ann_range_search_params.is_le_or_lt == false) {
        DCHECK(result.distance == nullptr) << "Should not have distance";
    }
#endif

    DCHECK(result.roaring != nullptr);
    row_bitmap = *result.roaring;

    if (params.is_le_or_lt == false) {
        DCHECK(result.distance == nullptr);
        DCHECK(result.row_ids == nullptr);
    }

    // Process virtual column
    if (_ann_range_search_params.dst_col_idx >= 0) {
        // Prepare materialization if we can use result from index.
        // Typical situation: range search and operator is LE or LT.
        if (result.distance != nullptr) {
            DCHECK(result.row_ids != nullptr);
            ColumnId dst_col_cid = idx_to_cid[_ann_range_search_params.dst_col_idx];
            DCHECK(dst_col_cid < column_iterators.size());
            DCHECK(column_iterators[dst_col_cid] != nullptr);
            segment_v2::ColumnIterator* column_iterator = column_iterators[dst_col_cid].get();
            DCHECK(column_iterator != nullptr);
            segment_v2::VirtualColumnIterator* virtual_column_iterator =
                    dynamic_cast<segment_v2::VirtualColumnIterator*>(column_iterator);
            DCHECK(virtual_column_iterator != nullptr);
            // Now convert distance to column
            size_t size = result.roaring->cardinality();
            // TODO: need to consider nullable column.
            auto distance_col = ColumnFloat32::create();

            distance_col->insert_many_raw_data(reinterpret_cast<char*>(result.distance.get()),
                                               size);
            virtual_column_iterator->prepare_materialization(std::move(distance_col),
                                                             std::move(result.row_ids));
        } else {
            DCHECK(this->op() != TExprOpcode::LE && this->op() != TExprOpcode::LT)
                    << "Should not have distance";
        }
    }

    _has_been_executed = true;
    LOG_INFO("Ann range search filtered {} rows, origin {} rows",
             origin_num - row_bitmap.cardinality(), origin_num);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
