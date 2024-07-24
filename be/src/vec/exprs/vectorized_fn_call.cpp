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
#include <gen_cpp/Types_types.h>

#include <ostream>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "runtime/runtime_state.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/exprs/vexpr_context.h"
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
            if (_data_type->get_type_as_type_descriptor().type != PrimitiveType::TYPE_AGG_STATE) {
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
                _fn.name.function_name, argument_template, _data_type, state->be_exec_version());
    }
    if (_function == nullptr) {
        return Status::InternalError(
                "Function {} get failed, expr is {} "
                "and return type is {}.",
                _fn.name.function_name, _expr_name, _data_type->get_name());
    }
    VExpr::register_function_context(state, context);
    _function_name = _fn.name.function_name;
    _can_fast_execute = can_fast_execute();
    _prepare_finished = true;
    return Status::OK();
}

Status VectorizedFnCall::open(RuntimeState* state, VExprContext* context,
                              FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    RETURN_IF_ERROR(VExpr::init_function_context(context, scope, _function));
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

Status VectorizedFnCall::eval_inverted_index(
        VExprContext* context,
        const std::unordered_map<ColumnId, std::pair<vectorized::IndexFieldNameAndTypePair,
                                                     segment_v2::InvertedIndexIterator*>>&
                colid_to_inverted_index_iter,
        uint32_t num_rows, roaring::Roaring* bitmap) const {
    DCHECK_GE(get_num_children(), 1);
    if (get_child(0)->is_slot_ref()) {
        auto* column_slot_ref = assert_cast<VSlotRef*>(get_child(0).get());
        if (auto iter = colid_to_inverted_index_iter.find(column_slot_ref->column_id());
            iter != colid_to_inverted_index_iter.end()) {
            const auto& pair = iter->second;
            return _function->eval_inverted_index(context->fn_context(_fn_context_index),
                                                  pair.first, pair.second, num_rows, bitmap);
        } else {
            return Status::NotSupported("column id {} not found in colid_to_inverted_index_iter",
                                        column_slot_ref->column_id());
        }
    } else {
        return Status::NotSupported("we can only eval inverted index for slot ref expr, but got ",
                                    get_child(0)->expr_name());
    }
    return Status::OK();
}

Status VectorizedFnCall::_do_execute(doris::vectorized::VExprContext* context,
                                     doris::vectorized::Block* block, int* result_column_id,
                                     std::vector<size_t>& args) {
    if (is_const_and_have_executed()) { // const have execute in open function
        return get_result_from_const(block, _expr_name, result_column_id);
    }

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
    size_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});
    if (_can_fast_execute) {
        auto can_fast_execute = fast_execute(*block, args, num_columns_without_result,
                                             block->rows(), _function->get_name());
        if (can_fast_execute) {
            *result_column_id = num_columns_without_result;
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_function->execute(context->fn_context(_fn_context_index), *block, args,
                                       num_columns_without_result, block->rows(), false));
    *result_column_id = num_columns_without_result;
    return Status::OK();
}

Status VectorizedFnCall::execute_runtime_fitler(doris::vectorized::VExprContext* context,
                                                doris::vectorized::Block* block,
                                                int* result_column_id, std::vector<size_t>& args) {
    return _do_execute(context, block, result_column_id, args);
}

Status VectorizedFnCall::execute(VExprContext* context, vectorized::Block* block,
                                 int* result_column_id) {
    std::vector<size_t> arguments;
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

bool VectorizedFnCall::can_fast_execute() const {
    auto function_name = _function->get_name();
    if (function_name == "eq" || function_name == "ne" || function_name == "lt" ||
        function_name == "gt" || function_name == "le" || function_name == "ge") {
        if (_children.size() == 2 && _children[0]->is_slot_ref() && _children[1]->is_literal()) {
            return true;
        }
    }
    return _function->can_push_down_to_index();
}

Status VectorizedFnCall::eval_inverted_index(VExprContext* context,
                                             segment_v2::FuncExprParams& params) {
    return _function->eval_inverted_index(context->fn_context(_fn_context_index), params);
}

bool VectorizedFnCall::equals(const VExpr& other) {
    const auto* other_ptr = dynamic_cast<const VectorizedFnCall*>(&other);
    if (!other_ptr) {
        return false;
    }
    if (this->_function_name != other_ptr->_function_name) {
        return false;
    }
    if (this->children().size() != other_ptr->children().size()) {
        return false;
    }
    for (size_t i = 0; i < this->children().size(); i++) {
        if (!this->get_child(i)->equals(*other_ptr->get_child(i))) {
            return false;
        }
    }
    return true;
}

} // namespace doris::vectorized
