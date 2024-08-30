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

#include "vec/exprs/vexpr.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string/split.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <stack>

#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vcase_expr.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vcolumn_ref.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vinfo_func.h"
#include "vec/exprs/vlambda_function_call_expr.h"
#include "vec/exprs/vlambda_function_expr.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vmap_literal.h"
#include "vec/exprs/vmatch_predicate.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vstruct_literal.h"
#include "vec/exprs/vtuple_is_null_predicate.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
class RuntimeState;

// NOLINTBEGIN(readability-function-cognitive-complexity)
// NOLINTBEGIN(readability-function-size)
TExprNode create_texpr_node_from(const void* data, const PrimitiveType& type, int precision,
                                 int scale) {
    TExprNode node;

    switch (type) {
    case TYPE_BOOLEAN: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_BOOLEAN>(data, &node));
        break;
    }
    case TYPE_TINYINT: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_TINYINT>(data, &node));
        break;
    }
    case TYPE_SMALLINT: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_SMALLINT>(data, &node));
        break;
    }
    case TYPE_INT: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_INT>(data, &node));
        break;
    }
    case TYPE_BIGINT: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_BIGINT>(data, &node));
        break;
    }
    case TYPE_LARGEINT: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_LARGEINT>(data, &node));
        break;
    }
    case TYPE_FLOAT: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_FLOAT>(data, &node));
        break;
    }
    case TYPE_DOUBLE: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DOUBLE>(data, &node));
        break;
    }
    case TYPE_DATEV2: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DATEV2>(data, &node));
        break;
    }
    case TYPE_DATETIMEV2: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DATETIMEV2>(data, &node, precision, scale));
        break;
    }
    case TYPE_DATE: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DATE>(data, &node));
        break;
    }
    case TYPE_DATETIME: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DATETIME>(data, &node));
        break;
    }
    case TYPE_DECIMALV2: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DECIMALV2>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL32: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DECIMAL32>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL64: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DECIMAL64>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL128I: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DECIMAL128I>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL256: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_DECIMAL256>(data, &node, precision, scale));
        break;
    }
    case TYPE_CHAR: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_CHAR>(data, &node));
        break;
    }
    case TYPE_VARCHAR: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_VARCHAR>(data, &node));
        break;
    }
    case TYPE_STRING: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_STRING>(data, &node));
        break;
    }
    case TYPE_IPV4: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_IPV4>(data, &node));
        break;
    }
    case TYPE_IPV6: {
        THROW_IF_ERROR(create_texpr_literal_node<TYPE_IPV6>(data, &node));
        break;
    }
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "runtime filter meet invalid type {}",
                        int(type));
    }
    return node;
}
// NOLINTEND(readability-function-size)
// NOLINTEND(readability-function-cognitive-complexity)
} // namespace doris

namespace doris::vectorized {

bool VExpr::is_acting_on_a_slot(const VExpr& expr) {
    const auto& children = expr.children();

    auto is_a_slot = std::any_of(children.begin(), children.end(),
                                 [](const auto& child) { return is_acting_on_a_slot(*child); });

    return is_a_slot ? true : (expr.node_type() == TExprNodeType::SLOT_REF);
}

VExpr::VExpr(const TExprNode& node)
        : _node_type(node.node_type),
          _opcode(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
          _type(TypeDescriptor::from_thrift(node.type)) {
    if (node.__isset.fn) {
        _fn = node.fn;
    }

    bool is_nullable = true;
    if (node.__isset.is_nullable) {
        is_nullable = node.is_nullable;
    }
    // If we define null literal ,should make nullable data type to get correct field instead of undefined ptr
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        CHECK(is_nullable);
    }
    _data_type = DataTypeFactory::instance().create_data_type(_type, is_nullable);
}

VExpr::VExpr(const VExpr& vexpr) = default;

VExpr::VExpr(TypeDescriptor type, bool is_slotref, bool is_nullable)
        : _opcode(TExprOpcode::INVALID_OPCODE), _type(std::move(type)) {
    if (is_slotref) {
        _node_type = TExprNodeType::SLOT_REF;
    }

    _data_type = DataTypeFactory::instance().create_data_type(_type, is_nullable);
}

Status VExpr::prepare(RuntimeState* state, const RowDescriptor& row_desc, VExprContext* context) {
    ++context->_depth_num;
    if (context->_depth_num > config::max_depth_of_expr_tree) {
        return Status::Error<ErrorCode::EXCEEDED_LIMIT>(
                "The depth of the expression tree is too big, make it less than {}",
                config::max_depth_of_expr_tree);
    }

    for (auto& i : _children) {
        RETURN_IF_ERROR(i->prepare(state, row_desc, context));
    }
    --context->_depth_num;
    _enable_inverted_index_query = state->query_options().enable_inverted_index_query;
    return Status::OK();
}

Status VExpr::open(RuntimeState* state, VExprContext* context,
                   FunctionContext::FunctionStateScope scope) {
    for (auto& i : _children) {
        RETURN_IF_ERROR(i->open(state, context, scope));
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    return Status::OK();
}

void VExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    for (auto& i : _children) {
        i->close(context, scope);
    }
}

// NOLINTBEGIN(readability-function-size)
Status VExpr::create_expr(const TExprNode& expr_node, VExprSPtr& expr) {
    try {
        switch (expr_node.node_type) {
        case TExprNodeType::BOOL_LITERAL:
        case TExprNodeType::INT_LITERAL:
        case TExprNodeType::LARGE_INT_LITERAL:
        case TExprNodeType::IPV4_LITERAL:
        case TExprNodeType::IPV6_LITERAL:
        case TExprNodeType::FLOAT_LITERAL:
        case TExprNodeType::DECIMAL_LITERAL:
        case TExprNodeType::DATE_LITERAL:
        case TExprNodeType::STRING_LITERAL:
        case TExprNodeType::JSON_LITERAL:
        case TExprNodeType::NULL_LITERAL: {
            expr = VLiteral::create_shared(expr_node);
            break;
        }
        case TExprNodeType::ARRAY_LITERAL: {
            expr = VArrayLiteral::create_shared(expr_node);
            break;
        }
        case TExprNodeType::MAP_LITERAL: {
            expr = VMapLiteral::create_shared(expr_node);
            break;
        }
        case TExprNodeType::STRUCT_LITERAL: {
            expr = VStructLiteral::create_shared(expr_node);
            break;
        }
        case TExprNodeType::SLOT_REF: {
            expr = VSlotRef::create_shared(expr_node);
            break;
        }
        case TExprNodeType::COLUMN_REF: {
            expr = VColumnRef::create_shared(expr_node);
            break;
        }
        case TExprNodeType::COMPOUND_PRED: {
            expr = VCompoundPred::create_shared(expr_node);
            break;
        }
        case TExprNodeType::LAMBDA_FUNCTION_EXPR: {
            expr = VLambdaFunctionExpr::create_shared(expr_node);
            break;
        }
        case TExprNodeType::LAMBDA_FUNCTION_CALL_EXPR: {
            expr = VLambdaFunctionCallExpr::create_shared(expr_node);
            break;
        }
        case TExprNodeType::ARITHMETIC_EXPR:
        case TExprNodeType::BINARY_PRED:
        case TExprNodeType::NULL_AWARE_BINARY_PRED:
        case TExprNodeType::FUNCTION_CALL:
        case TExprNodeType::COMPUTE_FUNCTION_CALL: {
            expr = VectorizedFnCall::create_shared(expr_node);
            break;
        }
        case TExprNodeType::MATCH_PRED: {
            expr = VMatchPredicate::create_shared(expr_node);
            break;
        }
        case TExprNodeType::CAST_EXPR: {
            expr = VCastExpr::create_shared(expr_node);
            break;
        }
        case TExprNodeType::IN_PRED: {
            expr = VInPredicate::create_shared(expr_node);
            break;
        }
        case TExprNodeType::CASE_EXPR: {
            if (!expr_node.__isset.case_expr) {
                return Status::InternalError("Case expression not set in thrift node");
            }
            expr = VCaseExpr::create_shared(expr_node);
            break;
        }
        case TExprNodeType::INFO_FUNC: {
            expr = VInfoFunc::create_shared(expr_node);
            break;
        }
        case TExprNodeType::TUPLE_IS_NULL_PRED: {
            expr = VTupleIsNullPredicate::create_shared(expr_node);
            break;
        }
        default:
            return Status::InternalError("Unknown expr node type: {}", expr_node.node_type);
        }
    } catch (const Exception& e) {
        if (e.code() == ErrorCode::INTERNAL_ERROR) {
            return Status::InternalError("Create Expr failed because {}\nTExprNode={}", e.what(),
                                         apache::thrift::ThriftDebugString(expr_node));
        }
        return Status::Error<false>(e.code(), "Create Expr failed because {}", e.what());
        LOG(WARNING) << "create expr failed, TExprNode={}, reason={}"
                     << apache::thrift::ThriftDebugString(expr_node) << e.what();
    }
    if (!expr->data_type()) {
        return Status::InvalidArgument("Unknown expr type: {}", expr_node.node_type);
    }
    return Status::OK();
}
// NOLINTEND(readability-function-size)

Status VExpr::create_tree_from_thrift(const std::vector<TExprNode>& nodes, int* node_idx,
                                      VExprSPtr& root_expr, VExprContextSPtr& ctx) {
    // propagate error case
    if (*node_idx >= nodes.size()) {
        return Status::InternalError("Failed to reconstruct expression tree from thrift.");
    }

    // create root expr
    int root_children = nodes[*node_idx].num_children;
    VExprSPtr root;
    RETURN_IF_ERROR(create_expr(nodes[*node_idx], root));
    DCHECK(root != nullptr);
    root_expr = root;
    ctx = std::make_shared<VExprContext>(root);
    // short path for leaf node
    if (root_children <= 0) {
        return Status::OK();
    }

    // non-recursive traversal
    std::stack<std::pair<VExprSPtr, int>> s;
    s.emplace(root, root_children);
    while (!s.empty()) {
        auto& parent = s.top();
        if (parent.second > 1) {
            parent.second -= 1;
        } else {
            s.pop();
        }

        if (++*node_idx >= nodes.size()) {
            return Status::InternalError("Failed to reconstruct expression tree from thrift.");
        }
        VExprSPtr expr;
        RETURN_IF_ERROR(create_expr(nodes[*node_idx], expr));
        DCHECK(expr != nullptr);
        parent.first->add_child(expr);
        int num_children = nodes[*node_idx].num_children;
        if (num_children > 0) {
            s.emplace(expr, num_children);
        }
    }
    return Status::OK();
}

Status VExpr::create_expr_tree(const TExpr& texpr, VExprContextSPtr& ctx) {
    if (texpr.nodes.empty()) {
        ctx = nullptr;
        return Status::OK();
    }
    int node_idx = 0;
    VExprSPtr e;
    Status status = create_tree_from_thrift(texpr.nodes, &node_idx, e, ctx);
    if (status.ok() && node_idx + 1 != texpr.nodes.size()) {
        status = Status::InternalError(
                "Expression tree only partially reconstructed. Not all thrift nodes were "
                "used.");
    }
    if (!status.ok()) {
        LOG(ERROR) << "Could not construct expr tree.\n"
                   << status << "\n"
                   << apache::thrift::ThriftDebugString(texpr);
    }
    return status;
}

Status VExpr::create_expr_trees(const std::vector<TExpr>& texprs, VExprContextSPtrs& ctxs) {
    ctxs.clear();
    for (const auto& texpr : texprs) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(create_expr_tree(texpr, ctx));
        ctxs.push_back(ctx);
    }
    return Status::OK();
}

Status VExpr::check_expr_output_type(const VExprContextSPtrs& ctxs,
                                     const RowDescriptor& output_row_desc) {
    if (ctxs.empty()) {
        return Status::OK();
    }
    auto name_and_types = VectorizedUtils::create_name_and_data_types(output_row_desc);
    if (ctxs.size() != name_and_types.size()) {
        return Status::InternalError(
                "output type size not match expr size {} , expected output size {} ", ctxs.size(),
                name_and_types.size());
    }
    auto check_type_can_be_converted = [](DataTypePtr& from, DataTypePtr& to) -> bool {
        if (to->equals(*from)) {
            return true;
        }
        if (to->is_nullable() && !from->is_nullable()) {
            return remove_nullable(to)->equals(*from);
        }
        return false;
    };
    for (int i = 0; i < ctxs.size(); i++) {
        auto real_expr_type = ctxs[i]->root()->data_type();
        auto&& [name, expected_type] = name_and_types[i];
        if (!check_type_can_be_converted(real_expr_type, expected_type)) {
            return Status::InternalError(
                    "output type not match expr type  , col name {} , expected type {} , real type "
                    "{}",
                    name, expected_type->get_name(), real_expr_type->get_name());
        }
    }
    return Status::OK();
}

Status VExpr::prepare(const VExprContextSPtrs& ctxs, RuntimeState* state,
                      const RowDescriptor& row_desc) {
    for (auto ctx : ctxs) {
        RETURN_IF_ERROR(ctx->prepare(state, row_desc));
    }
    return Status::OK();
}

Status VExpr::open(const VExprContextSPtrs& ctxs, RuntimeState* state) {
    for (const auto& ctx : ctxs) {
        RETURN_IF_ERROR(ctx->open(state));
    }
    return Status::OK();
}

Status VExpr::clone_if_not_exists(const VExprContextSPtrs& ctxs, RuntimeState* state,
                                  VExprContextSPtrs& new_ctxs) {
    if (!new_ctxs.empty()) {
        // 'ctxs' was already cloned into '*new_ctxs', nothing to do.
        DCHECK_EQ(new_ctxs.size(), ctxs.size());
        for (auto& new_ctx : new_ctxs) {
            DCHECK(new_ctx->_is_clone);
        }
        return Status::OK();
    }
    new_ctxs.resize(ctxs.size());
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->clone(state, new_ctxs[i]));
    }
    return Status::OK();
}

std::string VExpr::debug_string() const {
    // TODO: implement partial debug string for member vars
    std::stringstream out;
    out << " type=" << _type.debug_string();

    if (!_children.empty()) {
        out << " children=" << debug_string(_children);
    }

    return out.str();
}

std::string VExpr::debug_string(const VExprSPtrs& exprs) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < exprs.size(); ++i) {
        out << (i == 0 ? "" : " ") << exprs[i]->debug_string();
    }

    out << "]";
    return out.str();
}

std::string VExpr::debug_string(const VExprContextSPtrs& ctxs) {
    VExprSPtrs exprs;
    for (const auto& ctx : ctxs) {
        exprs.push_back(ctx->root());
    }
    return debug_string(exprs);
}

bool VExpr::is_constant() const {
    return std::all_of(_children.begin(), _children.end(),
                       [](const VExprSPtr& expr) { return expr->is_constant(); });
}

Status VExpr::get_const_col(VExprContext* context,
                            std::shared_ptr<ColumnPtrWrapper>* column_wrapper) {
    if (!is_constant()) {
        return Status::OK();
    }

    if (_constant_col != nullptr) {
        DCHECK(column_wrapper != nullptr);
        *column_wrapper = _constant_col;
        return Status::OK();
    }

    int result = -1;
    Block block;
    // If block is empty, some functions will produce no result. So we insert a column with
    // single value here.
    block.insert({ColumnUInt8::create(1), std::make_shared<DataTypeUInt8>(), ""});

    _getting_const_col = true;
    RETURN_IF_ERROR(execute(context, &block, &result));
    _getting_const_col = false;

    DCHECK(result != -1);
    const auto& column = block.get_by_position(result).column;
    _constant_col = std::make_shared<ColumnPtrWrapper>(column);
    if (column_wrapper != nullptr) {
        *column_wrapper = _constant_col;
    }

    return Status::OK();
}

void VExpr::register_function_context(RuntimeState* state, VExprContext* context) {
    std::vector<TypeDescriptor> arg_types;
    for (auto& i : _children) {
        arg_types.push_back(i->type());
    }

    _fn_context_index = context->register_function_context(state, _type, arg_types);
}

Status VExpr::init_function_context(VExprContext* context,
                                    FunctionContext::FunctionStateScope scope,
                                    const FunctionBasePtr& function) const {
    FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_cols;
        for (auto c : _children) {
            std::shared_ptr<ColumnPtrWrapper> const_col;
            RETURN_IF_ERROR(c->get_const_col(context, &const_col));
            constant_cols.push_back(const_col);
        }
        fn_ctx->set_constant_cols(constant_cols);
    }

    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    }
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::THREAD_LOCAL));
    return Status::OK();
}

void VExpr::close_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                   const FunctionBasePtr& function) const {
    if (_fn_context_index != -1) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        // `close_function_context` is called in VExprContext's destructor so do not throw exceptions here.
        static_cast<void>(function->close(fn_ctx, FunctionContext::THREAD_LOCAL));
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            static_cast<void>(function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
        }
    }
}

Status VExpr::check_constant(const Block& block, ColumnNumbers arguments) const {
    if (is_constant() && !VectorizedUtils::all_arguments_are_constant(block, arguments)) {
        return Status::InternalError("const check failed, expr={}", debug_string());
    }
    return Status::OK();
}

Status VExpr::get_result_from_const(vectorized::Block* block, const std::string& expr_name,
                                    int* result_column_id) {
    *result_column_id = block->columns();
    auto column = ColumnConst::create(_constant_col->column_ptr, block->rows());
    block->insert({std::move(column), _data_type, expr_name});
    return Status::OK();
}

Status VExpr::_evaluate_inverted_index(VExprContext* context, const FunctionBasePtr& function,
                                       uint32_t segment_num_rows) {
    // Pre-allocate vectors based on an estimated or known size
    std::vector<segment_v2::InvertedIndexIterator*> iterators;
    std::vector<vectorized::IndexFieldNameAndTypePair> data_type_with_names;
    std::vector<int> column_ids;
    vectorized::ColumnsWithTypeAndName arguments;
    VExprSPtrs children_exprs;

    // Reserve space to avoid multiple reallocations
    const size_t estimated_size = children().size();
    iterators.reserve(estimated_size);
    data_type_with_names.reserve(estimated_size);
    column_ids.reserve(estimated_size);
    children_exprs.reserve(estimated_size);

    auto index_context = context->get_inverted_index_context();

    // if child is cast expr, we need to ensure target data type is the same with storage data type.
    // or they are all string type
    // and if data type is array, we need to get the nested data type to ensure that.
    for (const auto& child : children()) {
        if (child->node_type() == TExprNodeType::CAST_EXPR) {
            auto* cast_expr = assert_cast<VCastExpr*>(child.get());
            DCHECK_EQ(cast_expr->children().size(), 1);
            if (cast_expr->get_child(0)->is_slot_ref()) {
                auto* column_slot_ref = assert_cast<VSlotRef*>(cast_expr->get_child(0).get());
                auto column_id = column_slot_ref->column_id();
                const auto* storage_name_type =
                        context->get_inverted_index_context()
                                ->get_storage_name_and_type_by_column_id(column_id);
                auto storage_type = remove_nullable(storage_name_type->second);
                auto target_type = cast_expr->get_target_type();
                auto origin_primitive_type = storage_type->get_type_as_type_descriptor().type;
                auto target_primitive_type = target_type->get_type_as_type_descriptor().type;
                if (is_complex_type(storage_type)) {
                    if (is_array(storage_type) && is_array(target_type)) {
                        auto nested_storage_type =
                                (assert_cast<const DataTypeArray*>(storage_type.get()))
                                        ->get_nested_type();
                        origin_primitive_type =
                                nested_storage_type->get_type_as_type_descriptor().type;
                        auto nested_target_type =
                                (assert_cast<const DataTypeArray*>(target_type.get()))
                                        ->get_nested_type();
                        target_primitive_type =
                                nested_target_type->get_type_as_type_descriptor().type;
                    } else {
                        continue;
                    }
                }
                if (origin_primitive_type != TYPE_VARIANT &&
                    (origin_primitive_type == target_primitive_type ||
                     (is_string_type(target_primitive_type) &&
                      is_string_type(origin_primitive_type)))) {
                    children_exprs.emplace_back(expr_without_cast(child));
                }
            }
        } else {
            children_exprs.emplace_back(child);
        }
    }

    if (children_exprs.empty()) {
        return Status::OK(); // Early exit if no children to process
    }

    for (const auto& child : children_exprs) {
        if (child->is_slot_ref()) {
            auto* column_slot_ref = assert_cast<VSlotRef*>(child.get());
            auto column_id = column_slot_ref->column_id();
            auto* iter =
                    context->get_inverted_index_context()->get_inverted_index_iterator_by_column_id(
                            column_id);
            //column does not have inverted index
            if (iter == nullptr) {
                continue;
            }
            const auto* storage_name_type =
                    context->get_inverted_index_context()->get_storage_name_and_type_by_column_id(
                            column_id);
            if (storage_name_type == nullptr) {
                auto err_msg = fmt::format(
                        "storage_name_type cannot be found for column {} while in {} "
                        "evaluate_inverted_index",
                        column_id, expr_name());
                LOG(ERROR) << err_msg;
                return Status::InternalError(err_msg);
            }
            iterators.emplace_back(iter);
            data_type_with_names.emplace_back(*storage_name_type);
            column_ids.emplace_back(column_id);
        } else if (child->is_literal()) {
            auto* column_literal = assert_cast<VLiteral*>(child.get());
            arguments.emplace_back(column_literal->get_column_ptr(),
                                   column_literal->get_data_type(), column_literal->expr_name());
        }
    }

    if (iterators.empty() || arguments.empty()) {
        return Status::OK(); // Nothing to evaluate or no literals to compare against
    }

    auto result_bitmap = segment_v2::InvertedIndexResultBitmap();
    auto res = function->evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                                 segment_num_rows, result_bitmap);
    if (!res.ok()) {
        return res;
    }
    if (!result_bitmap.is_empty()) {
        index_context->set_inverted_index_result_for_expr(this, result_bitmap);
        for (int column_id : column_ids) {
            index_context->set_true_for_inverted_index_status(this, column_id);
        }
        // set fast_execute when expr evaluated by inverted index correctly
        _can_fast_execute = true;
    }
    return Status::OK();
}

bool VExpr::fast_execute(doris::vectorized::VExprContext* context, doris::vectorized::Block* block,
                         int* result_column_id) {
    if (context->get_inverted_index_context() &&
        context->get_inverted_index_context()->get_inverted_index_result_column().contains(this)) {
        size_t num_columns_without_result = block->columns();
        // prepare a column to save result
        auto result_column =
                context->get_inverted_index_context()->get_inverted_index_result_column()[this];
        if (_data_type->is_nullable()) {
            block->insert(
                    {ColumnNullable::create(result_column, ColumnUInt8::create(block->rows(), 0)),
                     _data_type, expr_name()});
        } else {
            block->insert({result_column, _data_type, expr_name()});
        }
        *result_column_id = num_columns_without_result;
        return true;
    }
    return false;
}

bool VExpr::equals(const VExpr& other) {
    return false;
}

} // namespace doris::vectorized
