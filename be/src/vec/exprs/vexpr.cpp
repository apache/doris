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

#include <gen_cpp/Exprs_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <stack>

#include "common/config.h"
#include "common/exception.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_factory.hpp"
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
TExprNode create_texpr_node_from(const void* data, const PrimitiveType& type, int precision,
                                 int scale) {
    TExprNode node;

    switch (type) {
    case TYPE_BOOLEAN: {
        static_cast<void>(create_texpr_literal_node<TYPE_BOOLEAN>(data, &node));
        break;
    }
    case TYPE_TINYINT: {
        static_cast<void>(create_texpr_literal_node<TYPE_TINYINT>(data, &node));
        break;
    }
    case TYPE_SMALLINT: {
        static_cast<void>(create_texpr_literal_node<TYPE_SMALLINT>(data, &node));
        break;
    }
    case TYPE_INT: {
        static_cast<void>(create_texpr_literal_node<TYPE_INT>(data, &node));
        break;
    }
    case TYPE_BIGINT: {
        static_cast<void>(create_texpr_literal_node<TYPE_BIGINT>(data, &node));
        break;
    }
    case TYPE_LARGEINT: {
        static_cast<void>(create_texpr_literal_node<TYPE_LARGEINT>(data, &node));
        break;
    }
    case TYPE_FLOAT: {
        static_cast<void>(create_texpr_literal_node<TYPE_FLOAT>(data, &node));
        break;
    }
    case TYPE_DOUBLE: {
        static_cast<void>(create_texpr_literal_node<TYPE_DOUBLE>(data, &node));
        break;
    }
    case TYPE_DATEV2: {
        static_cast<void>(create_texpr_literal_node<TYPE_DATEV2>(data, &node));
        break;
    }
    case TYPE_DATETIMEV2: {
        static_cast<void>(create_texpr_literal_node<TYPE_DATETIMEV2>(data, &node));
        break;
    }
    case TYPE_DATE: {
        static_cast<void>(create_texpr_literal_node<TYPE_DATE>(data, &node));
        break;
    }
    case TYPE_DATETIME: {
        static_cast<void>(create_texpr_literal_node<TYPE_DATETIME>(data, &node));
        break;
    }
    case TYPE_DECIMALV2: {
        static_cast<void>(create_texpr_literal_node<TYPE_DECIMALV2>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL32: {
        static_cast<void>(create_texpr_literal_node<TYPE_DECIMAL32>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL64: {
        static_cast<void>(create_texpr_literal_node<TYPE_DECIMAL64>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL128I: {
        static_cast<void>(
                create_texpr_literal_node<TYPE_DECIMAL128I>(data, &node, precision, scale));
        break;
    }
    case TYPE_DECIMAL256: {
        static_cast<void>(
                create_texpr_literal_node<TYPE_DECIMAL256>(data, &node, precision, scale));
        break;
    }
    case TYPE_CHAR: {
        static_cast<void>(create_texpr_literal_node<TYPE_CHAR>(data, &node));
        break;
    }
    case TYPE_VARCHAR: {
        static_cast<void>(create_texpr_literal_node<TYPE_VARCHAR>(data, &node));
        break;
    }
    case TYPE_STRING: {
        static_cast<void>(create_texpr_literal_node<TYPE_STRING>(data, &node));
        break;
    }
    default:
        DCHECK(false);
        throw std::invalid_argument("Invalid type!");
    }
    return node;
}
} // namespace doris

namespace doris::vectorized {

VExpr::VExpr(const TExprNode& node)
        : _node_type(node.node_type),
          _opcode(node.__isset.opcode ? node.opcode : TExprOpcode::INVALID_OPCODE),
          _type(TypeDescriptor::from_thrift(node.type)),
          _fn_context_index(-1),
          _prepared(false) {
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

VExpr::VExpr(const TypeDescriptor& type, bool is_slotref, bool is_nullable)
        : _opcode(TExprOpcode::INVALID_OPCODE),
          _type(type),
          _fn_context_index(-1),
          _prepared(false) {
    if (is_slotref) {
        _node_type = TExprNodeType::SLOT_REF;
    }

    _data_type = DataTypeFactory::instance().create_data_type(_type, is_nullable);
}

Status VExpr::prepare(RuntimeState* state, const RowDescriptor& row_desc, VExprContext* context) {
    ++context->_depth_num;
    if (context->_depth_num > config::max_depth_of_expr_tree) {
        return Status::InternalError(
                "The depth of the expression tree is too big, make it less than {}",
                config::max_depth_of_expr_tree);
    }

    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state, row_desc, context));
    }
    --context->_depth_num;
    return Status::OK();
}

Status VExpr::open(RuntimeState* state, VExprContext* context,
                   FunctionContext::FunctionStateScope scope) {
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->open(state, context, scope));
    }
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(VExpr::get_const_col(context, nullptr));
    }
    return Status::OK();
}

void VExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->close(context, scope);
    }
}

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
    s.push({root, root_children});
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
            s.push({expr, num_children});
        }
    }
    return Status::OK();
}

Status VExpr::create_expr_tree(const TExpr& texpr, VExprContextSPtr& ctx) {
    if (texpr.nodes.size() == 0) {
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
    for (int i = 0; i < texprs.size(); ++i) {
        VExprContextSPtr ctx;
        RETURN_IF_ERROR(create_expr_tree(texprs[i], ctx));
        ctxs.push_back(ctx);
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
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->open(state));
    }
    return Status::OK();
}

Status VExpr::clone_if_not_exists(const VExprContextSPtrs& ctxs, RuntimeState* state,
                                  VExprContextSPtrs& new_ctxs) {
    if (!new_ctxs.empty()) {
        // 'ctxs' was already cloned into '*new_ctxs', nothing to do.
        DCHECK_EQ(new_ctxs.size(), ctxs.size());
        for (int i = 0; i < new_ctxs.size(); ++i) {
            DCHECK(new_ctxs[i]->_is_clone);
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
    for (int i = 0; i < ctxs.size(); ++i) {
        exprs.push_back(ctxs[i]->root());
    }
    return debug_string(exprs);
}

bool VExpr::is_constant() const {
    for (int i = 0; i < _children.size(); ++i) {
        if (!_children[i]->is_constant()) {
            return false;
        }
    }

    return true;
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
    RETURN_IF_ERROR(execute(context, &block, &result));
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
    for (int i = 0; i < _children.size(); ++i) {
        arg_types.push_back(_children[i]->type());
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

} // namespace doris::vectorized
