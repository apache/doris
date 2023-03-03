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
#include <thrift/protocol/TDebugProtocol.h>

#include <memory>

#include "gen_cpp/Exprs_types.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/varray_literal.h"
#include "vec/exprs/vcase_expr.h"
#include "vec/exprs/vcast_expr.h"
#include "vec/exprs/vcompound_pred.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vin_predicate.h"
#include "vec/exprs/vinfo_func.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vmap_literal.h"
#include "vec/exprs/vruntimefilter_wrapper.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/exprs/vstruct_literal.h"
#include "vec/exprs/vtuple_is_null_predicate.h"

namespace doris::vectorized {
using doris::Status;
using doris::RuntimeState;
using doris::RowDescriptor;
using doris::TypeDescriptor;

VExpr::VExpr(const doris::TExprNode& node)
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
    _data_type = DataTypeFactory::instance().create_data_type(_type, is_nullable);
}

VExpr::VExpr(const VExpr& vexpr)
        : _node_type(vexpr._node_type),
          _opcode(vexpr._opcode),
          _type(vexpr._type),
          _data_type(vexpr._data_type),
          _children(vexpr._children),
          _fn(vexpr._fn),
          _fn_context_index(vexpr._fn_context_index),
          _constant_col(vexpr._constant_col),
          _prepared(vexpr._prepared) {}

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
    return Status::OK();
}

void VExpr::close(doris::RuntimeState* state, VExprContext* context,
                  FunctionContext::FunctionStateScope scope) {
    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->close(state, context, scope);
    }
}

Status VExpr::create_expr(doris::ObjectPool* pool, const doris::TExprNode& texpr_node,
                          VExpr** expr) {
    switch (texpr_node.node_type) {
    case TExprNodeType::BOOL_LITERAL:
    case TExprNodeType::INT_LITERAL:
    case TExprNodeType::LARGE_INT_LITERAL:
    case TExprNodeType::FLOAT_LITERAL:
    case TExprNodeType::DECIMAL_LITERAL:
    case TExprNodeType::DATE_LITERAL:
    case TExprNodeType::STRING_LITERAL:
    case TExprNodeType::JSON_LITERAL:
    case TExprNodeType::NULL_LITERAL: {
        *expr = pool->add(new VLiteral(texpr_node));
        break;
    }
    case TExprNodeType::ARRAY_LITERAL: {
        *expr = pool->add(new VArrayLiteral(texpr_node));
        break;
    }
    case TExprNodeType::MAP_LITERAL: {
        *expr = pool->add(new VMapLiteral(texpr_node));
        break;
    }
    case TExprNodeType::STRUCT_LITERAL: {
        *expr = pool->add(new VStructLiteral(texpr_node));
        break;
    }
    case doris::TExprNodeType::SLOT_REF: {
        *expr = pool->add(new VSlotRef(texpr_node));
        break;
    }
    case doris::TExprNodeType::COMPOUND_PRED: {
        *expr = pool->add(new VcompoundPred(texpr_node));
        break;
    }
    case doris::TExprNodeType::ARITHMETIC_EXPR:
    case doris::TExprNodeType::BINARY_PRED:
    case doris::TExprNodeType::FUNCTION_CALL:
    case doris::TExprNodeType::COMPUTE_FUNCTION_CALL:
    case doris::TExprNodeType::MATCH_PRED: {
        *expr = pool->add(new VectorizedFnCall(texpr_node));
        break;
    }
    case doris::TExprNodeType::CAST_EXPR: {
        *expr = pool->add(new VCastExpr(texpr_node));
        break;
    }
    case doris::TExprNodeType::IN_PRED: {
        *expr = pool->add(new VInPredicate(texpr_node));
        break;
    }
    case doris::TExprNodeType::CASE_EXPR: {
        if (!texpr_node.__isset.case_expr) {
            return Status::InternalError("Case expression not set in thrift node");
        }
        *expr = pool->add(new VCaseExpr(texpr_node));
        break;
    }
    case TExprNodeType::INFO_FUNC: {
        *expr = pool->add(new VInfoFunc(texpr_node));
        break;
    }
    case TExprNodeType::TUPLE_IS_NULL_PRED: {
        *expr = pool->add(new VTupleIsNullPredicate(texpr_node));
        break;
    }
    default:
        return Status::InternalError("Unknown expr node type: {}", texpr_node.node_type);
    }
    if (!(*expr)->data_type()) {
        return Status::InvalidArgument("Unknown expr type: {}", texpr_node.node_type);
    }
    return Status::OK();
}

Status VExpr::create_tree_from_thrift(doris::ObjectPool* pool,
                                      const std::vector<doris::TExprNode>& nodes, VExpr* parent,
                                      int* node_idx, VExpr** root_expr, VExprContext** ctx) {
    // propagate error case
    if (*node_idx >= nodes.size()) {
        return Status::InternalError("Failed to reconstruct expression tree from thrift.");
    }
    int num_children = nodes[*node_idx].num_children;
    VExpr* expr = nullptr;
    RETURN_IF_ERROR(create_expr(pool, nodes[*node_idx], &expr));
    DCHECK(expr != nullptr);
    if (parent != nullptr) {
        parent->add_child(expr);
    } else {
        DCHECK(root_expr != nullptr);
        DCHECK(ctx != nullptr);
        *root_expr = expr;
        *ctx = pool->add(new VExprContext(expr));
    }
    for (int i = 0; i < num_children; i++) {
        *node_idx += 1;
        RETURN_IF_ERROR(create_tree_from_thrift(pool, nodes, expr, node_idx, nullptr, nullptr));
        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= nodes.size()) {
            return Status::InternalError("Failed to reconstruct expression tree from thrift.");
        }
    }
    return Status::OK();
}

Status VExpr::create_expr_tree(doris::ObjectPool* pool, const doris::TExpr& texpr,
                               VExprContext** ctx) {
    if (texpr.nodes.size() == 0) {
        *ctx = nullptr;
        return Status::OK();
    }
    int node_idx = 0;
    VExpr* e = nullptr;
    Status status = create_tree_from_thrift(pool, texpr.nodes, nullptr, &node_idx, &e, ctx);
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

Status VExpr::create_expr_trees(ObjectPool* pool, const std::vector<doris::TExpr>& texprs,
                                std::vector<VExprContext*>* ctxs) {
    ctxs->clear();
    for (int i = 0; i < texprs.size(); ++i) {
        VExprContext* ctx = nullptr;
        RETURN_IF_ERROR(create_expr_tree(pool, texprs[i], &ctx));
        ctxs->push_back(ctx);
    }
    return Status::OK();
}

Status VExpr::prepare(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                      const RowDescriptor& row_desc) {
    for (auto ctx : ctxs) {
        RETURN_IF_ERROR(ctx->prepare(state, row_desc));
    }
    return Status::OK();
}

void VExpr::close(const std::vector<VExprContext*>& ctxs, RuntimeState* state) {
    for (auto ctx : ctxs) {
        ctx->close(state);
    }
}

Status VExpr::open(const std::vector<VExprContext*>& ctxs, RuntimeState* state) {
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->open(state));
    }
    return Status::OK();
}

Status VExpr::clone_if_not_exists(const std::vector<VExprContext*>& ctxs, RuntimeState* state,
                                  std::vector<VExprContext*>* new_ctxs) {
    DCHECK(new_ctxs != nullptr);
    if (!new_ctxs->empty()) {
        // 'ctxs' was already cloned into '*new_ctxs', nothing to do.
        DCHECK_EQ(new_ctxs->size(), ctxs.size());
        for (int i = 0; i < new_ctxs->size(); ++i) {
            DCHECK((*new_ctxs)[i]->_is_clone);
        }
        return Status::OK();
    }
    new_ctxs->resize(ctxs.size());
    for (int i = 0; i < ctxs.size(); ++i) {
        RETURN_IF_ERROR(ctxs[i]->clone(state, &(*new_ctxs)[i]));
    }
    return Status::OK();
}

FunctionContext::TypeDesc VExpr::column_type_to_type_desc(const TypeDescriptor& type) {
    FunctionContext::TypeDesc out;
    switch (type.type) {
    case TYPE_BOOLEAN:
        out.type = FunctionContext::TYPE_BOOLEAN;
        break;
    case TYPE_TINYINT:
        out.type = FunctionContext::TYPE_TINYINT;
        break;
    case TYPE_SMALLINT:
        out.type = FunctionContext::TYPE_SMALLINT;
        break;
    case TYPE_INT:
        out.type = FunctionContext::TYPE_INT;
        break;
    case TYPE_BIGINT:
        out.type = FunctionContext::TYPE_BIGINT;
        break;
    case TYPE_LARGEINT:
        out.type = FunctionContext::TYPE_LARGEINT;
        break;
    case TYPE_FLOAT:
        out.type = FunctionContext::TYPE_FLOAT;
        break;
    case TYPE_TIME:
    case TYPE_TIMEV2:
    case TYPE_DOUBLE:
        out.type = FunctionContext::TYPE_DOUBLE;
        break;
    case TYPE_DATE:
        out.type = FunctionContext::TYPE_DATE;
        break;
    case TYPE_DATETIME:
        out.type = FunctionContext::TYPE_DATETIME;
        break;
    case TYPE_DATEV2:
        out.type = FunctionContext::TYPE_DATEV2;
        break;
    case TYPE_DATETIMEV2:
        out.type = FunctionContext::TYPE_DATETIMEV2;
        break;
    case TYPE_DECIMAL32:
        out.type = FunctionContext::TYPE_DECIMAL32;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_DECIMAL64:
        out.type = FunctionContext::TYPE_DECIMAL64;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_DECIMAL128I:
        out.type = FunctionContext::TYPE_DECIMAL128I;
        out.precision = type.precision;
        out.scale = type.scale;
        break;
    case TYPE_VARCHAR:
        out.type = FunctionContext::TYPE_VARCHAR;
        out.len = type.len;
        break;
    case TYPE_HLL:
        out.type = FunctionContext::TYPE_HLL;
        out.len = type.len;
        break;
    case TYPE_OBJECT:
        out.type = FunctionContext::TYPE_OBJECT;
        break;
    case TYPE_QUANTILE_STATE:
        out.type = FunctionContext::TYPE_QUANTILE_STATE;
        break;
    case TYPE_CHAR:
        out.type = FunctionContext::TYPE_CHAR;
        out.len = type.len;
        break;
    case TYPE_DECIMALV2:
        out.type = FunctionContext::TYPE_DECIMALV2;
        // out.precision = type.precision;
        // out.scale = type.scale;
        break;
    case TYPE_NULL:
        out.type = FunctionContext::TYPE_NULL;
        break;
    case TYPE_ARRAY:
        out.type = FunctionContext::TYPE_ARRAY;
        for (const auto& t : type.children) {
            out.children.push_back(VExpr::column_type_to_type_desc(t));
        }
        break;
    case TYPE_MAP:
        CHECK(type.children.size() == 2);
        // only support map key is scalar
        CHECK(!type.children[0].is_complex_type());
        out.type = FunctionContext::TYPE_MAP;
        for (const auto& t : type.children) {
            out.children.push_back(VExpr::column_type_to_type_desc(t));
        }
        break;
    case TYPE_STRUCT:
        CHECK(type.children.size() >= 1);
        out.type = FunctionContext::TYPE_STRUCT;
        for (const auto& t : type.children) {
            out.children.push_back(VExpr::column_type_to_type_desc(t));
        }
        break;
    case TYPE_STRING:
        out.type = FunctionContext::TYPE_STRING;
        out.len = type.len;
        break;
    case TYPE_JSONB:
        out.type = FunctionContext::TYPE_JSONB;
        out.len = type.len;
        break;
    default:
        DCHECK(false) << "Unknown type: " << type;
    }
    return out;
}

std::string VExpr::debug_string() const {
    // TODO: implement partial debug string for member vars
    std::stringstream out;
    out << " type=" << _type.debug_string();
    out << " codegen="
        << "false";

    if (!_children.empty()) {
        out << " children=" << debug_string(_children);
    }

    return out.str();
}

std::string VExpr::debug_string(const std::vector<VExpr*>& exprs) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < exprs.size(); ++i) {
        out << (i == 0 ? "" : " ") << exprs[i]->debug_string();
    }

    out << "]";
    return out.str();
}

std::string VExpr::debug_string(const std::vector<VExprContext*>& ctxs) {
    std::vector<VExpr*> exprs;
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
    *column_wrapper = _constant_col;
    return Status::OK();
}

void VExpr::register_function_context(doris::RuntimeState* state, VExprContext* context) {
    FunctionContext::TypeDesc return_type = VExpr::column_type_to_type_desc(_type);
    std::vector<FunctionContext::TypeDesc> arg_types;
    for (int i = 0; i < _children.size(); ++i) {
        arg_types.push_back(VExpr::column_type_to_type_desc(_children[i]->type()));
    }

    _fn_context_index = context->register_func(state, return_type, arg_types);
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
        fn_ctx->impl()->set_constant_cols(constant_cols);
    }

    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::FRAGMENT_LOCAL));
    }
    RETURN_IF_ERROR(function->open(fn_ctx, FunctionContext::THREAD_LOCAL));
    return Status::OK();
}

void VExpr::close_function_context(VExprContext* context, FunctionContext::FunctionStateScope scope,
                                   const FunctionBasePtr& function) const {
    if (_fn_context_index != -1 && !context->_stale) {
        FunctionContext* fn_ctx = context->fn_context(_fn_context_index);
        function->close(fn_ctx, FunctionContext::THREAD_LOCAL);
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            function->close(fn_ctx, FunctionContext::FRAGMENT_LOCAL);
        }
    }
}

} // namespace doris::vectorized
