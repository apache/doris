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

#include "exprs/case_expr.h"

#include "exprs/anyval_util.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"

namespace doris {

struct CaseExprState {
    // Space to store the values being compared in the interpreted path. This makes it
    // easier to pass around AnyVal subclasses. Allocated from the runtime state's object
    // pool in Prepare().
    AnyVal* case_val;
    AnyVal* when_val;
};

CaseExpr::CaseExpr(const TExprNode& node)
        : Expr(node),
          _has_case_expr(node.case_expr.has_case_expr),
          _has_else_expr(node.case_expr.has_else_expr) {}

CaseExpr::~CaseExpr() {}

Status CaseExpr::prepare(RuntimeState* state, const RowDescriptor& desc, ExprContext* ctx) {
    RETURN_IF_ERROR(Expr::prepare(state, desc, ctx));
    register_function_context(ctx, state, 0);
    return Status::OK();
}

Status CaseExpr::open(RuntimeState* state, ExprContext* ctx,
                      FunctionContext::FunctionStateScope scope) {
    RETURN_IF_ERROR(Expr::open(state, ctx, scope));
    FunctionContext* fn_ctx = ctx->fn_context(_fn_context_index);
    CaseExprState* case_state =
            reinterpret_cast<CaseExprState*>(fn_ctx->allocate(sizeof(CaseExprState)));
    fn_ctx->set_function_state(FunctionContext::THREAD_LOCAL, case_state);
    if (_has_case_expr) {
        case_state->case_val = create_any_val(state->obj_pool(), _children[0]->type());
        case_state->when_val = create_any_val(state->obj_pool(), _children[1]->type());
    } else {
        case_state->case_val = create_any_val(state->obj_pool(), TypeDescriptor(TYPE_BOOLEAN));
        case_state->when_val = create_any_val(state->obj_pool(), _children[0]->type());
    }
    return Status::OK();
}

void CaseExpr::close(RuntimeState* state, ExprContext* ctx,
                     FunctionContext::FunctionStateScope scope) {
    if (_fn_context_index != -1) {
        FunctionContext* fn_ctx = ctx->fn_context(_fn_context_index);
        void* case_state = fn_ctx->get_function_state(FunctionContext::THREAD_LOCAL);
        fn_ctx->free(reinterpret_cast<uint8_t*>(case_state));
    }
    Expr::close(state, ctx, scope);
}

std::string CaseExpr::debug_string() const {
    std::stringstream out;
    out << "CaseExpr(has_case_expr=" << _has_case_expr << " has_else_expr=" << _has_else_expr << " "
        << Expr::debug_string() << ")";
    return out.str();
}

void CaseExpr::get_child_val(int child_idx, ExprContext* ctx, TupleRow* row, AnyVal* dst) {
    switch (_children[child_idx]->type().type) {
    case TYPE_BOOLEAN:
        *reinterpret_cast<BooleanVal*>(dst) = _children[child_idx]->get_boolean_val(ctx, row);
        break;
    case TYPE_TINYINT:
        *reinterpret_cast<TinyIntVal*>(dst) = _children[child_idx]->get_tiny_int_val(ctx, row);
        break;
    case TYPE_SMALLINT:
        *reinterpret_cast<SmallIntVal*>(dst) = _children[child_idx]->get_small_int_val(ctx, row);
        break;
    case TYPE_INT:
        *reinterpret_cast<IntVal*>(dst) = _children[child_idx]->get_int_val(ctx, row);
        break;
    case TYPE_BIGINT:
        *reinterpret_cast<BigIntVal*>(dst) = _children[child_idx]->get_big_int_val(ctx, row);
        break;
    case TYPE_FLOAT:
        *reinterpret_cast<FloatVal*>(dst) = _children[child_idx]->get_float_val(ctx, row);
        break;
    case TYPE_DOUBLE:
        *reinterpret_cast<DoubleVal*>(dst) = _children[child_idx]->get_double_val(ctx, row);
        break;
    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeVal*>(dst) = _children[child_idx]->get_datetime_val(ctx, row);
        break;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING:
        *reinterpret_cast<StringVal*>(dst) = _children[child_idx]->get_string_val(ctx, row);
        break;
    case TYPE_DECIMALV2:
        *reinterpret_cast<DecimalV2Val*>(dst) = _children[child_idx]->get_decimalv2_val(ctx, row);
        break;
    case TYPE_LARGEINT:
        *reinterpret_cast<LargeIntVal*>(dst) = _children[child_idx]->get_large_int_val(ctx, row);
        break;
    default:
        DCHECK(false) << _children[child_idx]->type();
    }
}

bool CaseExpr::any_val_eq(const TypeDescriptor& type, const AnyVal* v1, const AnyVal* v2) {
    switch (type.type) {
    case TYPE_BOOLEAN:
        return AnyValUtil::equals(type, *reinterpret_cast<const BooleanVal*>(v1),
                                  *reinterpret_cast<const BooleanVal*>(v2));
    case TYPE_TINYINT:
        return AnyValUtil::equals(type, *reinterpret_cast<const TinyIntVal*>(v1),
                                  *reinterpret_cast<const TinyIntVal*>(v2));
    case TYPE_SMALLINT:
        return AnyValUtil::equals(type, *reinterpret_cast<const SmallIntVal*>(v1),
                                  *reinterpret_cast<const SmallIntVal*>(v2));
    case TYPE_INT:
        return AnyValUtil::equals(type, *reinterpret_cast<const IntVal*>(v1),
                                  *reinterpret_cast<const IntVal*>(v2));
    case TYPE_BIGINT:
        return AnyValUtil::equals(type, *reinterpret_cast<const BigIntVal*>(v1),
                                  *reinterpret_cast<const BigIntVal*>(v2));
    case TYPE_FLOAT:
        return AnyValUtil::equals(type, *reinterpret_cast<const FloatVal*>(v1),
                                  *reinterpret_cast<const FloatVal*>(v2));
    case TYPE_DOUBLE:
        return AnyValUtil::equals(type, *reinterpret_cast<const DoubleVal*>(v1),
                                  *reinterpret_cast<const DoubleVal*>(v2));
    case TYPE_DATE:
    case TYPE_DATETIME:
        return AnyValUtil::equals(type, *reinterpret_cast<const DateTimeVal*>(v1),
                                  *reinterpret_cast<const DateTimeVal*>(v2));
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING:
        return AnyValUtil::equals(type, *reinterpret_cast<const StringVal*>(v1),
                                  *reinterpret_cast<const StringVal*>(v2));
    case TYPE_DECIMALV2:
        return AnyValUtil::equals(type, *reinterpret_cast<const DecimalV2Val*>(v1),
                                  *reinterpret_cast<const DecimalV2Val*>(v2));
    case TYPE_LARGEINT:
        return AnyValUtil::equals(type, *reinterpret_cast<const LargeIntVal*>(v1),
                                  *reinterpret_cast<const LargeIntVal*>(v2));
    default:
        DCHECK(false) << type;
        return false;
    }
}

#define CASE_COMPUTE_FN(THEN_TYPE, TYPE_NAME)                                         \
    THEN_TYPE CaseExpr::get_##TYPE_NAME(ExprContext* ctx, TupleRow* row) {            \
        FunctionContext* fn_ctx = ctx->fn_context(_fn_context_index);                 \
        CaseExprState* state = reinterpret_cast<CaseExprState*>(                      \
                fn_ctx->get_function_state(FunctionContext::THREAD_LOCAL));           \
        DCHECK(state->case_val != nullptr);                                           \
        DCHECK(state->when_val != nullptr);                                           \
        int num_children = _children.size();                                          \
        if (has_case_expr()) {                                                        \
            /* All case and when exprs return the same type */                        \
            /* (we guaranteed that during analysis). */                               \
            get_child_val(0, ctx, row, state->case_val);                              \
        } else {                                                                      \
            /* If there's no case expression, compare the when values to "true". */   \
            *reinterpret_cast<BooleanVal*>(state->case_val) = BooleanVal(true);       \
        }                                                                             \
        if (state->case_val->is_null) {                                               \
            if (has_else_expr()) {                                                    \
                /* Return else value. */                                              \
                return _children[num_children - 1]->get_##TYPE_NAME(ctx, row);        \
            } else {                                                                  \
                return THEN_TYPE::null();                                             \
            }                                                                         \
        }                                                                             \
        int loop_start = has_case_expr() ? 1 : 0;                                     \
        int loop_end = (has_else_expr()) ? num_children - 1 : num_children;           \
        for (int i = loop_start; i < loop_end; i += 2) {                              \
            get_child_val(i, ctx, row, state->when_val);                              \
            if (state->when_val->is_null) continue;                                   \
            if (any_val_eq(_children[0]->type(), state->case_val, state->when_val)) { \
                /* Return then value. */                                              \
                return _children[i + 1]->get_##TYPE_NAME(ctx, row);                   \
            }                                                                         \
        }                                                                             \
        if (has_else_expr()) {                                                        \
            /* Return else value. */                                                  \
            return _children[num_children - 1]->get_##TYPE_NAME(ctx, row);            \
        }                                                                             \
        return THEN_TYPE::null();                                                     \
    }

#define CASE_COMPUTE_FN_WRAPPER(TYPE, TYPE_NAME) CASE_COMPUTE_FN(TYPE, TYPE_NAME)

CASE_COMPUTE_FN_WRAPPER(BooleanVal, boolean_val)
CASE_COMPUTE_FN_WRAPPER(TinyIntVal, tiny_int_val)
CASE_COMPUTE_FN_WRAPPER(SmallIntVal, small_int_val)
CASE_COMPUTE_FN_WRAPPER(IntVal, int_val)
CASE_COMPUTE_FN_WRAPPER(BigIntVal, big_int_val)
CASE_COMPUTE_FN_WRAPPER(FloatVal, float_val)
CASE_COMPUTE_FN_WRAPPER(DoubleVal, double_val)
CASE_COMPUTE_FN_WRAPPER(StringVal, string_val)
CASE_COMPUTE_FN_WRAPPER(DateTimeVal, datetime_val)
CASE_COMPUTE_FN_WRAPPER(DecimalV2Val, decimalv2_val)

} // namespace doris
