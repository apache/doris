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

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "exprs/anyval_util.h"
#include "runtime/runtime_state.h"
#include "gen_cpp/Exprs_types.h"

using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantInt;
using llvm::Function;
using llvm::LLVMContext;
using llvm::PHINode;
using llvm::PointerType;
using llvm::Value;

namespace doris {

struct CaseExprState {
    // Space to store the values being compared in the interpreted path. This makes it
    // easier to pass around AnyVal subclasses. Allocated from the runtime state's object
    // pool in Prepare().
    AnyVal* case_val;
    AnyVal* when_val;
};

CaseExpr::CaseExpr(const TExprNode& node) : 
        Expr(node),
        _has_case_expr(node.case_expr.has_case_expr),
        _has_else_expr(node.case_expr.has_else_expr) {
}

CaseExpr::~CaseExpr() {
}

Status CaseExpr::prepare(
        RuntimeState* state, const RowDescriptor& desc, ExprContext* ctx) {
    RETURN_IF_ERROR(Expr::prepare(state, desc, ctx));
    register_function_context(ctx, state, 0);
    return Status::OK();
}

Status CaseExpr::open(
        RuntimeState* state, ExprContext* ctx,
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

void CaseExpr::close(
        RuntimeState* state, ExprContext* ctx,
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
    out << "CaseExpr(has_case_expr=" << _has_case_expr
        << " has_else_expr=" << _has_else_expr
        << " " << Expr::debug_string() << ")";
    return out.str();
}

// Sample IR output when there is a case expression and else expression
// define i16 @CaseExpr(%"class.doris::ExprContext"* %context,
//                      %"class.doris::TupleRow"* %row) #20 {
// eval_case_expr:
//   %case_val = call i64 @GetSlotRef(%"class.doris::ExprContext"* %context,
//                                    %"class.doris::TupleRow"* %row)
//   %is_null = trunc i64 %case_val to i1
//   br i1 %is_null, label %return_else_expr, label %eval_first_when_expr
//
// eval_first_when_expr:                             ; preds = %eval_case_expr
//   %when_val = call i64 @Literal(%"class.doris::ExprContext"* %context,
//                                 %"class.doris::TupleRow"* %row)
//   %is_null1 = trunc i64 %when_val to i1
//   br i1 %is_null1, label %return_else_expr, label %check_when_expr_block
//
// check_when_expr_block:                            ; preds = %eval_first_when_expr
//   %0 = ashr i64 %when_val, 32
//   %1 = trunc i64 %0 to i32
//   %2 = ashr i64 %case_val, 32
//   %3 = trunc i64 %2 to i32
//   %eq = icmp eq i32 %3, %1
//   br i1 %eq, label %return_then_expr, label %return_else_expr
//
// return_then_expr:                                 ; preds = %check_when_expr_block
//   %then_val = call i16 @Literal12(%"class.doris::ExprContext"* %context,
//                                   %"class.doris::TupleRow"* %row)
//   ret i16 %then_val
//
// return_else_expr:                                 ; preds = %check_when_expr_block, %eval_first_when_expr, %eval_case_expr
//   %else_val = call i16 @Literal13(%"class.doris::ExprContext"* %context,
//                                   %"class.doris::TupleRow"* %row)
//   ret i16 %else_val
// }
//
// Sample IR output when there is case expression and no else expression
// define i16 @CaseExpr(%"class.doris::ExprContext"* %context,
//                      %"class.doris::TupleRow"* %row) #20 {
// eval_case_expr:
//   %case_val = call i64 @GetSlotRef(%"class.doris::ExprContext"* %context,
//                                    %"class.doris::TupleRow"* %row)
//   %is_null = trunc i64 %case_val to i1
//   br i1 %is_null, label %return_null, label %eval_first_when_expr
//
// eval_first_when_expr:                             ; preds = %eval_case_expr
//   %when_val = call i64 @Literal(%"class.doris::ExprContext"* %context,
//                                 %"class.doris::TupleRow"* %row)
//   %is_null1 = trunc i64 %when_val to i1
//   br i1 %is_null1, label %return_null, label %check_when_expr_block
//
// check_when_expr_block:                            ; preds = %eval_first_when_expr
//   %0 = ashr i64 %when_val, 32
//   %1 = trunc i64 %0 to i32
//   %2 = ashr i64 %case_val, 32
//   %3 = trunc i64 %2 to i32
//   %eq = icmp eq i32 %3, %1
//   br i1 %eq, label %return_then_expr, label %return_null
//
// return_then_expr:                                 ; preds = %check_when_expr_block
//   %then_val = call i16 @Literal12(%"class.doris::ExprContext"* %context,
//                                   %"class.doris::TupleRow"* %row)
//   ret i16 %then_val
//
// return_null:                                      ; preds = %check_when_expr_block, %eval_first_when_expr, %eval_case_expr
//   ret i16 1
// }
//
// Sample IR output when there is no case expr and else expression
// define i16 @CaseExpr(%"class.doris::ExprContext"* %context,
//                      %"class.doris::TupleRow"* %row) #20 {
// eval_first_when_expr:
//   %when_val = call i16 @Eq_IntVal_IntValWrapper1(
//       %"class.doris::ExprContext"* %context, %"class.doris::TupleRow"* %row)
//   %is_null = trunc i16 %when_val to i1
//   br i1 %is_null, label %return_else_expr, label %check_when_expr_block
//
// check_when_expr_block:                            ; preds = %eval_first_when_expr
//   %0 = ashr i16 %when_val, 8
//   %1 = trunc i16 %0 to i8
//   %val = trunc i8 %1 to i1
//   br i1 %val, label %return_then_expr, label %return_else_expr
//
// return_then_expr:                                 ; preds = %check_when_expr_block
//   %then_val = call i16 @Literal14(%"class.doris::ExprContext"* %context,
//                                   %"class.doris::TupleRow"* %row)
//   ret i16 %then_val
//
// return_else_expr:                                 ; preds = %check_when_expr_block, %eval_first_when_expr
//   %else_val = call i16 @Literal15(%"class.doris::ExprContext"* %context,
//                                   %"class.doris::TupleRow"* %row)
//   ret i16 %else_val
// }
Status CaseExpr::get_codegend_compute_fn(RuntimeState* state, Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }

    const int num_children = get_num_children();
    Function* child_fns[num_children];
    for (int i = 0; i < num_children; ++i) {
        RETURN_IF_ERROR(children()[i]->get_codegend_compute_fn(state, &child_fns[i]));
    }

    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);

    Value* args[2];
    Function* function = create_ir_function_prototype(codegen, "CaseExpr", &args);
    BasicBlock* eval_case_expr_block = NULL;

    // This is the block immediately after the when/then exprs. It will either point to a
    // block which returns the else expr, or returns NULL if no else expr is specified.
    BasicBlock* default_value_block = BasicBlock::Create(
        context, has_else_expr() ? "return_else_expr" : "return_null", function);

    // If there is a case expression, create a block to evaluate it.
    CodegenAnyVal case_val;
    BasicBlock* eval_first_when_expr_block = BasicBlock::Create(
        context, "eval_first_when_expr", function, default_value_block);
    BasicBlock* current_when_expr_block = eval_first_when_expr_block;
    if (has_case_expr()) {
        // Need at least case, when and then expr, and optionally an else expr
        DCHECK_GE(num_children, (has_else_expr()) ? 4 : 3);
        // If there is a case expr, create block eval_case_expr to evaluate the
        // case expr. Place this block before eval_first_when_expr_block
        eval_case_expr_block = BasicBlock::Create(context, "eval_case_expr",
                                                  function, eval_first_when_expr_block);
        builder.SetInsertPoint(eval_case_expr_block);
        case_val = CodegenAnyVal::create_call_wrapped(
            codegen, &builder, children()[0]->type(), child_fns[0], args, "case_val");
        builder.CreateCondBr(
            case_val.get_is_null(), default_value_block, eval_first_when_expr_block);
    } else {
        DCHECK_GE(num_children, (has_else_expr()) ? 3 : 2);
    }

    const int loop_end = (has_else_expr()) ? num_children - 1 : num_children;
    const int last_loop_iter = loop_end - 2;
    // The loop increments by two each time, because each iteration handles one when/then
    // pair. Both when and then subexpressions are single children. If there is a case expr
    // start loop at index 1. (case expr is children()[0] and has already be evaluated.
    for (int i = (has_case_expr()) ? 1 : 0; i < loop_end; i += 2) {
        BasicBlock* check_when_expr_block = BasicBlock::Create(
            context, "check_when_expr_block", function, default_value_block);
        BasicBlock* return_then_expr_block =
            BasicBlock::Create(context, "return_then_expr", function, default_value_block);

        // continue_or_exit_block either points to the next eval_next_when_expr block,
        // or points to the defaut_value_block if there are no more when/then expressions.
        BasicBlock* continue_or_exit_block = NULL;
        if (i == last_loop_iter) {
            continue_or_exit_block = default_value_block;
        } else {
            continue_or_exit_block = BasicBlock::Create(
                context, "eval_next_when_expr", function, default_value_block);
        }

        // Get the child value of the when statement. If NULL simply continue to next when
        // statement
        builder.SetInsertPoint(current_when_expr_block);
        CodegenAnyVal when_val = CodegenAnyVal::create_call_wrapped(
            codegen, &builder, children()[i]->type(), child_fns[i], args, "when_val");
        builder.CreateCondBr(
            when_val.get_is_null(), continue_or_exit_block, check_when_expr_block);

        builder.SetInsertPoint(check_when_expr_block);
        if (has_case_expr()) {
            // Compare for equality
            Value* is_equal = case_val.eq(&when_val);
            builder.CreateCondBr(is_equal, return_then_expr_block, continue_or_exit_block);
        } else {
            builder.CreateCondBr(
                when_val.get_val(), return_then_expr_block, continue_or_exit_block);
        }

        builder.SetInsertPoint(return_then_expr_block);

        // Eval and return then value
        Value* then_val = CodegenAnyVal::create_call(
            codegen, &builder, child_fns[i+1], args, "then_val");
        builder.CreateRet(then_val);

        current_when_expr_block = continue_or_exit_block;
    }

    builder.SetInsertPoint(default_value_block);
    if (has_else_expr()) {
        Value* else_val = CodegenAnyVal::create_call(
            codegen, &builder, child_fns[num_children - 1], args, "else_val");
        builder.CreateRet(else_val);
    } else {
        builder.CreateRet(CodegenAnyVal::get_null_val(codegen, type()));
    }

    *fn = codegen->finalize_function(function);
    DCHECK(*fn != NULL);
    _ir_compute_fn = *fn;
    return Status::OK();
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
        *reinterpret_cast<StringVal*>(dst) = _children[child_idx]->get_string_val(ctx, row);
        break;
    case TYPE_DECIMAL:
        *reinterpret_cast<DecimalVal*>(dst) = _children[child_idx]->get_decimal_val(ctx, row);
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
        return AnyValUtil::equals(type, *reinterpret_cast<const StringVal*>(v1),
                                  *reinterpret_cast<const StringVal*>(v2));
    case TYPE_DECIMAL:
        return AnyValUtil::equals(type, *reinterpret_cast<const DecimalVal*>(v1),
                                  *reinterpret_cast<const DecimalVal*>(v2));
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

#define CASE_COMPUTE_FN(THEN_TYPE, TYPE_NAME) \
    THEN_TYPE CaseExpr::get_##TYPE_NAME(ExprContext* ctx, TupleRow* row) { \
        FunctionContext* fn_ctx = ctx->fn_context(_fn_context_index); \
        CaseExprState* state = reinterpret_cast<CaseExprState*>( \
                fn_ctx->get_function_state(FunctionContext::THREAD_LOCAL)); \
        DCHECK(state->case_val != NULL); \
        DCHECK(state->when_val != NULL); \
        int num_children = _children.size(); \
        if (has_case_expr()) { \
            /* All case and when exprs return the same type */ \
            /* (we guaranteed that during analysis). */ \
            get_child_val(0, ctx, row, state->case_val); \
        } else { \
            /* If there's no case expression, compare the when values to "true". */ \
            *reinterpret_cast<BooleanVal*>(state->case_val) = BooleanVal(true); \
        } \
        if (state->case_val->is_null) { \
            if (has_else_expr()) { \
                /* Return else value. */ \
                return _children[num_children - 1]->get_##TYPE_NAME(ctx, row); \
            } else { \
                return THEN_TYPE::null(); \
            } \
        } \
        int loop_start = has_case_expr() ? 1 : 0; \
        int loop_end = (has_else_expr()) ? num_children - 1 : num_children; \
        for (int i = loop_start; i < loop_end; i += 2) { \
            get_child_val(i, ctx, row, state->when_val); \
            if (state->when_val->is_null) continue; \
            if (any_val_eq(_children[0]->type(), state->case_val, state->when_val)) {  \
                /* Return then value. */ \
                return _children[i + 1]->get_##TYPE_NAME(ctx, row); \
            } \
        } \
        if (has_else_expr()) { \
            /* Return else value. */ \
            return _children[num_children - 1]->get_##TYPE_NAME(ctx, row); \
        } \
        return THEN_TYPE::null(); \
    }

#define CASE_COMPUTE_FN_WAPPER(TYPE, TYPE_NAME) \
    CASE_COMPUTE_FN(TYPE, TYPE_NAME)

CASE_COMPUTE_FN_WAPPER(BooleanVal, boolean_val)
CASE_COMPUTE_FN_WAPPER(TinyIntVal, tiny_int_val)
CASE_COMPUTE_FN_WAPPER(SmallIntVal, small_int_val)
CASE_COMPUTE_FN_WAPPER(IntVal, int_val)
CASE_COMPUTE_FN_WAPPER(BigIntVal, big_int_val)
CASE_COMPUTE_FN_WAPPER(FloatVal, float_val)
CASE_COMPUTE_FN_WAPPER(DoubleVal, double_val)
CASE_COMPUTE_FN_WAPPER(StringVal, string_val)
CASE_COMPUTE_FN_WAPPER(DateTimeVal, datetime_val)
CASE_COMPUTE_FN_WAPPER(DecimalVal, decimal_val)
CASE_COMPUTE_FN_WAPPER(DecimalV2Val, decimalv2_val)


}
