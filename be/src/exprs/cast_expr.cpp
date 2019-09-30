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

#include "exprs/cast_expr.h"

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "runtime/runtime_state.h"

using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantInt;
using llvm::Function;
using llvm::Instruction;
using llvm::LLVMContext;
using llvm::PHINode;
using llvm::Value;

namespace doris {

Expr* CastExpr::from_thrift(const TExprNode& node) {
    switch (node.child_type) {
    case TPrimitiveType::BOOLEAN:
        return new CastBooleanExpr(node);
    case TPrimitiveType::TINYINT:
        return new CastTinyIntExpr(node);
    case TPrimitiveType::SMALLINT:
        return new CastSmallIntExpr(node);
    case TPrimitiveType::INT:
        return new CastIntExpr(node);
    case TPrimitiveType::BIGINT:
        return new CastBigIntExpr(node);
    case TPrimitiveType::LARGEINT:
        return new CastLargeIntExpr(node);
    case TPrimitiveType::FLOAT:
        return new CastFloatExpr(node);
    case TPrimitiveType::DOUBLE:
        return new CastDoubleExpr(node);
    default:
        return NULL;
    }
    return NULL;
}

#define CAST_SAME(CLASS, TYPE, FN) \
    TYPE CLASS::FN(ExprContext* context, TupleRow* row) { \
        return _children[0]->FN(context, row); \
    }

#define CAST_FUNCTION(CLASS, TO_TYPE, TO_FN, FROM_TYPE, FROM_FN) \
    TO_TYPE CLASS::TO_FN(ExprContext* context, TupleRow* row) { \
        FROM_TYPE v = _children[0]->FROM_FN(context, row); \
        if (v.is_null) { \
            return TO_TYPE::null(); \
        } \
        return TO_TYPE(v.val); \
    } 

#define CAST_FROM_BOOLEAN(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastBooleanExpr, TO_TYPE, TO_FN, BooleanVal, get_boolean_val)

CAST_SAME(CastBooleanExpr, BooleanVal, get_boolean_val)
CAST_FROM_BOOLEAN(TinyIntVal, get_tiny_int_val)
CAST_FROM_BOOLEAN(SmallIntVal, get_small_int_val)
CAST_FROM_BOOLEAN(IntVal, get_int_val)
CAST_FROM_BOOLEAN(BigIntVal, get_big_int_val)
CAST_FROM_BOOLEAN(LargeIntVal, get_large_int_val)
CAST_FROM_BOOLEAN(FloatVal, get_float_val)
CAST_FROM_BOOLEAN(DoubleVal, get_double_val)

#define CAST_FROM_TINYINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastTinyIntExpr, TO_TYPE, TO_FN, TinyIntVal, get_tiny_int_val)

CAST_SAME(CastTinyIntExpr, TinyIntVal, get_tiny_int_val)
CAST_FROM_TINYINT(BooleanVal, get_boolean_val)
CAST_FROM_TINYINT(SmallIntVal, get_small_int_val)
CAST_FROM_TINYINT(IntVal, get_int_val)
CAST_FROM_TINYINT(BigIntVal, get_big_int_val)
CAST_FROM_TINYINT(LargeIntVal, get_large_int_val)
CAST_FROM_TINYINT(FloatVal, get_float_val)
CAST_FROM_TINYINT(DoubleVal, get_double_val)

#define CAST_FROM_SMALLINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastSmallIntExpr, TO_TYPE, TO_FN, SmallIntVal, get_small_int_val)

CAST_SAME(CastSmallIntExpr, SmallIntVal, get_small_int_val)
CAST_FROM_SMALLINT(BooleanVal, get_boolean_val)
CAST_FROM_SMALLINT(TinyIntVal, get_tiny_int_val)
CAST_FROM_SMALLINT(IntVal, get_int_val)
CAST_FROM_SMALLINT(BigIntVal, get_big_int_val)
CAST_FROM_SMALLINT(LargeIntVal, get_large_int_val)
CAST_FROM_SMALLINT(FloatVal, get_float_val)
CAST_FROM_SMALLINT(DoubleVal, get_double_val)

#define CAST_FROM_INT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastIntExpr, TO_TYPE, TO_FN, IntVal, get_int_val)

CAST_SAME(CastIntExpr, IntVal, get_int_val)
CAST_FROM_INT(BooleanVal, get_boolean_val)
CAST_FROM_INT(TinyIntVal, get_tiny_int_val)
CAST_FROM_INT(SmallIntVal, get_small_int_val)
CAST_FROM_INT(BigIntVal, get_big_int_val)
CAST_FROM_INT(LargeIntVal, get_large_int_val)
CAST_FROM_INT(FloatVal, get_float_val)
CAST_FROM_INT(DoubleVal, get_double_val)

#define CAST_FROM_BIGINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastBigIntExpr, TO_TYPE, TO_FN, BigIntVal, get_big_int_val)

CAST_SAME(CastBigIntExpr, BigIntVal, get_big_int_val)
CAST_FROM_BIGINT(BooleanVal, get_boolean_val)
CAST_FROM_BIGINT(TinyIntVal, get_tiny_int_val)
CAST_FROM_BIGINT(SmallIntVal, get_small_int_val)
CAST_FROM_BIGINT(IntVal, get_int_val)
CAST_FROM_BIGINT(LargeIntVal, get_large_int_val)
CAST_FROM_BIGINT(FloatVal, get_float_val)
CAST_FROM_BIGINT(DoubleVal, get_double_val)

#define CAST_FROM_LARGEINT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastLargeIntExpr, TO_TYPE, TO_FN, LargeIntVal, get_large_int_val)

CAST_SAME(CastLargeIntExpr, LargeIntVal, get_large_int_val)
CAST_FROM_LARGEINT(BooleanVal, get_boolean_val)
CAST_FROM_LARGEINT(TinyIntVal, get_tiny_int_val)
CAST_FROM_LARGEINT(SmallIntVal, get_small_int_val)
CAST_FROM_LARGEINT(IntVal, get_int_val)
CAST_FROM_LARGEINT(BigIntVal, get_big_int_val)
CAST_FROM_LARGEINT(FloatVal, get_float_val)
CAST_FROM_LARGEINT(DoubleVal, get_double_val)

#define CAST_FROM_FLOAT(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastFloatExpr, TO_TYPE, TO_FN, FloatVal, get_float_val)

CAST_SAME(CastFloatExpr, FloatVal, get_float_val)
CAST_FROM_FLOAT(BooleanVal, get_boolean_val)
CAST_FROM_FLOAT(TinyIntVal, get_tiny_int_val)
CAST_FROM_FLOAT(SmallIntVal, get_small_int_val)
CAST_FROM_FLOAT(IntVal, get_int_val)
CAST_FROM_FLOAT(BigIntVal, get_big_int_val)
CAST_FROM_FLOAT(LargeIntVal, get_large_int_val)
CAST_FROM_FLOAT(DoubleVal, get_double_val)

#define CAST_FROM_DOUBLE(TO_TYPE, TO_FN) \
    CAST_FUNCTION(CastDoubleExpr, TO_TYPE, TO_FN, DoubleVal, get_double_val)

CAST_SAME(CastDoubleExpr, DoubleVal, get_double_val)
CAST_FROM_DOUBLE(BooleanVal, get_boolean_val)
CAST_FROM_DOUBLE(TinyIntVal, get_tiny_int_val)
CAST_FROM_DOUBLE(SmallIntVal, get_small_int_val)
CAST_FROM_DOUBLE(IntVal, get_int_val)
CAST_FROM_DOUBLE(BigIntVal, get_big_int_val)
CAST_FROM_DOUBLE(LargeIntVal, get_large_int_val)
CAST_FROM_DOUBLE(FloatVal, get_float_val)

// IR codegen for cast expression
//
// define i16 @cast(%"class.doris::ExprContext"* %context,
//                 %"class.doris::TupleRow"* %row) #20 {
// entry:
//   %child_val = call { i8, i64 } @get_slot_ref(%"class.doris::ExprContext"* %context,
//                                             %"class.doris::TupleRow"* %row)
//   %0 = extractvalue { i8, i64 } %child_val, 0
//   %child_is_null = trunc i8 %0 to i1
//   br i1 %child_is_null, label %ret, label %child_not_null
// 
// child_not_null:                                     ; preds = %entry
//   %val = add i64 %2, %3
//   br label %ret
//
// ret:                                              ; preds = %not_null_block, %null_block
//   %ret3 = phi i1 [ false, %null_block ], [ %4, %not_null_block ]
//   %5 = zext i1 %ret3 to i16
//   %6 = shl i16 %5, 8
//   %7 = or i16 0, %6
//   ret i16 %7
// }
Status CastExpr::codegen_cast_fn(RuntimeState* state, llvm::Function** fn) {
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Function* child_fn = NULL;
    RETURN_IF_ERROR(_children[0]->get_codegend_compute_fn(state, &child_fn));

    // Function protocol
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "cast", &args);
    LLVMContext& cg_ctx = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(cg_ctx);

    // Constant
    Value* zero = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 0);
    Value* one = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 1);

    // Block
    BasicBlock* entry_block = BasicBlock::Create(cg_ctx, "entry", *fn);
    BasicBlock* child_not_null_block = BasicBlock::Create(cg_ctx, "child_not_null", *fn);
    BasicBlock* ret_block = BasicBlock::Create(cg_ctx, "ret", *fn);

    // entry block
    builder.SetInsertPoint(entry_block);
    CodegenAnyVal child_val = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, _children[0]->type(), child_fn, args, "child_val");
    // if (v1.is_null) return null;
    Value* child_is_null = child_val.get_is_null();
    builder.CreateCondBr(child_is_null, ret_block, child_not_null_block);
    
    // child_not_null_block
    builder.SetInsertPoint(child_not_null_block);
    Value* val = NULL;
    Instruction::CastOps cast_op = codegen->get_cast_op(_children[0]->type(), _type);
    if (cast_op == Instruction::CastOps::CastOpsEnd) {
        return Status::InternalError("Unknow type");
    }
    val = builder.CreateCast(cast_op, child_val.get_val(), codegen->get_type(_type), "val");
    builder.CreateBr(ret_block);

    // ret_block
    builder.SetInsertPoint(ret_block);
    PHINode* is_null_phi = builder.CreatePHI(codegen->tinyint_type(), 2, "is_null_phi");
    is_null_phi->addIncoming(one, entry_block);
    is_null_phi->addIncoming(zero, child_not_null_block);
    PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
    Value* null = Constant::getNullValue(val->getType());
    val_phi->addIncoming(null, entry_block);
    val_phi->addIncoming(val, child_not_null_block);

    CodegenAnyVal result = CodegenAnyVal::get_non_null_val(
        codegen, &builder, type(), "result");
    result.set_is_null(is_null_phi);
    result.set_val(val_phi);
    builder.CreateRet(result.value());

    *fn = codegen->finalize_function(*fn);
    return Status::OK();
}

#define CODEGEN_DEFINE(CLASS) \
    Status CLASS::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) { \
        if (_ir_compute_fn != NULL) { \
            *fn = _ir_compute_fn; \
            return Status::OK(); \
        } \
        RETURN_IF_ERROR(codegen_cast_fn(state, fn)); \
        _ir_compute_fn = *fn; \
        return Status::OK(); \
    }

CODEGEN_DEFINE(CastBooleanExpr);
CODEGEN_DEFINE(CastTinyIntExpr);
CODEGEN_DEFINE(CastSmallIntExpr);
CODEGEN_DEFINE(CastIntExpr);
CODEGEN_DEFINE(CastBigIntExpr);
CODEGEN_DEFINE(CastLargeIntExpr);
CODEGEN_DEFINE(CastFloatExpr);
CODEGEN_DEFINE(CastDoubleExpr);
}
