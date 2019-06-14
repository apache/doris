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

#include "exprs/arithmetic_expr.h"

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "runtime/runtime_state.h"

using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantFP;
using llvm::ConstantInt;
using llvm::Function;
using llvm::LLVMContext;
using llvm::PHINode;
using llvm::Value;

namespace doris {

Expr* ArithmeticExpr::from_thrift(const TExprNode& node) {
    switch (node.opcode) {
    case TExprOpcode::ADD:
        return new AddExpr(node);
    case TExprOpcode::SUBTRACT:
        return new SubExpr(node);
    case TExprOpcode::MULTIPLY:
        return new MulExpr(node);
    case TExprOpcode::DIVIDE:
    case TExprOpcode::INT_DIVIDE:
        return new DivExpr(node);
    case TExprOpcode::MOD:
        return new ModExpr(node);
    case TExprOpcode::BITAND:
        return new BitAndExpr(node);
    case TExprOpcode::BITOR:
        return new BitOrExpr(node);
    case TExprOpcode::BITXOR:
        return new BitXorExpr(node);
    case TExprOpcode::BITNOT:
        return new BitNotExpr(node);
    default:
        return NULL;
    }
    return NULL;
}

#define BINARY_OP_CHECK_ZERO_FN(TYPE, CLASS, FN, OP) \
    TYPE CLASS::FN(ExprContext* context, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(context, row); \
        if (v1.is_null) { \
            return TYPE::null(); \
        } \
        TYPE v2 = _children[1]->FN(context, row); \
        if (v2.is_null || v2.val == 0) { \
            return TYPE::null(); \
        } \
        return TYPE(v1.val OP v2.val); \
    }

#define BINARY_OP_FN(TYPE, CLASS, FN, OP) \
    TYPE CLASS::FN(ExprContext* context, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(context, row); \
        if (v1.is_null) { \
            return TYPE::null(); \
        } \
        TYPE v2 = _children[1]->FN(context, row); \
        if (v2.is_null) { \
            return TYPE::null(); \
        } \
        return TYPE(v1.val OP v2.val); \
    }

#define BINARY_ARITH_FNS(CLASS, OP) \
    BINARY_OP_FN(TinyIntVal, CLASS, get_tiny_int_val, OP) \
    BINARY_OP_FN(SmallIntVal, CLASS, get_small_int_val, OP) \
    BINARY_OP_FN(IntVal, CLASS, get_int_val, OP) \
    BINARY_OP_FN(BigIntVal, CLASS, get_big_int_val, OP) \
    BINARY_OP_FN(LargeIntVal, CLASS, get_large_int_val, OP) \
    BINARY_OP_FN(FloatVal, CLASS, get_float_val, OP) \
    BINARY_OP_FN(DoubleVal, CLASS, get_double_val, OP) \

BINARY_ARITH_FNS(AddExpr, +)
BINARY_ARITH_FNS(SubExpr, -)
BINARY_ARITH_FNS(MulExpr, *)

#define BINARY_DIV_FNS() \
    BINARY_OP_CHECK_ZERO_FN(TinyIntVal, DivExpr, get_tiny_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(SmallIntVal, DivExpr, get_small_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(IntVal, DivExpr, get_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(BigIntVal, DivExpr, get_big_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(LargeIntVal, DivExpr, get_large_int_val, /) \
    BINARY_OP_CHECK_ZERO_FN(FloatVal, DivExpr, get_float_val, /) \
    BINARY_OP_CHECK_ZERO_FN(DoubleVal, DivExpr, get_double_val, /) \

BINARY_DIV_FNS()

#define BINARY_MOD_FNS() \
    BINARY_OP_CHECK_ZERO_FN(TinyIntVal, ModExpr, get_tiny_int_val, %) \
    BINARY_OP_CHECK_ZERO_FN(SmallIntVal, ModExpr, get_small_int_val, %) \
    BINARY_OP_CHECK_ZERO_FN(IntVal, ModExpr, get_int_val, %) \
    BINARY_OP_CHECK_ZERO_FN(BigIntVal, ModExpr, get_big_int_val, %) \
    BINARY_OP_CHECK_ZERO_FN(LargeIntVal, ModExpr, get_large_int_val, %) \

BINARY_MOD_FNS()

FloatVal ModExpr::get_float_val(ExprContext* context, TupleRow* row) { 
    FloatVal v1 = _children[0]->get_float_val(context, row); 
    if (v1.is_null) {
        return FloatVal::null();
    }
    FloatVal v2 = _children[1]->get_float_val(context, row); 
    if (v2.is_null) {
        return FloatVal::null();
    }
    return FloatVal(fmod(v1.val, v2.val));
}

DoubleVal ModExpr::get_double_val(ExprContext* context, TupleRow* row) { 
    DoubleVal v1 = _children[0]->get_double_val(context, row); 
    if (v1.is_null) {
        return DoubleVal::null();
    }
    DoubleVal v2 = _children[1]->get_double_val(context, row); 
    if (v2.is_null) {
        return DoubleVal::null();
    }
    return DoubleVal(fmod(v1.val, v2.val));
}

#define BINARY_BIT_FNS(CLASS, OP) \
    BINARY_OP_FN(TinyIntVal, CLASS, get_tiny_int_val, OP) \
    BINARY_OP_FN(SmallIntVal, CLASS, get_small_int_val, OP) \
    BINARY_OP_FN(IntVal, CLASS, get_int_val, OP) \
    BINARY_OP_FN(BigIntVal, CLASS, get_big_int_val, OP) \
    BINARY_OP_FN(LargeIntVal, CLASS, get_large_int_val, OP) \

BINARY_BIT_FNS(BitAndExpr, &)
BINARY_BIT_FNS(BitOrExpr, |)
BINARY_BIT_FNS(BitXorExpr, ^)

#define BITNOT_OP_FN(TYPE, FN) \
    TYPE BitNotExpr::FN(ExprContext* context, TupleRow* row) { \
        TYPE v = _children[0]->FN(context, row); \
        if (v.is_null) { \
            return TYPE::null(); \
        } \
        return TYPE(~v.val); \
    }

#define BITNOT_FNS() \
    BITNOT_OP_FN(TinyIntVal, get_tiny_int_val) \
    BITNOT_OP_FN(SmallIntVal, get_small_int_val) \
    BITNOT_OP_FN(IntVal, get_int_val) \
    BITNOT_OP_FN(BigIntVal, get_big_int_val) \
    BITNOT_OP_FN(LargeIntVal, get_large_int_val) \

BITNOT_FNS()

// IR codegen for compound add predicates.  Compound predicate has non trivial 
// null handling as well as many branches so this is pretty complicated.  The IR 
// for x && y is:
//
// define i16 @Add(%"class.doris::ExprContext"* %context,
//                 %"class.doris::TupleRow"* %row) #20 {
// entry:
//   %lhs_val = call { i8, i64 } @get_slot_ref(%"class.doris::ExprContext"* %context,
//                                             %"class.doris::TupleRow"* %row)
//   %0 = extractvalue { i8, i64 } %lhs_val, 0
//   %lhs_is_null = trunc i8 %0 to i1
//   br i1 %lhs_is_null, label %null, label %lhs_not_null
// 
// lhs_not_null:                                     ; preds = %entry
//   %rhs_val = call { i8, i64 } @get_slot_ref(%"class.doris::ExprContext"* %context,
//                                             %"class.doris::TupleRow"* %row)
//   %1 = extractvalue { i8, i64 } %lhs_val, 0
//   %rhs_is_null = trunc i8 %1 to i1
//   br i1 %rhs_is_null, label %null, label %rhs_not_null
// 
// rhs_not_null:                                     ; preds = %entry
//   %2 = extractvalue { i8, i64 } %lhs_val, 1
//   %3 = extractvalue { i8, i64 } %rhs_val, 1
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
Status ArithmeticExpr::codegen_binary_op(
        RuntimeState* state, llvm::Function** fn, BinaryOpType op_type) {
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Function* lhs_fn = NULL;
    RETURN_IF_ERROR(_children[0]->get_codegend_compute_fn(state, &lhs_fn));
    Function* rhs_fn = NULL;
    RETURN_IF_ERROR(_children[1]->get_codegend_compute_fn(state, &rhs_fn));

    // Function protocol
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "add", &args);
    LLVMContext& cg_ctx = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(cg_ctx);

    // Constant
    Value* zero = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 0);
    Value* one = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 1);

    // Block
    BasicBlock* entry_block = BasicBlock::Create(cg_ctx, "entry", *fn);
    BasicBlock* lhs_not_null_block = BasicBlock::Create(cg_ctx, "lhs_not_null", *fn);
    BasicBlock* rhs_not_null_block = NULL;
    if ((op_type == BinaryOpType::DIV || op_type == BinaryOpType::MOD)) {
            // && _type.type != TYPE_DOUBLE && _type.type != TYPE_FLOAT) {
        rhs_not_null_block = BasicBlock::Create(cg_ctx, "rhs_not_null", *fn);
    }
    BasicBlock* compute_block = BasicBlock::Create(cg_ctx, "compute", *fn);
    BasicBlock* null_block = BasicBlock::Create(cg_ctx, "null", *fn);
    BasicBlock* ret_block = BasicBlock::Create(cg_ctx, "ret", *fn);

    // entry block
    builder.SetInsertPoint(entry_block);
    CodegenAnyVal lhs_val = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, _children[0]->type(), lhs_fn, args, "lhs_val");
    // if (v1.is_null) return null;
    Value* lhs_is_null = lhs_val.get_is_null();
    builder.CreateCondBr(lhs_is_null, null_block, lhs_not_null_block);
    
    // lhs_not_null_block
    builder.SetInsertPoint(lhs_not_null_block);
    CodegenAnyVal rhs_val = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, _children[1]->type(), rhs_fn, args, "lhs_val");
    Value* rhs_is_null = rhs_val.get_is_null();
    // if (v2.is_null) return null;
    Value* rhs_llvm_val = NULL;
    if ((op_type == BinaryOpType::DIV || op_type == BinaryOpType::MOD)) {
            // && _type.type != TYPE_DOUBLE && _type.type != TYPE_FLOAT) {
        builder.CreateCondBr(rhs_is_null, null_block, rhs_not_null_block);
        // if (v2.val == 0)
        builder.SetInsertPoint(rhs_not_null_block);
        rhs_llvm_val = rhs_val.get_val();
        Value* rhs_zero = NULL;
        Value* rhs_is_zero = NULL;
        if (_type.type == TYPE_DOUBLE || _type.type == TYPE_FLOAT) {
            rhs_zero = ConstantFP::get(rhs_llvm_val->getType(), 0.0);
            rhs_is_zero = builder.CreateFCmpOEQ(rhs_llvm_val, rhs_zero, "rhs_is_zero");
        } else {
            rhs_zero = ConstantInt::get(rhs_llvm_val->getType(), 0);
            rhs_is_zero = builder.CreateICmpEQ(rhs_llvm_val, rhs_zero, "rhs_is_zero");
        }
        builder.CreateCondBr(rhs_is_zero, null_block, compute_block);
    } else {
        builder.CreateCondBr(rhs_is_null, null_block, compute_block);
    }

    // compute_block
    builder.SetInsertPoint(compute_block);
    Value* val = NULL;
    switch (op_type) {
    case ADD: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateAdd(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            val = builder.CreateFAdd(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case SUB: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateSub(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            val = builder.CreateFSub(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case MUL: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateMul(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            val = builder.CreateFMul(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case DIV: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateSDiv(lhs_val.get_val(), rhs_llvm_val, "val");
            break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            val = builder.CreateFDiv(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case MOD: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateSRem(lhs_val.get_val(), rhs_llvm_val, "val");
            break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            val = builder.CreateFRem(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case BIT_AND: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateAnd(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case BIT_OR: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateOr(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    case BIT_XOR: {
        switch (_type.type) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            val = builder.CreateXor(lhs_val.get_val(), rhs_val.get_val(), "val");
            break;
        default:
            return Status::InternalError("Unk");
        }
        break;
    }
    default:
        return Status::InternalError("Unknow operato");
    }
    builder.CreateBr(ret_block);

    // null block
    builder.SetInsertPoint(null_block);
    builder.CreateBr(ret_block);

    // ret block
    builder.SetInsertPoint(ret_block);
    PHINode* is_null_phi = builder.CreatePHI(codegen->tinyint_type(), 2, "is_null_phi");
    is_null_phi->addIncoming(one, null_block);
    is_null_phi->addIncoming(zero, compute_block);
    PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
    Value* null = Constant::getNullValue(val->getType());
    val_phi->addIncoming(null, null_block);
    val_phi->addIncoming(val, compute_block);

    CodegenAnyVal result = CodegenAnyVal::get_non_null_val(
        codegen, &builder, type(), "result");
    result.set_is_null(is_null_phi);
    result.set_val(val_phi);
    builder.CreateRet(result.value());

    *fn = codegen->finalize_function(*fn);
    return Status::OK();
}

Status AddExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::ADD));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status SubExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::SUB));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status MulExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::MUL));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status DivExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::DIV));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status ModExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::MOD));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status BitAndExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::BIT_AND));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status BitOrExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::BIT_OR));
    _ir_compute_fn = *fn;
    return Status::OK();
}

Status BitXorExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    RETURN_IF_ERROR(codegen_binary_op(state, fn , BinaryOpType::BIT_XOR));
    _ir_compute_fn = *fn;
    return Status::OK();
}

// IR codegen for compound add predicates.  Compound predicate has non trivial 
// null handling as well as many branches so this is pretty complicated.  The IR 
// for x && y is:
//
// define i16 @Add(%"class.doris::ExprContext"* %context,
//                 %"class.doris::TupleRow"* %row) #20 {
// entry:
//   %lhs_val = call { i8, i64 } @get_slot_ref(%"class.doris::ExprContext"* %context,
//                                             %"class.doris::TupleRow"* %row)
//   %0 = extractvalue { i8, i64 } %lhs_val, 0
//   %lhs_is_null = trunc i8 %0 to i1
//   br i1 %lhs_is_null, label %null, label %lhs_not_null
// 
// ret:                                              ; preds = %not_null_block, %null_block
//   %ret3 = phi i1 [ false, %null_block ], [ %4, %not_null_block ]
//   %5 = zext i1 %ret3 to i16
//   %6 = shl i16 %5, 8
//   %7 = or i16 0, %6
//   ret i16 %7
// }
Status BitNotExpr::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Function* child_fn = NULL;
    RETURN_IF_ERROR(_children[0]->get_codegend_compute_fn(state, &child_fn));

    // Function protocol
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "add", &args);
    LLVMContext& cg_ctx = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(cg_ctx);

    // Constant
    Value* zero = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 0);
    Value* one = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 1);

    // Block
    BasicBlock* entry_block = BasicBlock::Create(cg_ctx, "entry", *fn);
    BasicBlock* compute_block = BasicBlock::Create(cg_ctx, "compute", *fn);
    BasicBlock* ret_block = BasicBlock::Create(cg_ctx, "ret", *fn);

    // entry block
    builder.SetInsertPoint(entry_block);
    CodegenAnyVal child_val = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, _children[0]->type(), child_fn, args, "child_val");
    // if (v1.is_null) return null;
    Value* child_is_null = child_val.get_is_null();
    builder.CreateCondBr(child_is_null, ret_block, compute_block);
    
    // compute_block
    builder.SetInsertPoint(compute_block);
    Value* val = builder.CreateNot(child_val.get_val(), "val");
    builder.CreateBr(ret_block);

    // ret block
    builder.SetInsertPoint(ret_block);
    PHINode* is_null_phi = builder.CreatePHI(codegen->tinyint_type(), 2, "is_null_phi");
    is_null_phi->addIncoming(one, entry_block);
    is_null_phi->addIncoming(zero, compute_block);
    PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
    Value* null = Constant::getNullValue(val->getType());
    val_phi->addIncoming(null, entry_block);
    val_phi->addIncoming(val, compute_block);

    CodegenAnyVal result = CodegenAnyVal::get_non_null_val(codegen, &builder, type(), "result");
    result.set_is_null(is_null_phi);
    result.set_val(val_phi);
    builder.CreateRet(result.value());

    *fn = codegen->finalize_function(*fn);
    _ir_compute_fn = *fn;
    return Status::OK();
}

}

