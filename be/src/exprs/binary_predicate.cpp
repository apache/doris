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

#include "exprs/binary_predicate.h"

#include <sstream>

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "util/debug_util.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/datetime_value.h"
#include "runtime/decimal_value.h"
#include "runtime/decimalv2_value.h"

using llvm::BasicBlock;
using llvm::CmpInst;
using llvm::Constant;
using llvm::ConstantInt;
using llvm::Function;
using llvm::LLVMContext;
using llvm::PHINode;
using llvm::Value;

namespace doris {

Expr* BinaryPredicate::from_thrift(const TExprNode& node) {
    switch (node.opcode) {
    case TExprOpcode::EQ: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new EqBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new EqTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new EqSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new EqIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new EqBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new EqLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new EqFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new EqDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
            return new EqStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new EqDateTimeValPred(node);
        case TPrimitiveType::DECIMAL:
            return new EqDecimalValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new EqDecimalV2ValPred(node);
        default:
            return NULL;
        }
    }
    case TExprOpcode::NE: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new NeBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new NeTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new NeSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new NeIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new NeBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new NeLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new NeFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new NeDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
            return new NeStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new NeDateTimeValPred(node);
        case TPrimitiveType::DECIMAL:
            return new NeDecimalValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new NeDecimalV2ValPred(node);
        default:
            return NULL;
        }
    }
    case TExprOpcode::LT: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new LtBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new LtTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new LtSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new LtIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new LtBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new LtLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new LtFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new LtDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
            return new LtStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new LtDateTimeValPred(node);
        case TPrimitiveType::DECIMAL:
            return new LtDecimalValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new LtDecimalV2ValPred(node);
        default:
            return NULL;
        }
    }
    case TExprOpcode::LE: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new LeBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new LeTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new LeSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new LeIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new LeBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new LeLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new LeFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new LeDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
            return new LeStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new LeDateTimeValPred(node);
        case TPrimitiveType::DECIMAL:
            return new LeDecimalValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new LeDecimalV2ValPred(node);
        default:
            return NULL;
        }
    }
    case TExprOpcode::GT: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new GtBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new GtTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new GtSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new GtIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new GtBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new GtLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new GtFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new GtDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
            return new GtStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new GtDateTimeValPred(node);
        case TPrimitiveType::DECIMAL:
            return new GtDecimalValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new GtDecimalV2ValPred(node);
        default:
            return NULL;
        }
    }
    case TExprOpcode::GE: {
        switch (node.child_type) {
        case TPrimitiveType::BOOLEAN:
            return new GeBooleanValPred(node);
        case TPrimitiveType::TINYINT:
            return new GeTinyIntValPred(node);
        case TPrimitiveType::SMALLINT:
            return new GeSmallIntValPred(node);
        case TPrimitiveType::INT:
            return new GeIntValPred(node);
        case TPrimitiveType::BIGINT:
            return new GeBigIntValPred(node);
        case TPrimitiveType::LARGEINT:
            return new GeLargeIntValPred(node);
        case TPrimitiveType::FLOAT:
            return new GeFloatValPred(node);
        case TPrimitiveType::DOUBLE:
            return new GeDoubleValPred(node);
        case TPrimitiveType::CHAR:
        case TPrimitiveType::VARCHAR:
            return new GeStringValPred(node);
        case TPrimitiveType::DATE:
        case TPrimitiveType::DATETIME:
            return new GeDateTimeValPred(node);
        case TPrimitiveType::DECIMAL:
            return new GeDecimalValPred(node);
        case TPrimitiveType::DECIMALV2:
            return new GeDecimalV2ValPred(node);
        default:
            return NULL;
        }
    }
    default:
        return NULL;
    }
    return NULL;
}

std::string BinaryPredicate::debug_string() const {
    std::stringstream out;
    out << "BinaryPredicate(" << Expr::debug_string() << ")";
    return out.str();
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
Status BinaryPredicate::codegen_compare_fn(
        RuntimeState* state, llvm::Function** fn, CmpInst::Predicate pred) {
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Function* lhs_fn = NULL;
    RETURN_IF_ERROR(_children[0]->get_codegend_compute_fn(state, &lhs_fn));
    Function* rhs_fn = NULL;
    RETURN_IF_ERROR(_children[1]->get_codegend_compute_fn(state, &rhs_fn));

    // Function protocol
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "compare", &args);
    LLVMContext& cg_ctx = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(cg_ctx);

    // Constant
    Value* zero = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 0);
    Value* one = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 1);

    // Block
    BasicBlock* entry_block = BasicBlock::Create(cg_ctx, "entry", *fn);
    BasicBlock* lhs_not_null_block = BasicBlock::Create(cg_ctx, "lhs_not_null", *fn);
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
        codegen, &builder, _children[1]->type(), rhs_fn, args, "rhs_val");
    Value* rhs_is_null = rhs_val.get_is_null();
    builder.CreateCondBr(rhs_is_null, null_block, compute_block);

    // compute_block
    builder.SetInsertPoint(compute_block);
    Value* val = NULL;
    if (_children[0]->type().type == TYPE_DOUBLE || _children[0]->type().type == TYPE_FLOAT) {
        val = builder.CreateFCmp(pred, lhs_val.get_val(), rhs_val.get_val(), "val");
    } else {
        val = builder.CreateICmp(pred, lhs_val.get_val(), rhs_val.get_val(), "val");
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

#define BINARY_PRED_FN(CLASS, TYPE, FN, OP, LLVM_PRED) \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(ctx, row); \
        if (v1.is_null) { \
            return BooleanVal::null(); \
        } \
        TYPE v2 = _children[1]->FN(ctx, row); \
        if (v2.is_null) { \
            return BooleanVal::null(); \
        } \
        return BooleanVal(v1.val OP v2.val); \
    } \
    Status CLASS::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) { \
        if (_ir_compute_fn != NULL) { \
            *fn = _ir_compute_fn; \
            return Status::OK(); \
        } \
        RETURN_IF_ERROR(codegen_compare_fn(state, fn, LLVM_PRED)); \
        _ir_compute_fn = *fn; \
        return Status::OK(); \
    } \

// add '/**/' to pass codestyle check of cooder
#define BINARY_PRED_INT_FNS(TYPE, FN) \
    BINARY_PRED_FN(Eq##TYPE##Pred, TYPE, FN, /**/ == /**/, CmpInst::ICMP_EQ) \
    BINARY_PRED_FN(Ne##TYPE##Pred, TYPE, FN, /**/ != /**/, CmpInst::ICMP_NE) \
    BINARY_PRED_FN(Lt##TYPE##Pred, TYPE, FN, /**/ < /**/, CmpInst::ICMP_SLT) \
    BINARY_PRED_FN(Le##TYPE##Pred, TYPE, FN, /**/ <= /**/, CmpInst::ICMP_SLE) \
    BINARY_PRED_FN(Gt##TYPE##Pred, TYPE, FN, /**/ > /**/, CmpInst::ICMP_SGT) \
    BINARY_PRED_FN(Ge##TYPE##Pred, TYPE, FN, /**/ >= /**/, CmpInst::ICMP_SGE)

BINARY_PRED_INT_FNS(BooleanVal, get_boolean_val);
BINARY_PRED_INT_FNS(TinyIntVal, get_tiny_int_val);
BINARY_PRED_INT_FNS(SmallIntVal, get_small_int_val);
BINARY_PRED_INT_FNS(IntVal, get_int_val);
BINARY_PRED_INT_FNS(BigIntVal, get_big_int_val);
BINARY_PRED_INT_FNS(LargeIntVal, get_large_int_val);

#define BINARY_PRED_FLOAT_FNS(TYPE, FN) \
    BINARY_PRED_FN(Eq##TYPE##Pred, TYPE, FN, ==, CmpInst::FCMP_OEQ) \
    BINARY_PRED_FN(Ne##TYPE##Pred, TYPE, FN, !=, CmpInst::FCMP_UNE) \
    BINARY_PRED_FN(Lt##TYPE##Pred, TYPE, FN, <, CmpInst::FCMP_OLT) \
    BINARY_PRED_FN(Le##TYPE##Pred, TYPE, FN, <=, CmpInst::FCMP_OLE) \
    BINARY_PRED_FN(Gt##TYPE##Pred, TYPE, FN, >, CmpInst::FCMP_OGT) \
    BINARY_PRED_FN(Ge##TYPE##Pred, TYPE, FN, >=, CmpInst::FCMP_OGE)

BINARY_PRED_FLOAT_FNS(FloatVal, get_float_val);
BINARY_PRED_FLOAT_FNS(DoubleVal, get_double_val);

#define COMPLICATE_BINARY_PRED_FN(CLASS, TYPE, FN, DORIS_TYPE, FROM_FUNC, OP) \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        TYPE v1 = _children[0]->FN(ctx, row); \
        if (v1.is_null) { \
            return BooleanVal::null(); \
        } \
        TYPE v2 = _children[1]->FN(ctx, row); \
        if (v2.is_null) { \
            return BooleanVal::null(); \
        } \
        DORIS_TYPE pv1 = DORIS_TYPE::FROM_FUNC(v1); \
        DORIS_TYPE pv2 = DORIS_TYPE::FROM_FUNC(v2); \
        return BooleanVal(pv1 OP pv2); \
    } \
    Status CLASS::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) { \
        return get_codegend_compute_fn_wrapper(state, fn); \
    } \


#define COMPLICATE_BINARY_PRED_FNS(TYPE, FN, DORIS_TYPE, FROM_FUNC) \
    COMPLICATE_BINARY_PRED_FN(Eq##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, ==) \
    COMPLICATE_BINARY_PRED_FN(Ne##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, !=) \
    COMPLICATE_BINARY_PRED_FN(Lt##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, <) \
    COMPLICATE_BINARY_PRED_FN(Le##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, <=) \
    COMPLICATE_BINARY_PRED_FN(Gt##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, >) \
    COMPLICATE_BINARY_PRED_FN(Ge##TYPE##Pred, TYPE, FN, DORIS_TYPE, FROM_FUNC, >=)

COMPLICATE_BINARY_PRED_FNS(DecimalVal, get_decimal_val, DecimalValue, from_decimal_val)
COMPLICATE_BINARY_PRED_FNS(DecimalV2Val, get_decimalv2_val, DecimalV2Value, from_decimal_val)

#define DATETIME_BINARY_PRED_FN(CLASS, OP, LLVM_PRED) \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        DateTimeVal v1 = _children[0]->get_datetime_val(ctx, row); \
        if (v1.is_null) { \
            return BooleanVal::null(); \
        } \
        DateTimeVal v2 = _children[1]->get_datetime_val(ctx, row); \
        if (v2.is_null) { \
            return BooleanVal::null(); \
        } \
        return BooleanVal(v1.packed_time OP v2.packed_time); \
    } \
    Status CLASS::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) { \
        if (_ir_compute_fn != NULL) { \
            *fn = _ir_compute_fn; \
            return Status::OK(); \
        } \
        RETURN_IF_ERROR(codegen_compare_fn(state, fn , LLVM_PRED)); \
        _ir_compute_fn = *fn; \
        return Status::OK(); \
    } \

#define DATETIME_BINARY_PRED_FNS() \
    DATETIME_BINARY_PRED_FN(Eq##DateTimeVal##Pred, ==, CmpInst::ICMP_EQ) \
    DATETIME_BINARY_PRED_FN(Ne##DateTimeVal##Pred, !=, CmpInst::ICMP_NE) \
    DATETIME_BINARY_PRED_FN(Lt##DateTimeVal##Pred, <, CmpInst::ICMP_SLT) \
    DATETIME_BINARY_PRED_FN(Le##DateTimeVal##Pred, <=, CmpInst::ICMP_SLE) \
    DATETIME_BINARY_PRED_FN(Gt##DateTimeVal##Pred, >, CmpInst::ICMP_SGT) \
    DATETIME_BINARY_PRED_FN(Ge##DateTimeVal##Pred, >=, CmpInst::ICMP_SGE)

DATETIME_BINARY_PRED_FNS()

#define STRING_BINARY_PRED_FN(CLASS, OP) \
    BooleanVal CLASS::get_boolean_val(ExprContext* ctx, TupleRow* row) { \
        StringVal v1 = _children[0]->get_string_val(ctx, row); \
        if (v1.is_null) { \
            return BooleanVal::null(); \
        } \
        StringVal v2 = _children[1]->get_string_val(ctx, row); \
        if (v2.is_null) { \
            return BooleanVal::null(); \
        } \
        StringValue pv1 = StringValue::from_string_val(v1); \
        StringValue pv2 = StringValue::from_string_val(v2); \
        return BooleanVal(pv1 OP pv2); \
    } \
    Status CLASS::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) { \
        return get_codegend_compute_fn_wrapper(state, fn); \
    }

#define STRING_BINARY_PRED_FNS() \
    STRING_BINARY_PRED_FN(Ne##StringVal##Pred, !=) \
    STRING_BINARY_PRED_FN(Lt##StringVal##Pred, <) \
    STRING_BINARY_PRED_FN(Le##StringVal##Pred, <=) \
    STRING_BINARY_PRED_FN(Gt##StringVal##Pred, >) \
    STRING_BINARY_PRED_FN(Ge##StringVal##Pred, >=)

STRING_BINARY_PRED_FNS()

BooleanVal EqStringValPred::get_boolean_val(ExprContext* ctx, TupleRow* row) {
    StringVal v1 = _children[0]->get_string_val(ctx, row);
    if (v1.is_null) {
        return BooleanVal::null();
    }
    StringVal v2 = _children[1]->get_string_val(ctx, row);
    if (v2.is_null) {
        return BooleanVal::null();
    }
    if (v1.len != v2.len) {
        return BooleanVal(false);
    }
    return BooleanVal(string_compare((char*)v1.ptr, v1.len, (char*)v2.ptr, v2.len, v1.len) == 0);
}

Status EqStringValPred::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    return get_codegend_compute_fn_wrapper(state, fn);
}

#if 0
Status EqStringValPred::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Function* lhs_fn = NULL;
    RETURN_IF_ERROR(_children[0]->get_codegend_compute_fn(state, &lhs_fn));
    Function* rhs_fn = NULL;
    RETURN_IF_ERROR(_children[1]->get_codegend_compute_fn(state, &rhs_fn));

    // Function protocol
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "compare", &args);
    LLVMContext& cg_ctx = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(cg_ctx);

    // Constant
    Value* zero = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 0);
    Value* one = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 1);
    Value* false_val = ConstantInt::get(llvm::Type::getInt1Ty(cg_ctx), 0);
    // Value* true_val = ConstantInt::get(llvm::Type::getInt1Ty(cg_ctx), 1);

    // Block
    BasicBlock* entry_block = BasicBlock::Create(cg_ctx, "entry", *fn);
    BasicBlock* lhs_not_null_block = BasicBlock::Create(cg_ctx, "lhs_not_null", *fn);
    BasicBlock* rhs_not_null_block = BasicBlock::Create(cg_ctx, "rhs_not_null", *fn);
    BasicBlock* length_equal_block = BasicBlock::Create(cg_ctx, "length_equal", *fn);
    BasicBlock* null_block = BasicBlock::Create(cg_ctx, "null", *fn);
    BasicBlock* false_block = BasicBlock::Create(cg_ctx, "false", *fn);
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
        codegen, &builder, _children[1]->type(), rhs_fn, args, "rhs_val");
    Value* rhs_is_null = rhs_val.get_is_null();
    builder.CreateCondBr(rhs_is_null, null_block, rhs_not_null_block);

    // rhs_not_null_block
    builder.SetInsertPoint(rhs_not_null_block);
    Value* lhs_len = lhs_val.get_len();
    Value* rhs_len = rhs_val.get_len();
    // if (v1.len != v2.len)
    Value* len_eq = builder.CreateICmpEQ(lhs_len, rhs_len, "len_eq");
    builder.CreateCondBr(len_eq, length_equal_block, false_block);

    // length_equal_block
    builder.SetInsertPoint(length_equal_block);
    Function* compare_fn = codegen->get_function(IRFunction::IR_STRING_COMPARE);
    Value* compare_args[4] = {lhs_val.get_ptr(), lhs_len, lhs_val.get_ptr(), lhs_len};
    Value* compare_res = builder.CreateCall(compare_fn, compare_args, "compare_res");
    Value* int_zero = ConstantInt::get(codegen->get_type(TYPE_INT), 0);
    Value* val = builder.CreateICmpEQ(compare_res, int_zero, "val");
    builder.CreateBr(ret_block);

    // null block
    builder.SetInsertPoint(null_block);
    builder.CreateBr(ret_block);

    // false block
    builder.SetInsertPoint(false_block);
    builder.CreateBr(ret_block);

    // ret block
    builder.SetInsertPoint(ret_block);
    PHINode* is_null_phi = builder.CreatePHI(codegen->tinyint_type(), 3, "is_null_phi");
    is_null_phi->addIncoming(one, null_block);
    is_null_phi->addIncoming(zero, false_block);
    is_null_phi->addIncoming(zero, length_equal_block);
    PHINode* val_phi = builder.CreatePHI(val->getType(), 3, "val_phi");
    Value* null = Constant::getNullValue(val->getType());
    val_phi->addIncoming(null, null_block);
    val_phi->addIncoming(false_val, false_block);
    val_phi->addIncoming(val, length_equal_block);

    CodegenAnyVal result = CodegenAnyVal::get_non_null_val(
        codegen, &builder, type(), "result");
    result.set_is_null(is_null_phi);
    result.set_val(val_phi);
    builder.CreateRet(result.value());

    *fn = codegen->finalize_function(*fn);
    return Status::OK();
}

#endif

}
