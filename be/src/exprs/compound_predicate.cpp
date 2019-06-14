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

#include "exprs/compound_predicate.h"

#include <sstream>

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "util/debug_util.h"
#include "runtime/runtime_state.h"

using llvm::BasicBlock;
using llvm::Function;
using llvm::PHINode;
using llvm::Value;
using llvm::LLVMContext;

namespace doris {

CompoundPredicate::CompoundPredicate(const TExprNode& node) :
        Predicate(node) {
}
#if 0
Status CompoundPredicate::prepare(RuntimeState* state, const RowDescriptor& desc) {
    DCHECK_LE(_children.size(), 2);
    return Expr::prepare(state, desc);
}
#endif

void CompoundPredicate::init() {
}

BooleanVal CompoundPredicate::compound_not(FunctionContext* context, const BooleanVal& v) {
    if (v.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(!v.val);
}

BooleanVal AndPredicate::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_children.size(), 2);
    BooleanVal val1 = _children[0]->get_boolean_val(context, row);
    if (!val1.is_null && !val1.val) {
        return BooleanVal(false);
    }
    BooleanVal val2 = _children[1]->get_boolean_val(context, row);
    if (!val2.is_null && !val2.val) {
        return BooleanVal(false);
    }
    if (val1.is_null || val2.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(true);
}

BooleanVal OrPredicate::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_children.size(), 2);
    BooleanVal val1 = _children[0]->get_boolean_val(context, row);
    if (!val1.is_null && val1.val) {
        return BooleanVal(true);
    }
    BooleanVal val2 = _children[1]->get_boolean_val(context, row);
    if (!val2.is_null && val2.val) {
        return BooleanVal(true);
    }
    if (val1.is_null || val2.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(false);
}

BooleanVal NotPredicate::get_boolean_val(ExprContext* context, TupleRow* row) {
    BooleanVal val = _children[0]->get_boolean_val(context, row);
    if (val.is_null) {
        return BooleanVal::null();
    }
    return BooleanVal(!val.val);
}

std::string CompoundPredicate::debug_string() const {
    std::stringstream out;
    out << "CompoundPredicate(" << Expr::debug_string() << ")";
    return out.str();
}

// IR codegen for compound and/or predicates.  Compound predicate has non trivial 
// null handling as well as many branches so this is pretty complicated.  The IR 
// for x && y is:
//
// define i16 @CompoundPredicate(%"class.impala::ExprContext"* %context,
//                               %"class.impala::TupleRow"* %row) #20 {
// entry:
//   %lhs_call = call i16 @GetSlotRef1(%"class.impala::ExprContext"* %context,
//                                     %"class.impala::TupleRow"* %row)
//   %rhs_call = call i16 @Eq_IntVal_IntValWrapper(%"class.impala::ExprContext"* %context,
//                                                 %"class.impala::TupleRow"* %row)
//   %is_null = trunc i16 %lhs_call to i1
//   %is_null1 = trunc i16 %rhs_call to i1
//   %0 = ashr i16 %lhs_call, 8
//   %1 = trunc i16 %0 to i8
//   %val = trunc i8 %1 to i1
//   %2 = ashr i16 %rhs_call, 8
//   %3 = trunc i16 %2 to i8
//   %val2 = trunc i8 %3 to i1
//   %tmp_and = and i1 %val, %val2
//   br i1 %is_null, label %lhs_null, label %lhs_not_null
// 
// lhs_null:                                         ; preds = %entry
//   br i1 %is_null1, label %null_block, label %lhs_null_rhs_not_null
// 
// lhs_not_null:                                     ; preds = %entry
//   br i1 %is_null1, label %lhs_not_null_rhs_null, label %not_null_block
// 
// lhs_null_rhs_not_null:                            ; preds = %lhs_null
//   br i1 %val2, label %null_block, label %not_null_block
// 
// lhs_not_null_rhs_null:                            ; preds = %lhs_not_null
//   br i1 %val, label %null_block, label %not_null_block
// 
// null_block:                                       ; preds = %lhs_null_rhs_not_null,
//                                                     %lhs_not_null_rhs_null, %lhs_null
//   br label %ret
// 
// not_null_block:                                   ; preds = %lhs_null_rhs_not_null,
//                                                   %lhs_not_null_rhs_null, %lhs_not_null
//   %4 = phi i1 [ false, %lhs_null_rhs_not_null ],
//               [ false, %lhs_not_null_rhs_null ],
//               [ %tmp_and, %lhs_not_null ]
//   br label %ret
// 
// ret:                                              ; preds = %not_null_block, %null_block
//   %ret3 = phi i1 [ false, %null_block ], [ %4, %not_null_block ]
//   %5 = zext i1 %ret3 to i16
//   %6 = shl i16 %5, 8
//   %7 = or i16 0, %6
//   ret i16 %7
// }
Status CompoundPredicate::codegen_compute_fn(
        bool and_fn, RuntimeState* state, Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }

    DCHECK_EQ(get_num_children(), 2);

    Function* lhs_function = NULL;
    RETURN_IF_ERROR(children()[0]->get_codegend_compute_fn(state, &lhs_function));
    Function* rhs_function = NULL;
    RETURN_IF_ERROR(children()[1]->get_codegend_compute_fn(state, &rhs_function));

    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    LLVMContext& context = codegen->context();
    LlvmCodeGen::LlvmBuilder builder(context);
    Value* args[2];
    Function* function = create_ir_function_prototype(codegen, "CompoundPredicate", &args);

    BasicBlock* entry_block = BasicBlock::Create(context, "entry", function);
    builder.SetInsertPoint(entry_block);

    // Control blocks for aggregating results
    BasicBlock* lhs_null_block = BasicBlock::Create(context, "lhs_null", function);
    BasicBlock* lhs_not_null_block = 
        BasicBlock::Create(context, "lhs_not_null", function);
    BasicBlock* lhs_null_rhs_not_null_block = 
        BasicBlock::Create(context, "lhs_null_rhs_not_null", function);
    BasicBlock* lhs_not_null_rhs_null_block = 
        BasicBlock::Create(context, "lhs_not_null_rhs_null", function);
    BasicBlock* null_block = BasicBlock::Create(context, "null_block", function);
    BasicBlock* not_null_block = BasicBlock::Create(context, "not_null_block", function);
    BasicBlock* ret_block = BasicBlock::Create(context, "ret", function);

    // Call lhs
    CodegenAnyVal lhs_result = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, TypeDescriptor(TYPE_BOOLEAN), lhs_function, args, "lhs_call");
    // Call rhs
    CodegenAnyVal rhs_result = CodegenAnyVal::create_call_wrapped(
        codegen, &builder, TypeDescriptor(TYPE_BOOLEAN), rhs_function, args, "rhs_call");

    Value* lhs_is_null = lhs_result.get_is_null();
    Value* rhs_is_null = rhs_result.get_is_null();
    Value* lhs_value = lhs_result.get_val();
    Value* rhs_value = rhs_result.get_val();

    // Apply predicate
    Value* compare = NULL;
    if (and_fn) {
        compare = builder.CreateAnd(lhs_value, rhs_value, "tmp_and");
    } else {
        compare = builder.CreateOr(lhs_value, rhs_value, "tmp_or");
    }

    // Branch if lhs is null
    builder.CreateCondBr(lhs_is_null, lhs_null_block, lhs_not_null_block);

    // lhs_is_null block
    builder.SetInsertPoint(lhs_null_block);
    builder.CreateCondBr(rhs_is_null, null_block, lhs_null_rhs_not_null_block);

    // lhs_is_not_null block
    builder.SetInsertPoint(lhs_not_null_block);
    builder.CreateCondBr(rhs_is_null, lhs_not_null_rhs_null_block, not_null_block);

    // lhs_not_null rhs_null block
    builder.SetInsertPoint(lhs_not_null_rhs_null_block);
    if (and_fn) {
        // false && null -> false; true && null -> null
        builder.CreateCondBr(lhs_value, null_block, not_null_block);
    } else {
        // true || null -> true; false || null -> null
        builder.CreateCondBr(lhs_value, not_null_block, null_block);
    }

    // lhs_null rhs_not_null block
    builder.SetInsertPoint(lhs_null_rhs_not_null_block);
    if (and_fn) {
        // null && false -> false; null && true -> null
        builder.CreateCondBr(rhs_value, null_block, not_null_block);
    } else {
        // null || true -> true; null || false -> null
        builder.CreateCondBr(rhs_value, not_null_block, null_block);
    }

    // NULL block
    builder.SetInsertPoint(null_block);
    builder.CreateBr(ret_block);

    // not-NULL block
    builder.SetInsertPoint(not_null_block);
    PHINode* not_null_phi = builder.CreatePHI(codegen->get_type(TYPE_BOOLEAN), 3);
    if (and_fn) {
        not_null_phi->addIncoming(codegen->false_value(), lhs_null_rhs_not_null_block);
        not_null_phi->addIncoming(codegen->false_value(), lhs_not_null_rhs_null_block);
        not_null_phi->addIncoming(compare, lhs_not_null_block);
    } else {
        not_null_phi->addIncoming(codegen->true_value(), lhs_null_rhs_not_null_block);
        not_null_phi->addIncoming(codegen->true_value(), lhs_not_null_rhs_null_block);
        not_null_phi->addIncoming(compare, lhs_not_null_block);
    }
    builder.CreateBr(ret_block);

    // Ret/merge block
    builder.SetInsertPoint(ret_block);
    PHINode* is_null_phi = builder.CreatePHI(codegen->boolean_type(), 2, "is_null");
    is_null_phi->addIncoming(codegen->true_value(), null_block);
    is_null_phi->addIncoming(codegen->false_value(), not_null_block);

    PHINode* val_phi = builder.CreatePHI(codegen->boolean_type(), 2, "val");
    val_phi->addIncoming(codegen->false_value(), null_block);
    val_phi->addIncoming(not_null_phi, not_null_block);

    CodegenAnyVal ret(codegen, &builder, TypeDescriptor(TYPE_BOOLEAN), NULL, "ret");
    ret.set_is_null(is_null_phi);
    ret.set_val(val_phi);
    builder.CreateRet(ret.value());

    *fn = codegen->finalize_function(function);
    DCHECK(*fn != NULL);
    _ir_compute_fn = *fn;
    return Status::OK();
}

}
