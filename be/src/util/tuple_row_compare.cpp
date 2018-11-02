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

#include "util/tuple_row_compare.h"

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "runtime/runtime_state.h"

using llvm::BasicBlock;
using llvm::LLVMContext;
using llvm::Function;
using llvm::PointerType;
using llvm::Value;

namespace doris {

bool TupleRowComparator::codegen(RuntimeState* state) {
    Function* fn = codegen_compare(state);
    if (fn == NULL) {
        return false;
    }
    LlvmCodeGen* codegen = NULL;
    bool got_codegen = state->get_codegen(&codegen).ok();
    DCHECK(got_codegen);
    codegen->add_function_to_jit(fn, reinterpret_cast<void**>(&_codegend_compare_fn));
    return true;
}

// Codegens an unrolled version of Compare(). Uses codegen'd key exprs and injects
// _nulls_first and is_asc_ values.
//
// Example IR for comparing an int column then a float column:
//
// ; Function Attrs: alwaysinline
// define i32 @Compare(%"class.doris::ExprContext"** %key_expr_ctxs_lhs,
//                     %"class.doris::ExprContext"** %key_expr_ctxs_rhs,
//                     %"class.doris::TupleRow"* %lhs,
//                     %"class.doris::TupleRow"* %rhs) #20 {
// entry:
//   %type13 = alloca %"struct.doris::TypeDescriptor"
//   %0 = alloca float
//   %1 = alloca float
//   %type = alloca %"struct.doris::TypeDescriptor"
//   %2 = alloca i32
//   %3 = alloca i32
//   %4 = getelementptr %"class.doris::ExprContext"** %key_expr_ctxs_lhs, i32 0
//   %5 = load %"class.doris::ExprContext"** %4
//   %lhs_value = call i64 @GetSlotRef(
//       %"class.doris::ExprContext"* %5, %"class.doris::TupleRow"* %lhs)
//   %6 = getelementptr %"class.doris::ExprContext"** %key_expr_ctxs_rhs, i32 0
//   %7 = load %"class.doris::ExprContext"** %6
//   %rhs_value = call i64 @GetSlotRef(
//       %"class.doris::ExprContext"* %7, %"class.doris::TupleRow"* %rhs)
//   %is_null = trunc i64 %lhs_value to i1
//   %is_null1 = trunc i64 %rhs_value to i1
//   %both_null = and i1 %is_null, %is_null1
//   br i1 %both_null, label %next_key, label %non_null
//
// non_null:                                         ; preds = %entry
//   br i1 %is_null, label %lhs_null, label %lhs_non_null
//
// lhs_null:                                         ; preds = %non_null
//   ret i32 1
//
// lhs_non_null:                                     ; preds = %non_null
//   br i1 %is_null1, label %rhs_null, label %rhs_non_null
//
// rhs_null:                                         ; preds = %lhs_non_null
//   ret i32 -1
//
// rhs_non_null:                                     ; preds = %lhs_non_null
//   %8 = ashr i64 %lhs_value, 32
//   %9 = trunc i64 %8 to i32
//   store i32 %9, i32* %3
//   %10 = bitcast i32* %3 to i8*
//   %11 = ashr i64 %rhs_value, 32
//   %12 = trunc i64 %11 to i32
//   store i32 %12, i32* %2
//   %13 = bitcast i32* %2 to i8*
//   store %"struct.doris::TypeDescriptor" { i32 5, i32 -1, i32 -1, i32 -1,
//                                        %"class.std::vector.44" zeroinitializer,
//                                        %"class.std::vector.49" zeroinitializer },
//         %"struct.doris::TypeDescriptor"* %type
//   %result = call i32 @_ZN4doris8RawValue7CompareEPKvS2_RKNS_10ColumnTypeE(
//       i8* %10, i8* %13, %"struct.doris::TypeDescriptor"* %type)
//   %14 = icmp ne i32 %result, 0
//   br i1 %14, label %result_nonzero, label %next_key
//
// result_nonzero:                                   ; preds = %rhs_non_null
//   ret i32 %result
//
// next_key:                                         ; preds = %rhs_non_null, %entry
//   %15 = getelementptr %"class.doris::ExprContext"** %key_expr_ctxs_lhs, i32 1
//   %16 = load %"class.doris::ExprContext"** %15
//   %lhs_value3 = call i64 @GetSlotRef1(
//       %"class.doris::ExprContext"* %16, %"class.doris::TupleRow"* %lhs)
//   %17 = getelementptr %"class.doris::ExprContext"** %key_expr_ctxs_rhs, i32 1
//   %18 = load %"class.doris::ExprContext"** %17
//   %rhs_value4 = call i64 @GetSlotRef1(
//       %"class.doris::ExprContext"* %18, %"class.doris::TupleRow"* %rhs)
//   %is_null5 = trunc i64 %lhs_value3 to i1
//   %is_null6 = trunc i64 %rhs_value4 to i1
//   %both_null8 = and i1 %is_null5, %is_null6
//   br i1 %both_null8, label %next_key2, label %non_null7
//
// non_null7:                                        ; preds = %next_key
//   br i1 %is_null5, label %lhs_null9, label %lhs_non_null10
//
// lhs_null9:                                        ; preds = %non_null7
//   ret i32 1
//
// lhs_non_null10:                                   ; preds = %non_null7
//   br i1 %is_null6, label %rhs_null11, label %rhs_non_null12
//
// rhs_null11:                                       ; preds = %lhs_non_null10
//   ret i32 -1
//
// rhs_non_null12:                                   ; preds = %lhs_non_null10
//   %19 = ashr i64 %lhs_value3, 32
//   %20 = trunc i64 %19 to i32
//   %21 = bitcast i32 %20 to float
//   store float %21, float* %1
//   %22 = bitcast float* %1 to i8*
//   %23 = ashr i64 %rhs_value4, 32
//   %24 = trunc i64 %23 to i32
//   %25 = bitcast i32 %24 to float
//   store float %25, float* %0
//   %26 = bitcast float* %0 to i8*
//   store %"struct.doris::TypeDescriptor" { i32 7, i32 -1, i32 -1, i32 -1,
//                                        %"class.std::vector.44" zeroinitializer,
//                                        %"class.std::vector.49" zeroinitializer },
//         %"struct.doris::TypeDescriptor"* %type13
//   %result14 = call i32 @_ZN4doris8RawValue7CompareEPKvS2_RKNS_10ColumnTypeE(
//       i8* %22, i8* %26, %"struct.doris::TypeDescriptor"* %type13)
//   %27 = icmp ne i32 %result14, 0
//   br i1 %27, label %result_nonzero15, label %next_key2
//
// result_nonzero15:                                 ; preds = %rhs_non_null12
//   ret i32 %result14
//
// next_key2:                                        ; preds = %rhs_non_null12, %next_key
//   ret i32 0
// }
Function* TupleRowComparator::codegen_compare(RuntimeState* state) {
    LlvmCodeGen* codegen = NULL;
    if (!state->get_codegen(&codegen).ok()) {
        return NULL;
    }
    SCOPED_TIMER(codegen->codegen_timer());
    LLVMContext& context = codegen->context();

    // Get all the key compute functions from _key_expr_ctxs_lhs. The lhs and rhs functions
    // are the same since they're clones, and _key_expr_ctxs_rhs is not populated until
    // Open() is called.
    // DCHECK(_key_expr_ctxs_rhs.empty()) << "rhs exprs should be clones of lhs!";
    Function* key_fns[_key_expr_ctxs_lhs.size()];
    for (int i = 0; i < _key_expr_ctxs_lhs.size(); ++i) {
        Status status = _key_expr_ctxs_lhs[i]->root()->get_codegend_compute_fn(
            state, &key_fns[i]);
        if (!status.ok()) {
            VLOG_QUERY << "Could not codegen TupleRowComparator::Compare(): " 
                << status.get_error_msg();
            return NULL;
        }
    }

    // Construct function signature (note that this is different than the interpreted
    // Compare() function signature):
    // int compare(ExprContext** key_expr_ctxs_lhs, ExprContext** key_expr_ctxs_rhs,
    //     TupleRow* lhs, TupleRow* rhs)
    PointerType* expr_ctxs_type = codegen->get_ptr_type(
        ExprContext::_s_llvm_class_name)->getPointerTo();
    PointerType* tuple_row_type = codegen->get_ptr_type(TupleRow::_s_llvm_class_name);
    LlvmCodeGen::FnPrototype prototype(codegen, "compare", codegen->int_type());
    prototype.add_argument("key_expr_ctxs_lhs", expr_ctxs_type);
    prototype.add_argument("key_expr_ctxs_rhs", expr_ctxs_type);
    prototype.add_argument("lhs", tuple_row_type);
    prototype.add_argument("rhs", tuple_row_type);

    LlvmCodeGen::LlvmBuilder builder(codegen->context());
    Value* args[4];
    Function* fn = prototype.generate_prototype(&builder, args);
    Value* lhs_ctxs_arg = args[0];
    Value* rhs_ctxs_arg = args[1];
    Value* lhs_arg = args[2];
    Value* rhs_arg = args[3];

    // Unrolled loop over each key expr
    for (int i = 0; i < _key_expr_ctxs_lhs.size(); ++i) {
        // The start of the next key expr after this one. Used to implement "continue" logic
        // in the unrolled loop.
        BasicBlock* next_key_block = BasicBlock::Create(context, "next_key", fn);

        // Call key_fns[i](key_expr_ctxs_lhs[i], lhs_arg)
        Value* lhs_ctx = codegen->codegen_array_at(&builder, lhs_ctxs_arg, i, "");
        Value* lhs_args[] = { lhs_ctx, lhs_arg };
        CodegenAnyVal lhs_value = CodegenAnyVal::create_call_wrapped(
            codegen, &builder, _key_expr_ctxs_lhs[i]->root()->type(), 
            key_fns[i], lhs_args, "lhs_value", NULL);

        // Call key_fns[i](key_expr_ctxs_rhs[i], rhs_arg)
        Value* rhs_ctx = codegen->codegen_array_at(&builder, rhs_ctxs_arg, i, "");
        Value* rhs_args[] = { rhs_ctx, rhs_arg };
        CodegenAnyVal rhs_value = CodegenAnyVal::create_call_wrapped(
            codegen, &builder, _key_expr_ctxs_lhs[i]->root()->type(),
            key_fns[i], rhs_args, "rhs_value", NULL);

        // Handle NULLs if necessary
        Value* lhs_null = lhs_value.get_is_null();
        Value* rhs_null = rhs_value.get_is_null();
        // if (lhs_value == NULL && rhs_value == NULL) continue;
        Value* both_null = builder.CreateAnd(lhs_null, rhs_null, "both_null");
        BasicBlock* non_null_block =
            BasicBlock::Create(context, "non_null", fn, next_key_block);
        builder.CreateCondBr(both_null, next_key_block, non_null_block);
        // if (lhs_value == NULL && rhs_value != NULL) return _nulls_first[i];
        builder.SetInsertPoint(non_null_block);
        BasicBlock* lhs_null_block =
            BasicBlock::Create(context, "lhs_null", fn, next_key_block);
        BasicBlock* lhs_non_null_block =
            BasicBlock::Create(context, "lhs_non_null", fn, next_key_block);
        builder.CreateCondBr(lhs_null, lhs_null_block, lhs_non_null_block);
        builder.SetInsertPoint(lhs_null_block);
        builder.CreateRet(builder.getInt32(_nulls_first[i]));
        // if (lhs_value != NULL && rhs_value == NULL) return -_nulls_first[i];
        builder.SetInsertPoint(lhs_non_null_block);
        BasicBlock* rhs_null_block =
            BasicBlock::Create(context, "rhs_null", fn, next_key_block);
        BasicBlock* rhs_non_null_block =
            BasicBlock::Create(context, "rhs_non_null", fn, next_key_block);
        builder.CreateCondBr(rhs_null, rhs_null_block, rhs_non_null_block);
        builder.SetInsertPoint(rhs_null_block);
        builder.CreateRet(builder.getInt32(-_nulls_first[i]));

        // int result = RawValue::Compare(lhs_value, rhs_value, <type>)
        builder.SetInsertPoint(rhs_non_null_block);
        Value* result = lhs_value.compare(&rhs_value, "result");

        // if (!is_asc_[i]) result = -result;
        if (!_is_asc[i]) {
            result = builder.CreateSub(builder.getInt32(0), result, "result");
        }
        // if (result != 0) return result;
        // Otherwise, try the next Expr
        Value* result_nonzero = builder.CreateICmpNE(result, builder.getInt32(0));
        BasicBlock* result_nonzero_block =
            BasicBlock::Create(context, "result_nonzero", fn, next_key_block);
        builder.CreateCondBr(result_nonzero, result_nonzero_block, next_key_block);
        builder.SetInsertPoint(result_nonzero_block);
        builder.CreateRet(result);

        // Get builder ready for next iteration or final return
        builder.SetInsertPoint(next_key_block);
    }
    builder.CreateRet(builder.getInt32(0));
    return codegen->finalize_function(fn);
}

}
