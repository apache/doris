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

#include "null_literal.h"

#include "codegen/llvm_codegen.h"
#include "codegen/codegen_anyval.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"

using llvm::BasicBlock;
using llvm::Function;
using llvm::Value;

namespace doris {

NullLiteral::NullLiteral(const TExprNode& node) : 
        Expr(node) {
}

// NullLiteral::NullLiteral(PrimitiveType type) : Expr(TypeDescriptor(type)) {
// }

BooleanVal NullLiteral::get_boolean_val(ExprContext*, TupleRow*) {
    return BooleanVal::null();
}

TinyIntVal NullLiteral::get_tiny_int_val(ExprContext*, TupleRow*) {
    return TinyIntVal::null();
}

SmallIntVal NullLiteral::get_small_int_val(ExprContext*, TupleRow*) {
    return SmallIntVal::null();
}

IntVal NullLiteral::get_int_val(ExprContext*, TupleRow*) {
    return IntVal::null();
}

BigIntVal NullLiteral::get_big_int_val(ExprContext*, TupleRow*) {
    return BigIntVal::null();
}

FloatVal NullLiteral::get_float_val(ExprContext*, TupleRow*) {
    return FloatVal::null();
}

DoubleVal NullLiteral::get_double_val(ExprContext*, TupleRow*) {
    return DoubleVal::null();
}

StringVal NullLiteral::get_string_val(ExprContext*, TupleRow*) {
    return StringVal::null();
}

DateTimeVal NullLiteral::get_datetime_val(ExprContext*, TupleRow*) {
    return DateTimeVal::null();
}

DecimalVal NullLiteral::get_decimal_val(ExprContext*, TupleRow*) {
    return DecimalVal::null();
}

DecimalV2Val NullLiteral::get_decimalv2_val(ExprContext*, TupleRow*) {
    return DecimalV2Val::null();
}
// Generated IR for a bigint NULL literal:
//
// define { i8, i64 } @NullLiteral(i8* %context, %"class.impala::TupleRow"* %row) {
// entry:
//   ret { i8, i64 } { i8 1, i64 0 }
// }
Status NullLiteral::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }

    DCHECK_EQ(get_num_children(), 0);
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "NullLiteral", &args);
    BasicBlock* entry_block = BasicBlock::Create(codegen->context(), "entry", *fn);
    LlvmCodeGen::LlvmBuilder builder(entry_block);

    Value* v = CodegenAnyVal::get_null_val(codegen, type());
    builder.CreateRet(v);
    *fn = codegen->finalize_function(*fn);
    _ir_compute_fn = *fn;
    return Status::OK();
}

}
