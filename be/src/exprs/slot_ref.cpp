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

#include "exprs/slot_ref.h"

#include <sstream>

#include "codegen/codegen_anyval.h"
#include "codegen/llvm_codegen.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/runtime_state.h"
#include "util/types.h"

using llvm::BasicBlock;
using llvm::Constant;
using llvm::ConstantInt;
using llvm::Function;
using llvm::LLVMContext;
using llvm::PHINode;
using llvm::PointerType;
using llvm::Value;

namespace doris {

SlotRef::SlotRef(const TExprNode& node) :
        Expr(node, true),
    _slot_offset(-1),  // invalid
    _null_indicator_offset(0, 0),
    _slot_id(node.slot_ref.slot_id),
    _tuple_id(node.slot_ref.tuple_id) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc) :
        Expr(desc->type(), true),
    _slot_offset(-1),
    _null_indicator_offset(0, 0),
    _slot_id(desc->id()) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc, const TypeDescriptor& type) :
        Expr(type, true),
        _slot_offset(-1),
        _null_indicator_offset(0, 0),
        _slot_id(desc->id()) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const TypeDescriptor& type, int offset) :
        Expr(type, true),
        _tuple_idx(0),
        _slot_offset(offset),
        _null_indicator_offset(0, -1),
        _slot_id(-1) {
}

Status SlotRef::prepare(
        RuntimeState* state, const RowDescriptor& row_desc, ExprContext* ctx) {
    DCHECK_EQ(_children.size(), 0);
    if (_slot_id == -1) {
        return Status::OK();
    }

    const SlotDescriptor* slot_desc  = state->desc_tbl().get_slot_descriptor(_slot_id);
    if (slot_desc == NULL) {
        // TODO: create macro MAKE_ERROR() that returns a stream
        std::stringstream error;
        error << "couldn't resolve slot descriptor " << _slot_id;
        return Status::InternalError(error.str());
    }

    if (!slot_desc->is_materialized()) {
        std::stringstream error;
        error << "reference to non-materialized slot. slot_id: " << _slot_id;
        return Status::InternalError(error.str());
    }

    // TODO(marcel): get from runtime state
    _tuple_idx = row_desc.get_tuple_idx(slot_desc->parent());
    if (_tuple_idx == RowDescriptor::INVALID_IDX) {
        return Status::InternalError("can't support");
    }
    DCHECK(_tuple_idx != RowDescriptor::INVALID_IDX);
    _tuple_is_nullable = row_desc.tuple_is_nullable(_tuple_idx);
    _slot_offset = slot_desc->tuple_offset();
    _null_indicator_offset = slot_desc->null_indicator_offset();
    _is_nullable = slot_desc->is_nullable();
    return Status::OK();
}

int SlotRef::get_slot_ids(std::vector<SlotId>* slot_ids) const {
    slot_ids->push_back(_slot_id);
    return 1;
}

bool SlotRef::is_bound(std::vector<TupleId>* tuple_ids) const {
    for (int i = 0; i < tuple_ids->size(); i++) {
        if (_tuple_id == (*tuple_ids)[i]) {
            return true;
        }
    }

    return false;
}

std::string SlotRef::debug_string() const {
    std::stringstream out;
    out << "SlotRef(slot_id=" << _slot_id
        << " tuple_idx=" << _tuple_idx << " slot_offset=" << _slot_offset
        << " null_indicator=" << _null_indicator_offset
        << " " << Expr::debug_string() << ")";
    return out.str();
}

// There are four possible cases we may generate:
//   1. Tuple is non-nullable and slot is non-nullable
//   2. Tuple is non-nullable and slot is nullable
//   3. Tuple is nullable and slot is non-nullable (when the aggregate output is the
//      "nullable" side of an outer join).
//   4. Tuple is nullable and slot is nullable
//
// Resulting IR for a bigint slotref:
// (Note: some of the GEPs that look like no-ops are because certain offsets are 0
// in this slot descriptor.)
//
// define { i8, i64 } @get_slot_ref(i8** %context, %"class.doris::TupleRow"* %row) {
// entry:
//   %cast_row_ptr = bitcast %"class.doris::TupleRow"* %row to i8**
//   %tuple_addr = getelementptr i8** %cast_row_ptr, i32 0
//   %tuple_ptr = load i8** %tuple_addr
//   br label %check_slot_null
//
// check_slot_null:                                  ; preds = %entry
//   %null_ptr = getelementptr i8* %tuple_ptr, i32 0
//   %null_byte = load i8* %null_ptr
//   %null_byte_set = and i8 %null_byte, 2
//   %slot_is_null = icmp ne i8 %null_byte_set, 0
//   br i1 %slot_is_null, label %ret, label %get_slot
//
// get_slot:                                         ; preds = %check_slot_null
//   %slot_addr = getelementptr i8* %tuple_ptr, i32 8
//   %val_ptr = bitcast i8* %slot_addr to i64*
//   %val = load i64* %val_ptr
//   br label %ret
//
// ret:                                              ; preds = %get_slot, %check_slot_null
//   %is_null_phi = phi i8 [ 1, %check_slot_null ], [ 0, %get_slot ]
//   %val_phi = phi i64 [ 0, %check_slot_null ], [ %val, %get_slot ]
//   %result = insertvalue { i8, i64 } zeroinitializer, i8 %is_null_phi, 0
//   %result1 = insertvalue { i8, i64 } %result, i64 %val_phi, 1
//   ret { i8, i64 } %result1
// }
//
// TODO: We could generate a typed struct (and not a char*) for Tuple for llvm.  We know
// the types from the TupleDesc.  It will likey make this code simpler to reason about.
Status SlotRef::get_codegend_compute_fn(RuntimeState* state, llvm::Function** fn) {
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }

    DCHECK_EQ(get_num_children(), 0);
    LlvmCodeGen* codegen = NULL;
    RETURN_IF_ERROR(state->get_codegen(&codegen));

    // SlotRefs are based on the slot_id and tuple_idx.  Combine them to make a
    // query-wide unique id. We also need to combine whether the tuple is nullable. For
    // example, in an outer join the scan node could have the same tuple id and slot id
    // as the join node. When the slot is being used in the scan-node, the tuple is
    // non-nullable. Used in the join node (and above in the plan tree), it is nullable.
    // TODO: can we do something better.
    const int64_t TUPLE_NULLABLE_MASK = 1L << 63;
    int64_t unique_slot_id = _slot_id | ((int64_t)_tuple_idx) << 32;
    DCHECK_EQ(unique_slot_id & TUPLE_NULLABLE_MASK, 0);
    if (_tuple_is_nullable) {
        unique_slot_id |= TUPLE_NULLABLE_MASK;
    }
    Function* _ir_compute_fn = codegen->get_registered_expr_fn(unique_slot_id);
    if (_ir_compute_fn != NULL) {
        *fn = _ir_compute_fn;
        return Status::OK();
    }

    LLVMContext& context = codegen->context();
    Value* args[2];
    *fn = create_ir_function_prototype(codegen, "get_slot_ref", &args);
    Value* row_ptr = args[1];

    Value* tuple_offset = ConstantInt::get(codegen->int_type(), _tuple_idx);
    Value* null_byte_offset = ConstantInt::get(
        codegen->int_type(), _null_indicator_offset.byte_offset);
    Value* slot_offset = ConstantInt::get(codegen->int_type(), _slot_offset);
    Value* null_mask = ConstantInt::get(
        codegen->tinyint_type(), _null_indicator_offset.bit_mask);
    Value* zero = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 0);
    Value* one = ConstantInt::get(codegen->get_type(TYPE_TINYINT), 1);

    BasicBlock* entry_block = BasicBlock::Create(context, "entry", *fn);
    BasicBlock* check_slot_null_indicator_block = NULL;
    if (_null_indicator_offset.bit_mask != 0) {
        check_slot_null_indicator_block = BasicBlock::Create(
            context, "check_slot_null", *fn);
    }
    BasicBlock* get_slot_block = BasicBlock::Create(context, "get_slot", *fn);
    BasicBlock* ret_block = BasicBlock::Create(context, "ret", *fn);

    LlvmCodeGen::LlvmBuilder builder(entry_block);
    // Get the tuple offset addr from the row
    Value* cast_row_ptr = builder.CreateBitCast(
        row_ptr, PointerType::get(codegen->ptr_type(), 0), "cast_row_ptr");
    Value* tuple_addr = builder.CreateGEP(cast_row_ptr, tuple_offset, "tuple_addr");
    // Load the tuple*
    Value* tuple_ptr = builder.CreateLoad(tuple_addr, "tuple_ptr");

    // Check if tuple* is null only if the tuple is nullable
    if (_tuple_is_nullable) {
        Value* tuple_is_null = builder.CreateIsNull(tuple_ptr, "tuple_is_null");
        // Check slot is null only if the null indicator bit is set
        if (_null_indicator_offset.bit_mask == 0) {
            builder.CreateCondBr(tuple_is_null, ret_block, get_slot_block);
        } else {
            builder.CreateCondBr(tuple_is_null, ret_block, check_slot_null_indicator_block);
        }
    } else {
        if (_null_indicator_offset.bit_mask == 0) {
            builder.CreateBr(get_slot_block);
        } else {
            builder.CreateBr(check_slot_null_indicator_block);
        }
    }

    // Branch for tuple* != NULL.  Need to check if null-indicator is set
    if (check_slot_null_indicator_block != NULL) {
        builder.SetInsertPoint(check_slot_null_indicator_block);
        Value* null_addr = builder.CreateGEP(tuple_ptr, null_byte_offset, "null_ptr");
        Value* null_val = builder.CreateLoad(null_addr, "null_byte");
        Value* slot_null_mask = builder.CreateAnd(null_val, null_mask, "null_byte_set");
        Value* is_slot_null = builder.CreateICmpNE(slot_null_mask, zero, "slot_is_null");
        builder.CreateCondBr(is_slot_null, ret_block, get_slot_block);
    }

    // Branch for slot != NULL
    builder.SetInsertPoint(get_slot_block);
    Value* slot_ptr = builder.CreateGEP(tuple_ptr, slot_offset, "slot_addr");
    Value* val_ptr = builder.CreateBitCast(slot_ptr, codegen->get_ptr_type(_type), "val_ptr");
    // Depending on the type, load the values we need
    Value* val = NULL;
    Value* ptr = NULL;
    Value* len = NULL;
    if (_type.is_string_type()) {
        Value* ptr_ptr = builder.CreateStructGEP(val_ptr, 0, "ptr_ptr");
        ptr = builder.CreateLoad(ptr_ptr, "ptr");
        Value* len_ptr = builder.CreateStructGEP(val_ptr, 1, "len_ptr");
        len = builder.CreateLoad(len_ptr, "len");
    } else if (_type.is_date_type()) {
        // TODO(zc)
#if 0
        Value* time_of_day_ptr = builder.CreateStructGEP(val_ptr, 0, "time_of_day_ptr");
        // Cast boost::posix_time::time_duration to i64
        Value* time_of_day_cast =
            builder.CreateBitCast(time_of_day_ptr, codegen->get_ptr_type(TYPE_BIGINT));
        time_of_day = builder.CreateLoad(time_of_day_cast, "time_of_day");
        Value* date_ptr = builder.CreateStructGEP(val_ptr, 1, "date_ptr");
        // Cast boost::gregorian::date to i32
        Value* date_cast = builder.CreateBitCast(date_ptr, codegen->get_ptr_type(TYPE_INT));
        date = builder.CreateLoad(date_cast, "date");
#endif
        Function* func = codegen->get_function(IRFunction::TO_DATETIME_VAL);
        Value* val_val_ptr = codegen->create_entry_block_alloca(
            builder, CodegenAnyVal::get_lowered_type(codegen, _type), "val_val_ptr");
        builder.CreateCall2(func, val_ptr, val_val_ptr);
        val = builder.CreateLoad(val_val_ptr, "val");
    } else if (_type.is_decimal_type()) {
        // TODO(zc): to think about it
        Function* func = codegen->get_function(IRFunction::TO_DECIMAL_VAL);
        Value* val_val_ptr = codegen->create_entry_block_alloca(
            builder, CodegenAnyVal::get_lowered_type(codegen, _type), "val_val_ptr");
        builder.CreateCall2(func, val_ptr, val_val_ptr);
        val = builder.CreateLoad(val_val_ptr, "val");
    } else {
        // val_ptr is a native type
        val = builder.CreateLoad(val_ptr, "val");
    }
    builder.CreateBr(ret_block);

    // Return block
    builder.SetInsertPoint(ret_block);
    PHINode* is_null_phi = builder.CreatePHI(codegen->tinyint_type(), 2, "is_null_phi");
    if (_tuple_is_nullable) {
        is_null_phi->addIncoming(one, entry_block);
    }
    if (check_slot_null_indicator_block != NULL) {
        is_null_phi->addIncoming(one, check_slot_null_indicator_block);
    }
    is_null_phi->addIncoming(zero, get_slot_block);

    // Depending on the type, create phi nodes for each value needed to populate the return
    // *Val. The optimizer does a better job when there is a phi node for each value, rather
    // than having get_slot_block generate an AnyVal and having a single phi node over that.
    // TODO: revisit this code, can possibly be simplified
    if (type().is_string_type()) {
        DCHECK(ptr != NULL);
        DCHECK(len != NULL);
        PHINode* ptr_phi = builder.CreatePHI(ptr->getType(), 2, "ptr_phi");
        Value* null = Constant::getNullValue(ptr->getType());
        if (_tuple_is_nullable) {
            ptr_phi->addIncoming(null, entry_block);
        }
        if (check_slot_null_indicator_block != NULL) {
            ptr_phi->addIncoming(null, check_slot_null_indicator_block);
        }
        ptr_phi->addIncoming(ptr, get_slot_block);

        PHINode* len_phi = builder.CreatePHI(len->getType(), 2, "len_phi");
        null = ConstantInt::get(len->getType(), 0);
        if (_tuple_is_nullable) {
            len_phi->addIncoming(null, entry_block);
        }
        if (check_slot_null_indicator_block != NULL) {
            len_phi->addIncoming(null, check_slot_null_indicator_block);
        }
        len_phi->addIncoming(len, get_slot_block);

        CodegenAnyVal result = CodegenAnyVal::get_non_null_val(
            codegen, &builder, type(), "result");
        result.set_is_null(is_null_phi);
        result.set_ptr(ptr_phi);
        result.set_len(len_phi);
        builder.CreateRet(result.value());
#if 0
    } else if (type().is_date_type()) {
        DCHECK(time_of_day != NULL);
        DCHECK(date != NULL);
        PHINode* time_of_day_phi =
            builder.CreatePHI(time_of_day->getType(), 2, "time_of_day_phi");
        Value* null = ConstantInt::get(time_of_day->getType(), 0);
        if (_tuple_is_nullable) {
            time_of_day_phi->addIncoming(null, entry_block);
        }
        if (check_slot_null_indicator_block != NULL) {
            time_of_day_phi->addIncoming(null, check_slot_null_indicator_block);
        }
        time_of_day_phi->addIncoming(time_of_day, get_slot_block);

        PHINode* date_phi = builder.CreatePHI(date->getType(), 2, "date_phi");
        null = ConstantInt::get(date->getType(), 0);
        if (_tuple_is_nullable) {
            date_phi->addIncoming(null, entry_block);
        }
        if (check_slot_null_indicator_block != NULL) {
            date_phi->addIncoming(null, check_slot_null_indicator_block);
        }
        date_phi->addIncoming(date, get_slot_block);

        CodegenAnyVal result = CodegenAnyVal::get_non_null_val(
            codegen, &builder, type(), "result");
        result.set_is_null(is_null_phi);
        result.set_time_of_day(time_of_day_phi);
        result.set_date(date_phi);
        builder.CreateRet(result.value());
#endif
    } else if (type().is_decimal_type() || type().is_date_type()) {
        PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
        Value* null = Constant::getNullValue(val->getType());
        if (_tuple_is_nullable) {
            val_phi->addIncoming(null, entry_block);
        }
        if (check_slot_null_indicator_block != NULL) {
            val_phi->addIncoming(null, check_slot_null_indicator_block);
        }
        val_phi->addIncoming(val, get_slot_block);

        CodegenAnyVal result(codegen, &builder, _type, val_phi, "result");
        result.set_is_null(is_null_phi);
        builder.CreateRet(result.value());
    } else {
        DCHECK(val != NULL);
        PHINode* val_phi = builder.CreatePHI(val->getType(), 2, "val_phi");
        Value* null = Constant::getNullValue(val->getType());
        if (_tuple_is_nullable) {
            val_phi->addIncoming(null, entry_block);
        }
        if (check_slot_null_indicator_block != NULL) {
            val_phi->addIncoming(null, check_slot_null_indicator_block);
        }
        val_phi->addIncoming(val, get_slot_block);

        CodegenAnyVal result = CodegenAnyVal::get_non_null_val(
            codegen, &builder, type(), "result");
        result.set_is_null(is_null_phi);
        result.set_val(val_phi);
        builder.CreateRet(result.value());
    }

    *fn = codegen->finalize_function(*fn);
    codegen->register_expr_fn(unique_slot_id, *fn);
    _ir_compute_fn = *fn;
    return Status::OK();
}

BooleanVal SlotRef::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BOOLEAN);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return BooleanVal::null();
    }
    return BooleanVal(*reinterpret_cast<bool*>(t->get_slot(_slot_offset)));
}

TinyIntVal SlotRef::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_TINYINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return TinyIntVal::null();
    }

    return TinyIntVal(*reinterpret_cast<int8_t*>(t->get_slot(_slot_offset)));
}

SmallIntVal SlotRef::get_small_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_SMALLINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return SmallIntVal::null();
    }
    return SmallIntVal(*reinterpret_cast<int16_t*>(t->get_slot(_slot_offset)));
}

IntVal SlotRef::get_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_INT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return IntVal::null();
    }
    return IntVal(*reinterpret_cast<int32_t*>(t->get_slot(_slot_offset)));
}

BigIntVal SlotRef::get_big_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BIGINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return BigIntVal::null();
    }
    return BigIntVal(*reinterpret_cast<int64_t*>(t->get_slot(_slot_offset)));
}

LargeIntVal SlotRef::get_large_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_LARGEINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return LargeIntVal::null();
    }
    return LargeIntVal(reinterpret_cast<PackedInt128*>(t->get_slot(_slot_offset))->value);
}

FloatVal SlotRef::get_float_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_FLOAT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return FloatVal::null();
    }
    return FloatVal(*reinterpret_cast<float*>(t->get_slot(_slot_offset)));
}

DoubleVal SlotRef::get_double_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DOUBLE);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return DoubleVal::null();
    }
    return DoubleVal(*reinterpret_cast<double*>(t->get_slot(_slot_offset)));
}

StringVal SlotRef::get_string_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_string_type());
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return StringVal::null();
    }
    StringVal result;
    StringValue* sv = reinterpret_cast<StringValue*>(t->get_slot(_slot_offset));
    sv->to_string_val(&result);
    return result;
}

DateTimeVal SlotRef::get_datetime_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_date_type());
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return DateTimeVal::null();
    }
    DateTimeValue* tv = reinterpret_cast<DateTimeValue*>(t->get_slot(_slot_offset));
    DateTimeVal result;
    tv->to_datetime_val(&result);
    return result;
}

TimeVal SlotRef::get_time_val(ExprContext* context, TupleRow* row) {
    Tuple *t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return TimeVal::null();
    }
    DateTimeValue *tv = reinterpret_cast<DateTimeValue *>(t->get_slot(_slot_offset));
    TimeVal tm_val;
    tv->to_time_val(&tm_val);
    return tm_val;
}

DecimalVal SlotRef::get_decimal_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMAL);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return DecimalVal::null();
    }
    DecimalVal dec_val;
    reinterpret_cast<DecimalValue*>(t->get_slot(_slot_offset))->to_decimal_val(&dec_val);
    return dec_val;
}

DecimalV2Val SlotRef::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMALV2);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == NULL || t->is_null(_null_indicator_offset)) {
        return DecimalV2Val::null();
    }

    return DecimalV2Val(reinterpret_cast<PackedInt128*>(t->get_slot(_slot_offset))->value);
}

}
