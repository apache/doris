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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/slot-ref.cc
// and modified by Doris

#include "exprs/slot_ref.h"

#include <sstream>

#include "gen_cpp/Exprs_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/runtime_state.h"
#include "util/types.h"

namespace doris {

SlotRef::SlotRef(const TExprNode& node)
        : Expr(node, true),
          _slot_offset(-1), // invalid
          _null_indicator_offset(0, 0),
          _slot_id(node.slot_ref.slot_id),
          _tuple_id(node.slot_ref.tuple_id) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc)
        : Expr(desc->type(), true),
          _slot_offset(-1),
          _null_indicator_offset(0, 0),
          _slot_id(desc->id()) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const SlotDescriptor* desc, const TypeDescriptor& type)
        : Expr(type, true), _slot_offset(-1), _null_indicator_offset(0, 0), _slot_id(desc->id()) {
    // _slot/_null_indicator_offset are set in Prepare()
}

SlotRef::SlotRef(const TypeDescriptor& type, int offset)
        : Expr(type, true),
          _tuple_idx(0),
          _slot_offset(offset),
          _null_indicator_offset(0, -1),
          _slot_id(-1) {}

Status SlotRef::prepare(const SlotDescriptor* slot_desc, const RowDescriptor& row_desc) {
    if (!slot_desc->is_materialized()) {
        return Status::InternalError("reference to non-materialized slot. slot_id: {}", _slot_id);
    }
    _tuple_idx = row_desc.get_tuple_idx(slot_desc->parent());
    if (_tuple_idx == RowDescriptor::INVALID_IDX) {
        return Status::InternalError("failed to get tuple idx with tuple id: {}, slot id: {}",
                                     slot_desc->parent(), _slot_id);
    }
    _tuple_is_nullable = row_desc.tuple_is_nullable(_tuple_idx);
    _slot_offset = slot_desc->tuple_offset();
    _null_indicator_offset = slot_desc->null_indicator_offset();
    _is_nullable = slot_desc->is_nullable();
    return Status::OK();
}

Status SlotRef::prepare(RuntimeState* state, const RowDescriptor& row_desc, ExprContext* ctx) {
    DCHECK_EQ(_children.size(), 0);
    if (_slot_id == -1) {
        return Status::OK();
    }

    const SlotDescriptor* slot_desc = state->desc_tbl().get_slot_descriptor(_slot_id);
    if (slot_desc == nullptr) {
        // TODO: create macro MAKE_ERROR() that returns a stream
        return Status::InternalError("couldn't resolve slot descriptor {}", _slot_id);
    }

    if (!slot_desc->is_materialized()) {
        return Status::InternalError("reference to non-materialized slot. slot_id: {}", _slot_id);
    }

    // TODO(marcel): get from runtime state
    _tuple_idx = row_desc.get_tuple_idx(slot_desc->parent());
    if (_tuple_idx == RowDescriptor::INVALID_IDX) {
        return Status::InternalError(
                "failed to get tuple idx when prepare with tuple id: {}, slot id: {}",
                slot_desc->parent(), _slot_id);
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
    out << "SlotRef(slot_id=" << _slot_id << " tuple_idx=" << _tuple_idx
        << " slot_offset=" << _slot_offset << " null_indicator=" << _null_indicator_offset << " "
        << Expr::debug_string() << ")";
    return out.str();
}

BooleanVal SlotRef::get_boolean_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BOOLEAN);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return BooleanVal::null();
    }
    return BooleanVal(*reinterpret_cast<bool*>(t->get_slot(_slot_offset)));
}

TinyIntVal SlotRef::get_tiny_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_TINYINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return TinyIntVal::null();
    }

    return TinyIntVal(*reinterpret_cast<int8_t*>(t->get_slot(_slot_offset)));
}

SmallIntVal SlotRef::get_small_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_SMALLINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return SmallIntVal::null();
    }
    return SmallIntVal(*reinterpret_cast<int16_t*>(t->get_slot(_slot_offset)));
}

IntVal SlotRef::get_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_INT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return IntVal::null();
    }
    return IntVal(*reinterpret_cast<int32_t*>(t->get_slot(_slot_offset)));
}

BigIntVal SlotRef::get_big_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_BIGINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return BigIntVal::null();
    }
    return BigIntVal(*reinterpret_cast<int64_t*>(t->get_slot(_slot_offset)));
}

LargeIntVal SlotRef::get_large_int_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_LARGEINT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return LargeIntVal::null();
    }
    return LargeIntVal(reinterpret_cast<PackedInt128*>(t->get_slot(_slot_offset))->value);
}

FloatVal SlotRef::get_float_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_FLOAT);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return FloatVal::null();
    }
    return FloatVal(*reinterpret_cast<float*>(t->get_slot(_slot_offset)));
}

DoubleVal SlotRef::get_double_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DOUBLE);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return DoubleVal::null();
    }
    return DoubleVal(*reinterpret_cast<double*>(t->get_slot(_slot_offset)));
}

StringVal SlotRef::get_string_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_string_type());
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
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
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return DateTimeVal::null();
    }
    DateTimeValue* tv = reinterpret_cast<DateTimeValue*>(t->get_slot(_slot_offset));
    DateTimeVal result;
    tv->to_datetime_val(&result);
    return result;
}

DateV2Val SlotRef::get_datev2_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_date_v2_type());
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return DateV2Val::null();
    }
    doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>* tv =
            reinterpret_cast<doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>*>(
                    t->get_slot(_slot_offset));
    DateV2Val result;
    tv->to_datev2_val(&result);
    return result;
}

DateTimeV2Val SlotRef::get_datetimev2_val(ExprContext* context, TupleRow* row) {
    DCHECK(_type.is_datetime_v2_type());
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return DateTimeV2Val::null();
    }
    doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>* tv = reinterpret_cast<
            doris::vectorized::DateV2Value<doris::vectorized::DateTimeV2ValueType>*>(
            t->get_slot(_slot_offset));
    DateTimeV2Val result;
    tv->to_datetimev2_val(&result);
    return result;
}

DecimalV2Val SlotRef::get_decimalv2_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMALV2);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return DecimalV2Val::null();
    }

    return DecimalV2Val(reinterpret_cast<PackedInt128*>(t->get_slot(_slot_offset))->value);
}

Decimal32Val SlotRef::get_decimal32_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMAL32);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return Decimal32Val::null();
    }

    return Decimal32Val(*reinterpret_cast<int32_t*>(t->get_slot(_slot_offset)));
}

Decimal64Val SlotRef::get_decimal64_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMAL64);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return Decimal64Val::null();
    }

    return Decimal64Val(*reinterpret_cast<int64_t*>(t->get_slot(_slot_offset)));
}

Decimal128Val SlotRef::get_decimal128_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_DECIMAL128I);
    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return Decimal128Val::null();
    }

    return Decimal128Val(reinterpret_cast<PackedInt128*>(t->get_slot(_slot_offset))->value);
}

doris_udf::CollectionVal SlotRef::get_array_val(ExprContext* context, TupleRow* row) {
    DCHECK_EQ(_type.type, TYPE_ARRAY);

    Tuple* t = row->get_tuple(_tuple_idx);
    if (t == nullptr || t->is_null(_null_indicator_offset)) {
        return CollectionVal::null();
    }

    CollectionVal val;
    reinterpret_cast<CollectionValue*>(t->get_slot(_slot_offset))->to_collection_val(&val);
    return val;
}
} // namespace doris
