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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_SLOT_REF_H
#define DORIS_BE_SRC_QUERY_EXPRS_SLOT_REF_H

#include "common/object_pool.h"
#include "exprs/expr.h"

namespace doris {

// Reference to a single slot of a tuple.
// We inline this here in order for Expr::get_value() to be able
// to reference SlotRef::compute_fn() directly.
// Splitting it up into separate .h files would require circular #includes.
class SlotRef final : public Expr {
public:
    SlotRef(const TExprNode& node);
    SlotRef(const SlotDescriptor* desc);
    virtual Expr* clone(ObjectPool* pool) const override { return pool->add(new SlotRef(*this)); }

    // TODO: this is a hack to allow aggregation nodes to work around nullptr slot
    // descriptors. Ideally the FE would dictate the type of the intermediate SlotRefs.
    SlotRef(const SlotDescriptor* desc, const TypeDescriptor& type);

    // Used for testing.  get_value will return tuple + offset interpreted as 'type'
    SlotRef(const TypeDescriptor& type, int offset);

    Status prepare(const SlotDescriptor* slot_desc, const RowDescriptor& row_desc);

    virtual Status prepare(RuntimeState* state, const RowDescriptor& row_desc,
                           ExprContext* ctx) override;
    static void* get_value(Expr* expr, TupleRow* row);
    void* get_slot(TupleRow* row);
    Tuple* get_tuple(TupleRow* row);
    bool is_null_bit_set(TupleRow* row);
    static bool vector_compute_fn(Expr* expr, VectorizedRowBatch* batch);
    static bool is_nullable(Expr* expr);
    virtual std::string debug_string() const override;
    virtual bool is_constant() const override { return false; }
    virtual bool is_vectorized() const override { return true; }
    virtual bool is_bound(std::vector<TupleId>* tuple_ids) const override;
    virtual int get_slot_ids(std::vector<SlotId>* slot_ids) const override;
    SlotId slot_id() const { return _slot_id; }
    inline NullIndicatorOffset null_indicator_offset() const { return _null_indicator_offset; }

    virtual doris_udf::BooleanVal get_boolean_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::TinyIntVal get_tiny_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::SmallIntVal get_small_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::IntVal get_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::BigIntVal get_big_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::LargeIntVal get_large_int_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::FloatVal get_float_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DoubleVal get_double_val(ExprContext* context, TupleRow* row) override;
    virtual doris_udf::StringVal get_string_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DateTimeVal get_datetime_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::DecimalV2Val get_decimalv2_val(ExprContext* context, TupleRow*) override;
    virtual doris_udf::CollectionVal get_array_val(ExprContext* context, TupleRow*) override;

private:
    int _tuple_idx;                             // within row
    int _slot_offset;                           // within tuple
    NullIndicatorOffset _null_indicator_offset; // within tuple
    const SlotId _slot_id;
    bool _tuple_is_nullable; // true if the tuple is nullable.
    TupleId _tuple_id;       // used for desc this slot from
    bool _is_nullable;
};

inline bool SlotRef::vector_compute_fn(Expr* expr, VectorizedRowBatch* /* batch */) {
    return true;
}

inline void* SlotRef::get_value(Expr* expr, TupleRow* row) {
    SlotRef* ref = (SlotRef*)expr;
    Tuple* t = row->get_tuple(ref->_tuple_idx);
    if (t == nullptr || t->is_null(ref->_null_indicator_offset)) {
        return nullptr;
    }
    return t->get_slot(ref->_slot_offset);
}

inline void* SlotRef::get_slot(TupleRow* row) {
    //get_slot需要获取slot所在的position,
    //以用于在小批量导入聚合时修改其内容
    Tuple* t = row->get_tuple(_tuple_idx);
    return t->get_slot(_slot_offset);
}

inline Tuple* SlotRef::get_tuple(TupleRow* row) {
    Tuple* t = row->get_tuple(_tuple_idx);
    return t;
}

inline bool SlotRef::is_null_bit_set(TupleRow* row) {
    Tuple* t = row->get_tuple(_tuple_idx);
    return t->is_null(_null_indicator_offset);
}

inline bool SlotRef::is_nullable(Expr* expr) {
    SlotRef* ref = (SlotRef*)expr;
    return ref->_is_nullable;
}

} // namespace doris

#endif
