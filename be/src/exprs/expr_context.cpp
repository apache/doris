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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/expr-context.cc
// and modified by Doris

#include "exprs/expr_context.h"

#include <gperftools/profiler.h>

#include <sstream>

#include "exprs/anyval_util.h"
#include "exprs/expr.h"
#include "exprs/slot_ref.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "udf/udf_internal.h"

namespace doris {

ExprContext::ExprContext(Expr* root)
        : _root(root), _is_clone(false), _prepared(false), _opened(false), _closed(false) {}

ExprContext::~ExprContext() {
    DCHECK(!_prepared || _closed);
    for (int i = 0; i < _fn_contexts.size(); ++i) {
        delete _fn_contexts[i];
    }
}

Status ExprContext::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    DCHECK(!_prepared);
    DCHECK(_pool.get() == nullptr);
    _prepared = true;
    _pool.reset(new MemPool());
    return _root->prepare(state, row_desc, this);
}

Status ExprContext::open(RuntimeState* state) {
    DCHECK(_prepared);
    if (_opened) {
        return Status::OK();
    }
    _opened = true;
    // Fragment-local state is only initialized for original contexts. Clones inherit the
    // original's fragment state and only need to have thread-local state initialized.
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    return _root->open(state, this, scope);
}

// TODO chenhao , replace ExprContext with ScalarExprEvaluator
Status ExprContext::open(std::vector<ExprContext*> evals, RuntimeState* state) {
    for (int i = 0; i < evals.size(); ++i) {
        RETURN_IF_ERROR(evals[i]->open(state));
    }
    return Status::OK();
}

void ExprContext::close(RuntimeState* state) {
    DCHECK(!_closed);
    FunctionContext::FunctionStateScope scope =
            _is_clone ? FunctionContext::THREAD_LOCAL : FunctionContext::FRAGMENT_LOCAL;
    _root->close(state, this, scope);

    for (int i = 0; i < _fn_contexts.size(); ++i) {
        _fn_contexts[i]->impl()->close();
    }
    // _pool can be nullptr if Prepare() was never called
    if (_pool != nullptr) {
        _pool->free_all();
    }
    _closed = true;
}

int ExprContext::register_func(RuntimeState* state,
                               const doris_udf::FunctionContext::TypeDesc& return_type,
                               const std::vector<doris_udf::FunctionContext::TypeDesc>& arg_types,
                               int varargs_buffer_size) {
    _fn_contexts.push_back(FunctionContextImpl::create_context(
            state, _pool.get(), return_type, arg_types, varargs_buffer_size, false));
    return _fn_contexts.size() - 1;
}

Status ExprContext::clone(RuntimeState* state, ExprContext** new_ctx) {
    DCHECK(_prepared);
    DCHECK(_opened);
    DCHECK(*new_ctx == nullptr);

    *new_ctx = state->obj_pool()->add(new ExprContext(_root));
    (*new_ctx)->_pool.reset(new MemPool());
    for (int i = 0; i < _fn_contexts.size(); ++i) {
        (*new_ctx)->_fn_contexts.push_back(_fn_contexts[i]->impl()->clone((*new_ctx)->_pool.get()));
    }

    (*new_ctx)->_is_clone = true;
    (*new_ctx)->_prepared = true;
    (*new_ctx)->_opened = true;

    return _root->open(state, *new_ctx, FunctionContext::THREAD_LOCAL);
}

Status ExprContext::clone(RuntimeState* state, ExprContext** new_ctx, Expr* root) {
    DCHECK(_prepared);
    DCHECK(_opened);
    DCHECK(*new_ctx == nullptr);

    *new_ctx = state->obj_pool()->add(new ExprContext(root));
    (*new_ctx)->_pool.reset(new MemPool());
    for (int i = 0; i < _fn_contexts.size(); ++i) {
        (*new_ctx)->_fn_contexts.push_back(_fn_contexts[i]->impl()->clone((*new_ctx)->_pool.get()));
    }

    (*new_ctx)->_is_clone = true;
    (*new_ctx)->_prepared = true;
    (*new_ctx)->_opened = true;

    return root->open(state, *new_ctx, FunctionContext::THREAD_LOCAL);
}

void ExprContext::free_local_allocations() {
    free_local_allocations(_fn_contexts);
}

void ExprContext::free_local_allocations(const std::vector<ExprContext*>& ctxs) {
    for (int i = 0; i < ctxs.size(); ++i) {
        ctxs[i]->free_local_allocations();
    }
}

void ExprContext::free_local_allocations(const std::vector<FunctionContext*>& fn_ctxs) {
    for (int i = 0; i < fn_ctxs.size(); ++i) {
        if (fn_ctxs[i]->impl()->closed()) {
            continue;
        }
        fn_ctxs[i]->impl()->free_local_allocations();
    }
}

bool ExprContext::is_nullable() {
    if (_root->is_slotref()) {
        return SlotRef::is_nullable(_root);
    }
    return false;
}

void* ExprContext::get_value(Expr* e, TupleRow* row, int precision, int scale) {
    switch (e->_type.type) {
    case TYPE_NULL: {
        return nullptr;
    }
    case TYPE_BOOLEAN: {
        doris_udf::BooleanVal v = e->get_boolean_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.bool_val = v.val;
        return &_result.bool_val;
    }
    case TYPE_TINYINT: {
        doris_udf::TinyIntVal v = e->get_tiny_int_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.tinyint_val = v.val;
        return &_result.tinyint_val;
    }
    case TYPE_SMALLINT: {
        doris_udf::SmallIntVal v = e->get_small_int_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.smallint_val = v.val;
        return &_result.smallint_val;
    }
    case TYPE_INT: {
        doris_udf::IntVal v = e->get_int_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.int_val = v.val;
        return &_result.int_val;
    }
    case TYPE_BIGINT: {
        doris_udf::BigIntVal v = e->get_big_int_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.bigint_val = v.val;
        return &_result.bigint_val;
    }
    case TYPE_LARGEINT: {
        doris_udf::LargeIntVal v = e->get_large_int_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.large_int_val = v.val;
        return &_result.large_int_val;
    }
    case TYPE_FLOAT: {
        doris_udf::FloatVal v = e->get_float_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.float_val = v.val;
        return &_result.float_val;
    }
    case TYPE_TIME:
    case TYPE_TIMEV2:
    case TYPE_DOUBLE: {
        doris_udf::DoubleVal v = e->get_double_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.double_val = v.val;
        return &_result.double_val;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_QUANTILE_STATE:
    case TYPE_STRING: {
        doris_udf::StringVal v = e->get_string_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.string_val.ptr = reinterpret_cast<char*>(v.ptr);
        _result.string_val.len = v.len;
        return &_result.string_val;
    }
    case TYPE_DATE:
    case TYPE_DATETIME: {
        doris_udf::DateTimeVal v = e->get_datetime_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.datetime_val = DateTimeValue::from_datetime_val(v);
        return &_result.datetime_val;
    }
    case TYPE_DATEV2: {
        doris_udf::DateV2Val v = e->get_datev2_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.datev2_val =
                doris::vectorized::DateV2Value<doris::vectorized::DateV2ValueType>::from_datev2_val(
                        v);
        return &_result.datev2_val;
    }
    case TYPE_DATETIMEV2: {
        doris_udf::DateTimeV2Val v = e->get_datetimev2_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.datetimev2_val = doris::vectorized::DateV2Value<
                doris::vectorized::DateTimeV2ValueType>::from_datetimev2_val(v);
        return &_result.datetimev2_val;
    }
    case TYPE_DECIMALV2: {
        DecimalV2Val v = e->get_decimalv2_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.decimalv2_val = DecimalV2Value::from_decimal_val(v);
        return &_result.decimalv2_val;
    }
    case TYPE_DECIMAL32: {
        doris_udf::Decimal32Val v = e->get_decimal32_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.int_val = v.val;
        return &_result.int_val;
    }
    case TYPE_DECIMAL64: {
        doris_udf::Decimal64Val v = e->get_decimal64_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.bigint_val = v.val;
        return &_result.bigint_val;
    }
    case TYPE_DECIMAL128I: {
        doris_udf::Decimal128Val v = e->get_decimal128_val(this, row);
        if (v.is_null) {
            return nullptr;
        }
        _result.large_int_val = v.val;
        return &_result.large_int_val;
    }
    case TYPE_ARRAY: {
        doris_udf::CollectionVal v = e->get_array_val(this, row);
        if (v.is_null) {
            return nullptr;
        }

        _result.array_val = CollectionValue::from_collection_val(v);
        return &_result.array_val;
    }
    default:
        DCHECK(false) << "Type not implemented: " << e->_type;
        return nullptr;
    }
}

void ExprContext::print_value(TupleRow* row, std::string* str) {
    RawValue::print_value(get_value(row), _root->type(), _root->_output_scale, str);
}

void ExprContext::print_value(void* value, std::string* str) {
    RawValue::print_value(value, _root->type(), _root->_output_scale, str);
}

void ExprContext::print_value(void* value, std::stringstream* stream) {
    RawValue::print_value(value, _root->type(), _root->_output_scale, stream);
}

void ExprContext::print_value(TupleRow* row, std::stringstream* stream) {
    RawValue::print_value(get_value(row), _root->type(), _root->_output_scale, stream);
}

BooleanVal ExprContext::get_boolean_val(TupleRow* row) {
    return _root->get_boolean_val(this, row);
}

TinyIntVal ExprContext::get_tiny_int_val(TupleRow* row) {
    return _root->get_tiny_int_val(this, row);
}

SmallIntVal ExprContext::get_small_int_val(TupleRow* row) {
    return _root->get_small_int_val(this, row);
}

IntVal ExprContext::get_int_val(TupleRow* row) {
    return _root->get_int_val(this, row);
}

BigIntVal ExprContext::get_big_int_val(TupleRow* row) {
    return _root->get_big_int_val(this, row);
}

FloatVal ExprContext::get_float_val(TupleRow* row) {
    return _root->get_float_val(this, row);
}

DoubleVal ExprContext::get_double_val(TupleRow* row) {
    return _root->get_double_val(this, row);
}

StringVal ExprContext::get_string_val(TupleRow* row) {
    return _root->get_string_val(this, row);
}

// TODO(zc)
// ArrayVal ExprContext::GetArrayVal(TupleRow* row) {
//   return _root->GetArrayVal(this, row);
// }

DateTimeVal ExprContext::get_datetime_val(TupleRow* row) {
    return _root->get_datetime_val(this, row);
}

DateV2Val ExprContext::get_datev2_val(TupleRow* row) {
    return _root->get_datev2_val(this, row);
}

DateTimeV2Val ExprContext::get_datetimev2_val(TupleRow* row) {
    return _root->get_datetimev2_val(this, row);
}

DecimalV2Val ExprContext::get_decimalv2_val(TupleRow* row) {
    return _root->get_decimalv2_val(this, row);
}

Status ExprContext::get_const_value(RuntimeState* state, Expr& expr, AnyVal** const_val) {
    DCHECK(_opened);
    if (!expr.is_constant()) {
        *const_val = nullptr;
        return Status::OK();
    }

    // A constant expression shouldn't have any SlotRefs expr in it.
    DCHECK_EQ(expr.get_slot_ids(nullptr), 0);
    DCHECK(_pool != nullptr);
    const TypeDescriptor& result_type = expr.type();
    ObjectPool* obj_pool = state->obj_pool();
    *const_val = create_any_val(obj_pool, result_type);
    if (*const_val == nullptr) {
        return Status::InternalError("Could not create any val");
    }

    const void* result = ExprContext::get_value(&expr, nullptr);
    AnyValUtil::set_any_val(result, result_type, *const_val);
    if (result_type.is_string_type()) {
        StringVal* sv = reinterpret_cast<StringVal*>(*const_val);
        if (!sv->is_null && sv->len > 0) {
            // Make sure the memory is owned by this evaluator.
            char* ptr_copy = reinterpret_cast<char*>(_pool->try_allocate(sv->len));
            if (ptr_copy == nullptr) {
                RETURN_LIMIT_EXCEEDED(state, "Could not allocate constant string value", sv->len);
            }
            memcpy(ptr_copy, sv->ptr, sv->len);
            sv->ptr = reinterpret_cast<uint8_t*>(ptr_copy);
        }
    }
    return get_error(expr._fn_ctx_idx_start, expr._fn_ctx_idx_end);
}

Status ExprContext::get_error(int start_idx, int end_idx) const {
    DCHECK(_opened);
    end_idx = end_idx == -1 ? _fn_contexts.size() : end_idx;
    DCHECK_GE(start_idx, 0);
    DCHECK_LE(end_idx, _fn_contexts.size());
    for (int idx = start_idx; idx < end_idx; ++idx) {
        DCHECK_LT(idx, _fn_contexts.size());
        FunctionContext* fn_ctx = _fn_contexts[idx];
        if (fn_ctx->has_error()) return Status::InternalError(fn_ctx->error_msg());
    }
    return Status::OK();
}

std::string ExprContext::get_error_msg() const {
    for (auto fn_ctx : _fn_contexts) {
        if (fn_ctx->has_error()) {
            return std::string(fn_ctx->error_msg());
        }
    }
    return "";
}

void ExprContext::clear_error_msg() {
    for (auto fn_ctx : _fn_contexts) {
        fn_ctx->clear_error_msg();
    }
}

} // namespace doris
