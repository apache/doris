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

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Warray-bounds"
#elif defined(__GNUC__) || defined(__GNUG__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
#pragma GCC diagnostic ignored "-Wstringop-overflow="
#endif

#include "exprs/agg_fn_evaluator.h"

#include <sstream>

#include "common/logging.h"
#include "exprs/aggregate_functions.h"
#include "exprs/anyval_util.h"
#include "runtime/datetime_value.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/user_function_cache.h"
#include "thrift/protocol/TDebugProtocol.h"
#include "udf/udf_internal.h"
#include "util/debug_util.h"

namespace doris {
using doris_udf::FunctionContext;
using doris_udf::BooleanVal;
using doris_udf::TinyIntVal;
using doris_udf::SmallIntVal;
using doris_udf::IntVal;
using doris_udf::BigIntVal;
using doris_udf::LargeIntVal;
using doris_udf::FloatVal;
using doris_udf::DoubleVal;
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::StringVal;
using doris_udf::AnyVal;

// typedef for builtin aggregate functions. Unfortunately, these type defs don't
// really work since the actual builtin is implemented not in terms of the base
// AnyVal* type. Due to this, there are lots of casts when we use these typedefs.
// TODO: these typedefs exists as wrappers to go from (TupleRow, Tuple) to the
// types the aggregation functions need. This needs to be done with codegen instead.
typedef void (*InitFn)(FunctionContext*, AnyVal*);
typedef void (*UpdateFn0)(FunctionContext*, AnyVal*);
typedef void (*UpdateFn1)(FunctionContext*, const AnyVal&, AnyVal*);
typedef void (*UpdateFn2)(FunctionContext*, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn3)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn4)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, AnyVal*);
typedef void (*UpdateFn5)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn6)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn7)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, AnyVal*);
typedef void (*UpdateFn8)(FunctionContext*, const AnyVal&, const AnyVal&, const AnyVal&,
                          const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&, const AnyVal&,
                          AnyVal*);
typedef StringVal (*SerializeFn)(FunctionContext*, const StringVal&);
typedef AnyVal (*GetValueFn)(FunctionContext*, const AnyVal&);
typedef AnyVal (*FinalizeFn)(FunctionContext*, const AnyVal&);

Status AggFnEvaluator::create(ObjectPool* pool, const TExpr& desc, AggFnEvaluator** result) {
    return create(pool, desc, false, result);
}

Status AggFnEvaluator::create(ObjectPool* pool, const TExpr& desc, bool is_analytic_fn,
                              AggFnEvaluator** result) {
    *result = pool->add(new AggFnEvaluator(desc.nodes[0], is_analytic_fn));
    int node_idx = 0;
    for (int i = 0; i < desc.nodes[0].num_children; ++i) {
        ++node_idx;
        Expr* expr = nullptr;
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(
                Expr::create_tree_from_thrift(pool, desc.nodes, nullptr, &node_idx, &expr, &ctx));
        (*result)->_input_exprs_ctxs.push_back(ctx);
    }
    return Status::OK();
}

AggFnEvaluator::AggFnEvaluator(const TExprNode& desc, bool is_analytic_fn)
        : _fn(desc.fn),
          _is_merge(desc.agg_expr.is_merge_agg),
          _is_analytic_fn(is_analytic_fn),
          _return_type(TypeDescriptor::from_thrift(desc.fn.ret_type)),
          _intermediate_type(TypeDescriptor::from_thrift(desc.fn.aggregate_fn.intermediate_type)),
          _function_type(desc.fn.binary_type),
          _total_mem_consumption(0),
          _accumulated_mem_consumption(0),
          _intermediate_slot_desc(nullptr),
          _output_slot_desc(nullptr),
          _init_fn(nullptr),
          _update_fn(nullptr),
          _remove_fn(nullptr),
          _merge_fn(nullptr),
          _serialize_fn(nullptr),
          _get_value_fn(nullptr),
          _finalize_fn(nullptr) {
    if (_fn.name.function_name == "count") {
        _agg_op = COUNT;
    } else if (_fn.name.function_name == "min") {
        _agg_op = MIN;
    } else if (_fn.name.function_name == "max") {
        _agg_op = MAX;
    } else if (_fn.name.function_name == "sum") {
        _agg_op = SUM;
    } else if (_fn.name.function_name == "avg") {
        _agg_op = AVG;
    } else if (_fn.name.function_name == "ndv" || _fn.name.function_name == "ndv_no_finalize") {
        _agg_op = NDV;
    } else if (_fn.name.function_name == "count_distinct" ||
               _fn.name.function_name == "count_distinct") {
        _agg_op = COUNT_DISTINCT;
    } else if (_fn.name.function_name == "sum_distinct" ||
               _fn.name.function_name == "sum_distinct") {
        _agg_op = SUM_DISTINCT;
    } else if (_fn.name.function_name == "hll_union_agg") {
        _agg_op = HLL_UNION_AGG;
    } else {
        _agg_op = OTHER;
    }
}

Status AggFnEvaluator::prepare(RuntimeState* state, const RowDescriptor& desc, MemPool* pool,
                               const SlotDescriptor* intermediate_slot_desc,
                               const SlotDescriptor* output_slot_desc,
                               const std::shared_ptr<MemTracker>& mem_tracker,
                               FunctionContext** agg_fn_ctx) {
    DCHECK(pool != nullptr);
    DCHECK(intermediate_slot_desc != nullptr);
    DCHECK(_intermediate_slot_desc == nullptr);
    _output_slot_desc = output_slot_desc;
    //DCHECK(_intermediate_slot_desc == nullptr);
    _intermediate_slot_desc = intermediate_slot_desc;

    _string_buffer_len = 0;
    _mem_tracker = mem_tracker;

    Status status = Expr::prepare(_input_exprs_ctxs, state, desc, _mem_tracker);
    RETURN_IF_ERROR(status);

    ObjectPool* obj_pool = state->obj_pool();

    for (int i = 0; i < _input_exprs_ctxs.size(); ++i) {
        _staging_input_vals.push_back(
                create_any_val(obj_pool, input_expr_ctxs()[i]->root()->type()));
    }

    // window has intermediate_slot_type
    if (_intermediate_slot_desc != nullptr) {
        _staging_intermediate_val = create_any_val(obj_pool, _intermediate_slot_desc->type());
        _staging_merge_input_val = create_any_val(obj_pool, _intermediate_slot_desc->type());
    }

    //_staging_output_val = create_any_val(obj_pool, _output_slot_desc->type());
    _is_multi_distinct = false;

    if (_agg_op == AggregationOp::COUNT_DISTINCT) {
        _hybrid_map.reset(new HybridMap(TYPE_VARCHAR));
        _is_multi_distinct = true;
        _string_buffer.reset(new char[1024]);
        _string_buffer_len = 1024;
    } else if (_agg_op == AggregationOp::SUM_DISTINCT) {
        _hybrid_map.reset(new HybridMap(input_expr_ctxs()[0]->root()->type().type));
        _is_multi_distinct = true;
    }
    // TODO: this should be made identical for the builtin and UDA case by
    // putting all this logic in an improved opcode registry.

    // Load the function pointers. Merge is not required if this is evaluating an
    // analytic function.
    if (_fn.aggregate_fn.init_fn_symbol.empty() || _fn.aggregate_fn.update_fn_symbol.empty() ||
        (!_is_analytic_fn && _fn.aggregate_fn.merge_fn_symbol.empty())) {
        // This path is only for partially implemented builtins.
        DCHECK_EQ(_fn.binary_type, TFunctionBinaryType::BUILTIN);
        std::stringstream ss;
        ss << "Function " << _fn.name.function_name << " is not implemented.";
        return Status::InternalError(ss.str());
    }

    // Load the function pointers.
    RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
            _fn.id, _fn.aggregate_fn.init_fn_symbol, _fn.hdfs_location, _fn.checksum, &_init_fn,
            nullptr));

    RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
            _fn.id, _fn.aggregate_fn.update_fn_symbol, _fn.hdfs_location, _fn.checksum, &_update_fn,
            nullptr));

    // Merge() is not loaded if evaluating the agg fn as an analytic function.
    if (!_is_analytic_fn) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.aggregate_fn.merge_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &_merge_fn, nullptr));
    }

    // Serialize and Finalize are optional
    if (!_fn.aggregate_fn.serialize_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.aggregate_fn.serialize_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &_serialize_fn, nullptr));
    }
    if (!_fn.aggregate_fn.finalize_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.aggregate_fn.finalize_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &_finalize_fn, nullptr));
    }

    if (!_fn.aggregate_fn.get_value_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.aggregate_fn.get_value_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &_get_value_fn, nullptr));
    }
    if (!_fn.aggregate_fn.remove_fn_symbol.empty()) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_function_ptr(
                _fn.id, _fn.aggregate_fn.remove_fn_symbol, _fn.hdfs_location, _fn.checksum,
                &_remove_fn, nullptr));
    }

    std::vector<FunctionContext::TypeDesc> arg_types;
    for (int j = 0; j < _input_exprs_ctxs.size(); ++j) {
        arg_types.push_back(
                AnyValUtil::column_type_to_type_desc(_input_exprs_ctxs[j]->root()->type()));
    }

    FunctionContext::TypeDesc intermediate_type =
            AnyValUtil::column_type_to_type_desc(_intermediate_type);
    FunctionContext::TypeDesc output_type =
            AnyValUtil::column_type_to_type_desc(_output_slot_desc->type());

    *agg_fn_ctx = FunctionContextImpl::create_context(state, pool, intermediate_type, output_type,
                                                      arg_types, 0, false);
    return Status::OK();
}

Status AggFnEvaluator::open(RuntimeState* state, FunctionContext* agg_fn_ctx) {
    RETURN_IF_ERROR(Expr::open(_input_exprs_ctxs, state));
    // Now that we have opened all our input exprs, it is safe to evaluate any constant
    // values for the UDA's FunctionContext (we cannot evaluate exprs before calling Open()
    // on them).
    std::vector<AnyVal*> constant_args(_input_exprs_ctxs.size());
    for (int i = 0; i < _input_exprs_ctxs.size(); ++i) {
        constant_args[i] = _input_exprs_ctxs[i]->root()->get_const_val(_input_exprs_ctxs[i]);
    }
    agg_fn_ctx->impl()->set_constant_args(constant_args);
    return Status::OK();
}

void AggFnEvaluator::close(RuntimeState* state) {
    Expr::close(_input_exprs_ctxs, state);
    if (UNLIKELY(_total_mem_consumption > 0)) {
        _mem_tracker->Release(_total_mem_consumption);
    }
}

// Utility to put val into an AnyVal struct
inline void AggFnEvaluator::set_any_val(const void* slot, const TypeDescriptor& type, AnyVal* dst) {
    if (slot == nullptr) {
        dst->is_null = true;
        return;
    }

    dst->is_null = false;

    switch (type.type) {
    case TYPE_NULL:
        return;

    case TYPE_BOOLEAN:
        reinterpret_cast<BooleanVal*>(dst)->val = *reinterpret_cast<const bool*>(slot);
        return;

    case TYPE_TINYINT:
        reinterpret_cast<TinyIntVal*>(dst)->val = *reinterpret_cast<const int8_t*>(slot);
        return;

    case TYPE_SMALLINT:
        reinterpret_cast<SmallIntVal*>(dst)->val = *reinterpret_cast<const int16_t*>(slot);
        return;

    case TYPE_INT:
        reinterpret_cast<IntVal*>(dst)->val = *reinterpret_cast<const int32_t*>(slot);
        return;

    case TYPE_BIGINT:
        reinterpret_cast<BigIntVal*>(dst)->val = *reinterpret_cast<const int64_t*>(slot);
        return;

    case TYPE_FLOAT:
        reinterpret_cast<FloatVal*>(dst)->val = *reinterpret_cast<const float*>(slot);
        return;

    case TYPE_DOUBLE:
        reinterpret_cast<DoubleVal*>(dst)->val = *reinterpret_cast<const double*>(slot);
        return;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_STRING:
        reinterpret_cast<const StringValue*>(slot)->to_string_val(
                reinterpret_cast<StringVal*>(dst));
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        reinterpret_cast<const DateTimeValue*>(slot)->to_datetime_val(
                reinterpret_cast<DateTimeVal*>(dst));
        return;

    case TYPE_DECIMALV2:
        reinterpret_cast<DecimalV2Val*>(dst)->val =
                reinterpret_cast<const PackedInt128*>(slot)->value;
        return;

    case TYPE_LARGEINT:
        memcpy(&reinterpret_cast<LargeIntVal*>(dst)->val, slot, sizeof(__int128));
        return;

    default:
        DCHECK(false) << "NYI";
    }
}

inline void AggFnEvaluator::set_output_slot(const AnyVal* src, const SlotDescriptor* dst_slot_desc,
                                            Tuple* dst) {
    if (src->is_null && dst_slot_desc->is_nullable()) {
        dst->set_null(dst_slot_desc->null_indicator_offset());
        return;
    }

    dst->set_not_null(dst_slot_desc->null_indicator_offset());
    void* slot = dst->get_slot(dst_slot_desc->tuple_offset());

    switch (dst_slot_desc->type().type) {
    case TYPE_NULL:
        return;

    case TYPE_BOOLEAN:
        *reinterpret_cast<bool*>(slot) = reinterpret_cast<const BooleanVal*>(src)->val;
        return;

    case TYPE_TINYINT:
        *reinterpret_cast<int8_t*>(slot) = reinterpret_cast<const TinyIntVal*>(src)->val;
        return;

    case TYPE_SMALLINT:
        *reinterpret_cast<int16_t*>(slot) = reinterpret_cast<const SmallIntVal*>(src)->val;
        return;

    case TYPE_INT:
        *reinterpret_cast<int32_t*>(slot) = reinterpret_cast<const IntVal*>(src)->val;
        return;

    case TYPE_BIGINT:
        *reinterpret_cast<int64_t*>(slot) = reinterpret_cast<const BigIntVal*>(src)->val;
        return;

    case TYPE_FLOAT:
        *reinterpret_cast<float*>(slot) = reinterpret_cast<const FloatVal*>(src)->val;
        return;

    case TYPE_DOUBLE:
        *reinterpret_cast<double*>(slot) = reinterpret_cast<const DoubleVal*>(src)->val;
        return;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_STRING:
        *reinterpret_cast<StringValue*>(slot) =
                StringValue::from_string_val(*reinterpret_cast<const StringVal*>(src));
        return;

    case TYPE_DATE:
    case TYPE_DATETIME:
        *reinterpret_cast<DateTimeValue*>(slot) =
                DateTimeValue::from_datetime_val(*reinterpret_cast<const DateTimeVal*>(src));
        return;

    case TYPE_DECIMALV2:
        *reinterpret_cast<PackedInt128*>(slot) = reinterpret_cast<const DecimalV2Val*>(src)->val;
        return;

    case TYPE_LARGEINT: {
        memcpy(slot, &reinterpret_cast<const LargeIntVal*>(src)->val, sizeof(__int128));
        return;
    }

    default:
        DCHECK(false) << "NYI";
    }
}

bool AggFnEvaluator::is_in_hybridmap(void* input_val, Tuple* dst, bool* is_add_buckets) {
    bool is_in_hashset = false;
    HybridSetBase* _set_ptr = nullptr;
    _set_ptr = _hybrid_map->find_or_insert_set(reinterpret_cast<uint64_t>(dst), is_add_buckets);
    is_in_hashset = _set_ptr->find(input_val);

    if (!is_in_hashset) {
        _set_ptr->insert(input_val);
    }

    return is_in_hashset;
}

// This function would be replaced in codegen.
void AggFnEvaluator::init(FunctionContext* agg_fn_ctx, Tuple* dst) {
    DCHECK(_init_fn != nullptr);
    reinterpret_cast<InitFn>(_init_fn)(agg_fn_ctx, _staging_intermediate_val);
    set_output_slot(_staging_intermediate_val, _intermediate_slot_desc, dst);
    agg_fn_ctx->impl()->set_num_updates(0);
    agg_fn_ctx->impl()->set_num_removes(0);
}

void AggFnEvaluator::update_mem_limlits(int len) {
    _accumulated_mem_consumption += len;
    // per 16M , update mem_tracker one time
    if (UNLIKELY(_accumulated_mem_consumption > 16777216)) {
        _mem_tracker->Consume(_accumulated_mem_consumption);
        _total_mem_consumption += _accumulated_mem_consumption;
        _accumulated_mem_consumption = 0;
    }
}

AggFnEvaluator::~AggFnEvaluator() {}

inline void AggFnEvaluator::update_mem_trackers(bool is_filter, bool is_add_buckets, int len) {
    if (!is_filter) {
        int total_len = len;

        if (is_add_buckets) {
            total_len += BIGINT_SIZE; //map's key size
        }

        update_mem_limlits(total_len);
    }
}

bool AggFnEvaluator::count_distinct_data_filter(TupleRow* row, Tuple* dst) {
    std::vector<int32_t> vec_string_len;
    int total_len = 0;

    // 1. calculate the total_len of all input parameters
    for (int i = 0; i < input_expr_ctxs().size(); ++i) {
        void* src_slot = input_expr_ctxs()[i]->get_value(row);
        set_any_val(src_slot, input_expr_ctxs()[i]->root()->type(), _staging_input_vals[i]);

        if (_staging_input_vals[i]->is_null) {
            // even though only one parameter is null, the row will be abandon
            return true;
        }

        if (input_expr_ctxs()[i]->root()->type().is_string_type()) {
            const int string_len = reinterpret_cast<const StringVal*>(_staging_input_vals[i])->len;
            vec_string_len.push_back(string_len);
            total_len += string_len;
        }

        total_len += get_real_byte_size(input_expr_ctxs()[i]->root()->type().type);
    }

    int32_t vec_size = vec_string_len.size();
    int32_t int_size = INT_SIZE;
    total_len += vec_size * int_size;

    // 2. merge multi parameter into one parameter(StringVal)
    if (_string_buffer_len < total_len) {
        _string_buffer_len = ((total_len << 10) + 1) >> 10; // (len/1024+1)*1024
        _string_buffer.reset(new char[_string_buffer_len]);
    }

    StringValue string_val(_string_buffer.get(), total_len);
    // the content of StringVal:
    //    header: the STRING_VALUE's len
    //    body:   all input parameters' content
    char* begin = string_val.ptr;

    for (int i = 0; i < vec_size; i++) {
        memcpy(begin, &vec_string_len[0], int_size);
        begin += int_size;
    }

    for (int i = 0; i < input_expr_ctxs().size(); ++i) {
        switch (input_expr_ctxs()[i]->root()->type().type) {
        case TYPE_NULL:
            return true;

        case TYPE_BOOLEAN: {
            *begin = (uint8_t) reinterpret_cast<BooleanVal*>(_staging_input_vals[i])->val;
            begin += TINYINT_SIZE;
            break;
        }

        case TYPE_TINYINT: {
            memcpy(begin, &reinterpret_cast<TinyIntVal*>(_staging_input_vals[i])->val,
                   TINYINT_SIZE);
            begin += TINYINT_SIZE;
            break;
        }

        case TYPE_SMALLINT: {
            memcpy(begin, &reinterpret_cast<SmallIntVal*>(_staging_input_vals[i])->val,
                   SMALLINT_SIZE);
            begin += SMALLINT_SIZE;
            break;
        }

        case TYPE_INT: {
            memcpy(begin, &reinterpret_cast<IntVal*>(_staging_input_vals[i])->val, INT_SIZE);
            begin += INT_SIZE;
            break;
        }

        case TYPE_BIGINT: {
            memcpy(begin, &reinterpret_cast<BigIntVal*>(_staging_input_vals[i])->val, BIGINT_SIZE);
            begin += BIGINT_SIZE;
            break;
        }

        case TYPE_LARGEINT: {
            LargeIntVal* value = reinterpret_cast<LargeIntVal*>(_staging_input_vals[i]);
            memcpy(begin, &value->val, LARGEINT_SIZE);
            begin += LARGEINT_SIZE;
            break;
        }

        case TYPE_FLOAT: {
            memcpy(begin, &reinterpret_cast<FloatVal*>(_staging_input_vals[i])->val, FLOAT_SIZE);
            begin += FLOAT_SIZE;
            break;
        }

        case TYPE_DOUBLE: {
            memcpy(begin, &reinterpret_cast<DoubleVal*>(_staging_input_vals[i])->val, DOUBLE_SIZE);
            begin += DOUBLE_SIZE;
            break;
        }

        case TYPE_DECIMALV2: {
            DecimalV2Val* value = reinterpret_cast<DecimalV2Val*>(_staging_input_vals[i]);
            memcpy(begin, value, sizeof(DecimalV2Val));
            begin += sizeof(DecimalV2Val);
            break;
        }

        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_HLL:
        case TYPE_OBJECT:
        case TYPE_STRING: {
            StringVal* value = reinterpret_cast<StringVal*>(_staging_input_vals[i]);
            memcpy(begin, value->ptr, value->len);
            begin += value->len;
            break;
        }

        case TYPE_DATE:
        case TYPE_DATETIME: {
            DateTimeVal* value = reinterpret_cast<DateTimeVal*>(_staging_input_vals[i]);
            memcpy(begin, &value->packed_time, DATETIME_SIZE);
            begin += DATETIME_SIZE;
            break;
        }

        default: {
            DCHECK(0) << "FYI" << input_expr_ctxs()[i]->root()->type();
        }
        }
    }

    DCHECK(begin == string_val.ptr + string_val.len)
            << "COUNT_DISTINCT: StringVal's len doesn't match";
    bool is_add_buckets = false;
    bool is_filter = is_in_hybridmap(&string_val, dst, &is_add_buckets);
    update_mem_trackers(is_filter, is_add_buckets, string_val.len);
    return is_filter;
}

bool AggFnEvaluator::sum_distinct_data_filter(TupleRow* row, Tuple* dst) {
    DCHECK(input_expr_ctxs().size() == 1);
    void* src_slot = input_expr_ctxs()[0]->get_value(row);
    set_any_val(src_slot, input_expr_ctxs()[0]->root()->type(), _staging_input_vals[0]);

    if (_staging_input_vals[0]->is_null) {
        // if the parameter is null, the row will be abandon
        return true;
    }

    bool is_filter = false;
    bool is_add_buckets = false;

    switch (input_expr_ctxs()[0]->root()->type().type) {
    case TYPE_NULL: {
        return true;
    }

    case TYPE_BIGINT: {
        const BigIntVal* value = reinterpret_cast<BigIntVal*>(_staging_input_vals[0]);
        is_filter = is_in_hybridmap((void*)&(value->val), dst, &is_add_buckets);
        update_mem_trackers(is_filter, is_add_buckets, BIGINT_SIZE);
        return is_filter;
    }

    case TYPE_FLOAT: {
        const FloatVal* value = reinterpret_cast<FloatVal*>(_staging_input_vals[0]);
        is_filter = is_in_hybridmap((void*)&(value->val), dst, &is_add_buckets);
        update_mem_trackers(is_filter, is_add_buckets, FLOAT_SIZE);
        return is_filter;
    }

    case TYPE_DOUBLE: {
        const DoubleVal* value = reinterpret_cast<DoubleVal*>(_staging_input_vals[0]);
        is_filter = is_in_hybridmap((void*)&(value->val), dst, &is_add_buckets);
        update_mem_trackers(is_filter, is_add_buckets, DOUBLE_SIZE);
        return is_filter;
    }

    case TYPE_DECIMALV2: {
        const DecimalV2Val* value = reinterpret_cast<DecimalV2Val*>(_staging_input_vals[0]);
        DecimalV2Value temp_value = DecimalV2Value::from_decimal_val(*value);
        is_filter = is_in_hybridmap((void*)&(temp_value), dst, &is_add_buckets);
        update_mem_trackers(is_filter, is_add_buckets, DECIMALV2_SIZE);
        return is_filter;
    }

    case TYPE_LARGEINT: {
        const LargeIntVal* value = reinterpret_cast<LargeIntVal*>(_staging_input_vals[0]);
        is_filter = is_in_hybridmap((void*)&(value->val), dst, &is_add_buckets);
        update_mem_trackers(is_filter, is_add_buckets, LARGEINT_SIZE);
        return is_filter;
    }

    default: {
        DCHECK(0) << "FYI";
    }
    }

    return false;
}

void AggFnEvaluator::update_or_merge(FunctionContext* agg_fn_ctx, TupleRow* row, Tuple* dst,
                                     void* fn) {
    if (fn == nullptr) {
        return;
    }

    bool dst_null = dst->is_null(_intermediate_slot_desc->null_indicator_offset());
    void* dst_slot = nullptr;

    if (!dst_null) {
        dst_slot = dst->get_slot(_intermediate_slot_desc->tuple_offset());
    }

    set_any_val(dst_slot, _intermediate_slot_desc->type(), _staging_intermediate_val);

    if (_is_multi_distinct) {
        if (_agg_op == COUNT_DISTINCT) {
            bool is_need_filter = count_distinct_data_filter(row, dst);

            if (is_need_filter) {
                _staging_input_vals[0]->is_null = true;
            }
        } else if (_agg_op == SUM_DISTINCT) {
            bool is_need_filter = sum_distinct_data_filter(row, dst);

            if (is_need_filter) {
                _staging_input_vals[0]->is_null = true;
            }
        } else {
            DCHECK(0) << "we only support count_distinct and sum_distinct";
        }
    } else {
        for (int i = 0; i < input_expr_ctxs().size(); ++i) {
            void* src_slot = input_expr_ctxs()[i]->get_value(row);
            set_any_val(src_slot, input_expr_ctxs()[i]->root()->type(), _staging_input_vals[i]);
        }
    }

    // TODO: this part is not so good and not scalable. It can be replaced with
    // codegen but we can also consider leaving it for the first few cases for
    // debugging.

    // if _agg_op is TAggregationOp::COUNT_DISTINCT, it has only one
    // input parameter, we consider the first parameter as the only input parameter
    if (_is_multi_distinct && _agg_op == AggregationOp::COUNT_DISTINCT) {
        reinterpret_cast<UpdateFn1>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                        _staging_intermediate_val);
    } else {
        switch (input_expr_ctxs().size()) {
        case 0:
            reinterpret_cast<UpdateFn0>(fn)(agg_fn_ctx, _staging_intermediate_val);
            break;

        case 1:
            reinterpret_cast<UpdateFn1>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            _staging_intermediate_val);
            break;

        case 2:
            reinterpret_cast<UpdateFn2>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            *_staging_input_vals[1], _staging_intermediate_val);
            break;

        case 3:
            reinterpret_cast<UpdateFn3>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            *_staging_input_vals[1], *_staging_input_vals[2],
                                            _staging_intermediate_val);
            break;

        case 4:
            reinterpret_cast<UpdateFn4>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            *_staging_input_vals[1], *_staging_input_vals[2],
                                            *_staging_input_vals[3], _staging_intermediate_val);
            break;

        case 5:
            reinterpret_cast<UpdateFn5>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            *_staging_input_vals[1], *_staging_input_vals[2],
                                            *_staging_input_vals[3], *_staging_input_vals[4],
                                            _staging_intermediate_val);
            break;

        case 6:
            reinterpret_cast<UpdateFn6>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            *_staging_input_vals[1], *_staging_input_vals[2],
                                            *_staging_input_vals[3], *_staging_input_vals[4],
                                            *_staging_input_vals[5], _staging_intermediate_val);
            break;

        case 7:
            reinterpret_cast<UpdateFn7>(fn)(
                    agg_fn_ctx, *_staging_input_vals[0], *_staging_input_vals[1],
                    *_staging_input_vals[2], *_staging_input_vals[3], *_staging_input_vals[4],
                    *_staging_input_vals[5], *_staging_input_vals[6], _staging_intermediate_val);
            break;

        case 8:
            reinterpret_cast<UpdateFn8>(fn)(agg_fn_ctx, *_staging_input_vals[0],
                                            *_staging_input_vals[1], *_staging_input_vals[2],
                                            *_staging_input_vals[3], *_staging_input_vals[4],
                                            *_staging_input_vals[5], *_staging_input_vals[6],
                                            *_staging_input_vals[7], _staging_intermediate_val);
            break;

        default:
            DCHECK(false) << "NYI";
        }
    }

    set_output_slot(_staging_intermediate_val, _intermediate_slot_desc, dst);
}

void AggFnEvaluator::update(FunctionContext* agg_fn_ctx, TupleRow* row, Tuple* dst, void* fn,
                            MemPool* pool) {
    return update_or_merge(agg_fn_ctx, row, dst, fn);
}

void AggFnEvaluator::merge(FunctionContext* agg_fn_ctx, TupleRow* row, Tuple* dst, MemPool* pool) {
    return update_or_merge(agg_fn_ctx, row, dst, _merge_fn);
}

static void set_any_val2(const SlotDescriptor* desc, Tuple* tuple, AnyVal* dst) {
    bool is_null = tuple->is_null(desc->null_indicator_offset());
    void* slot = nullptr;
    if (!is_null) {
        slot = tuple->get_slot(desc->tuple_offset());
    }
    AnyValUtil::set_any_val(slot, desc->type(), dst);
}

void AggFnEvaluator::merge(FunctionContext* agg_fn_ctx, Tuple* src, Tuple* dst) {
    DCHECK(_merge_fn != nullptr);

    set_any_val2(_intermediate_slot_desc, dst, _staging_intermediate_val);
    set_any_val2(_intermediate_slot_desc, src, _staging_merge_input_val);

    // The merge fn always takes one input argument.
    reinterpret_cast<UpdateFn1>(_merge_fn)(agg_fn_ctx, *_staging_merge_input_val,
                                           _staging_intermediate_val);

    set_output_slot(_staging_intermediate_val, _intermediate_slot_desc, dst);
}

void AggFnEvaluator::choose_update_or_merge(FunctionContext* agg_fn_ctx, TupleRow* row,
                                            Tuple* dst) {
    if (_is_merge) {
        return update_or_merge(agg_fn_ctx, row, dst, _merge_fn);
    } else {
        return update_or_merge(agg_fn_ctx, row, dst, _update_fn);
    }
}

void AggFnEvaluator::serialize_or_finalize(FunctionContext* agg_fn_ctx, Tuple* src,
                                           const SlotDescriptor* dst_slot_desc, Tuple* dst,
                                           void* fn, bool add_null) {
    // DCHECK_EQ(dst_slot_desc->type().type, _return_type.type);
    if (src == nullptr) {
        src = dst;
    }
    if (fn == nullptr && src == dst) {
        return;
    }

    // same
    bool src_slot_null = add_null || src->is_null(_intermediate_slot_desc->null_indicator_offset());
    void* src_slot = nullptr;

    if (!src_slot_null) {
        src_slot = src->get_slot(_intermediate_slot_desc->tuple_offset());
    }

    // not same
    // if (_is_analytic_fn) {
    // No fn was given but the src and dst tuples are different (doing a finalize()).
    // Just copy the src slot into the dst tuple.
    if (fn == nullptr) {
        DCHECK_EQ(_intermediate_slot_desc->type(), dst_slot_desc->type());
        RawValue::write(src_slot, dst, dst_slot_desc, nullptr);
        return;
    }
    // }
    set_any_val(src_slot, _intermediate_slot_desc->type(), _staging_intermediate_val);

    switch (dst_slot_desc->type().type) {
    case TYPE_BOOLEAN: {
        typedef BooleanVal (*Fn)(FunctionContext*, AnyVal*);
        BooleanVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_TINYINT: {
        typedef TinyIntVal (*Fn)(FunctionContext*, AnyVal*);
        TinyIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_SMALLINT: {
        typedef SmallIntVal (*Fn)(FunctionContext*, AnyVal*);
        SmallIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_INT: {
        typedef IntVal (*Fn)(FunctionContext*, AnyVal*);
        IntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_BIGINT: {
        typedef BigIntVal (*Fn)(FunctionContext*, AnyVal*);
        BigIntVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_FLOAT: {
        typedef FloatVal (*Fn)(FunctionContext*, AnyVal*);
        FloatVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_DOUBLE: {
        typedef DoubleVal (*Fn)(FunctionContext*, AnyVal*);
        DoubleVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_STRING: {
        typedef StringVal (*Fn)(FunctionContext*, AnyVal*);
        StringVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_DATE:
    case TYPE_DATETIME: {
        typedef DateTimeVal (*Fn)(FunctionContext*, AnyVal*);
        DateTimeVal v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    case TYPE_DECIMALV2: {
        typedef DecimalV2Val (*Fn)(FunctionContext*, AnyVal*);
        DecimalV2Val v = reinterpret_cast<Fn>(fn)(agg_fn_ctx, _staging_intermediate_val);
        set_output_slot(&v, dst_slot_desc, dst);
        break;
    }

    default:
        DCHECK(false) << "NYI";
    }
}

void AggFnEvaluator::serialize(FunctionContext* agg_fn_ctx, Tuple* tuple) {
    serialize_or_finalize(agg_fn_ctx, nullptr, _intermediate_slot_desc, tuple, _serialize_fn);
}

//void AggFnEvaluator::finalize(FunctionContext* agg_fn_ctx, Tuple* tuple) {
//    serialize_or_finalize(agg_fn_ctx, nullptr, _output_slot_desc, tuple, _finalize_fn);
//}

std::string AggFnEvaluator::debug_string(const std::vector<AggFnEvaluator*>& exprs) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < exprs.size(); ++i) {
        out << (i == 0 ? "" : " ") << exprs[i]->debug_string();
    }

    out << "]";
    return out.str();
}

std::string AggFnEvaluator::debug_string() const {
    std::stringstream out;
    out << "AggFnEvaluator(op=" << _agg_op;

    out << ")";
    return out.str();
}

} // namespace doris

#if defined(__clang__)
#pragma clang diagnostic pop
#elif defined(__GNUC__) || defined(__GNUG__)
#pragma GCC diagnostic pop
#endif
