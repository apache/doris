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

// include aggregate_functions.h first to make sure that all need includes is written in header files
#include "exprs/aggregate_functions.h"

#include <math.h>
#include <sstream>
#include <unordered_set>

#include "common/logging.h"
#include "runtime/string_value.h"
#include "runtime/datetime_value.h"
#include "exprs/anyval_util.h"
#include "exprs/hybird_set.h"
#include "util/tdigest.h"
#include "util/debug_util.h"

// TODO: this file should be cross compiled and then all of the builtin
// aggregate functions will have a codegen enabled path. Then we can remove
// the custom code in aggregation node.
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
using doris_udf::DecimalVal;
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::StringVal;
using doris_udf::AnyVal;

// Delimiter to use if the separator is NULL.
static const StringVal DEFAULT_STRING_CONCAT_DELIM((uint8_t*)", ", 2);

void AggregateFunctions::init_null(FunctionContext*, AnyVal* dst) {
    dst->is_null = true;
}

template<typename T>
void AggregateFunctions::init_zero(FunctionContext*, T* dst) {
    dst->is_null = false;
    dst->val = 0;
}

template<>
void AggregateFunctions::init_zero(FunctionContext*, DecimalVal* dst) {
    dst->set_to_zero();
}

template<>
void AggregateFunctions::init_zero(FunctionContext*, DecimalV2Val* dst) {
    dst->set_to_zero();
}

template<typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::sum_remove(FunctionContext* ctx, const SRC_VAL& src,
    DST_VAL* dst) {
    // Do not count null values towards the number of removes
    if (src.is_null) {
        ctx->impl()->increment_num_removes(-1);
    }
    if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
        *dst = DST_VAL::null();
        return;
    }
    if (src.is_null) {
        return;
    }
    if (dst->is_null) {
        init_zero<DST_VAL>(ctx, dst);
    }
    dst->val -= src.val;
}

template<>
void AggregateFunctions::sum_remove(FunctionContext* ctx, const DecimalVal& src,
    DecimalVal* dst) {
    if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
        *dst = DecimalVal::null();
        return;
    }
    if (src.is_null) {
        return;
    }
    if (dst->is_null) {
        init_zero<DecimalVal>(ctx, dst);
    }

    DecimalValue new_src = DecimalValue::from_decimal_val(src);
    DecimalValue new_dst = DecimalValue::from_decimal_val(*dst);
    new_dst = new_dst - new_src;
    new_dst.to_decimal_val(dst);
}

template<>
void AggregateFunctions::sum_remove(FunctionContext* ctx, const DecimalV2Val& src,
    DecimalV2Val* dst) {
    if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
        *dst = DecimalV2Val::null();
        return;
    }
    if (src.is_null) {
        return;
    }
    if (dst->is_null) {
        init_zero<DecimalV2Val>(ctx, dst);
    }

    DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value new_dst = DecimalV2Value::from_decimal_val(*dst);
    new_dst = new_dst - new_src;
    new_dst.to_decimal_val(dst);
}


StringVal AggregateFunctions::string_val_get_value(
        FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return src;
    }
    StringVal result(ctx, src.len);
    memcpy(result.ptr, src.ptr, src.len);
    return result;
}

StringVal AggregateFunctions::string_val_serialize_or_finalize(
        FunctionContext* ctx, const StringVal& src) {
    StringVal result = string_val_get_value(ctx, src);
    if (!src.is_null) {
        ctx->free(src.ptr);
    }
    return result;
}

void AggregateFunctions::count_update(
    FunctionContext*, const AnyVal& src, BigIntVal* dst) {
    DCHECK(!dst->is_null);

    if (!src.is_null) {
        ++dst->val;
    }
}

void AggregateFunctions::count_merge(FunctionContext*, const BigIntVal& src,
        BigIntVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    dst->val += src.val;
}

void AggregateFunctions::count_remove(
    FunctionContext*, const AnyVal& src, BigIntVal* dst) {
    DCHECK(!dst->is_null);
    if (!src.is_null) {
        --dst->val;
        DCHECK_GE(dst->val, 0);
    }
}

struct AvgState {
    double sum;
    int64_t count;
};

struct DecimalAvgState {
    DecimalVal sum;
    int64_t count;
};

struct DecimalV2AvgState {
    DecimalV2Val sum;
    int64_t count;
};

void AggregateFunctions::avg_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(AvgState);
    dst->ptr = ctx->allocate(dst->len);
    memset(dst->ptr, 0, sizeof(AvgState));
}

void AggregateFunctions::decimal_avg_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(DecimalAvgState);
    dst->ptr = ctx->allocate(dst->len);
    // memset(dst->ptr, 0, sizeof(DecimalAvgState));
    DecimalAvgState* avg = reinterpret_cast<DecimalAvgState*>(dst->ptr);
    avg->count = 0;
    avg->sum.set_to_zero();
}

void AggregateFunctions::decimalv2_avg_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(DecimalV2AvgState);
    dst->ptr = ctx->allocate(dst->len);
    // memset(dst->ptr, 0, sizeof(DecimalAvgState));
    DecimalV2AvgState* avg = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);
    avg->count = 0;
    avg->sum.set_to_zero();
}


template <typename T>
void AggregateFunctions::avg_update(FunctionContext* ctx, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    avg->sum += src.val;
    ++avg->count;
}

struct PercentileApproxState {
public:
    PercentileApproxState() : digest(new TDigest()){
    }
    ~PercentileApproxState() {
        delete digest;
    }
    
    TDigest *digest = nullptr;
    double targetQuantile = -1.0;
};

void AggregateFunctions::percentile_approx_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(PercentileApproxState);
    dst->ptr = (uint8_t*) new PercentileApproxState();
};

template<typename T>
void AggregateFunctions::percentile_approx_update(FunctionContext* ctx, const T& src, const DoubleVal& quantile, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(PercentileApproxState), dst->len);

    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState*>(dst->ptr);
    percentile->digest->add(src.val);
    percentile->targetQuantile = quantile.val;
}

StringVal AggregateFunctions::percentile_approx_serialize(FunctionContext* ctx, const StringVal& src) {
    DCHECK(!src.is_null);

    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState*>(src.ptr);
    uint32_t serialized_size = percentile->digest->serialized_size();
    StringVal result(ctx, sizeof(double) + serialized_size);
    memcpy(result.ptr, &percentile->targetQuantile, sizeof(double));
    percentile->digest->serialize(result.ptr + sizeof(double));

    delete percentile;
    return result;
}

void AggregateFunctions::percentile_approx_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(PercentileApproxState), dst->len);

    double quantile;
    memcpy(&quantile, src.ptr, sizeof(double));

    PercentileApproxState *src_percentile = new PercentileApproxState();
    src_percentile->targetQuantile = quantile;
    src_percentile->digest->unserialize(src.ptr + sizeof(double));

    PercentileApproxState* dst_percentile = reinterpret_cast<PercentileApproxState*>(dst->ptr);
    dst_percentile->digest->merge(src_percentile->digest);
    dst_percentile->targetQuantile = quantile;

    delete src_percentile;
}

DoubleVal AggregateFunctions::percentile_approx_finalize(FunctionContext* ctx, const StringVal& src) {
    DCHECK(!src.is_null);

    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState *>(src.ptr);
    double quantile = percentile->targetQuantile;
    double result = percentile->digest->quantile(quantile);

    delete percentile;
    return DoubleVal(result);
}

void AggregateFunctions::decimal_avg_update(FunctionContext* ctx,
        const DecimalVal& src,
        StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(DecimalAvgState), dst->len);
    DecimalAvgState* avg = reinterpret_cast<DecimalAvgState*>(dst->ptr);

    DecimalValue v1 = DecimalValue::from_decimal_val(avg->sum);
    DecimalValue v2 = DecimalValue::from_decimal_val(src);
    DecimalValue v = v1 + v2;
    v.to_decimal_val(&avg->sum);

    ++avg->count;
}

void AggregateFunctions::decimalv2_avg_update(FunctionContext* ctx,
        const DecimalV2Val& src,
        StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(DecimalV2AvgState), dst->len);
    DecimalV2AvgState* avg = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);

    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(avg->sum);
    DecimalV2Value v2 = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value v = v1 + v2;
    v.to_decimal_val(&avg->sum);

    ++avg->count;
}

template <typename T>
void AggregateFunctions::avg_remove(FunctionContext* ctx, const T& src, StringVal* dst) {
    // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
    // because Finalize() returns NULL if count is 0.
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    avg->sum -= src.val;
    --avg->count;
    DCHECK_GE(avg->count, 0);
}

void AggregateFunctions::decimal_avg_remove(doris_udf::FunctionContext* ctx,
        const DecimalVal& src,
        StringVal* dst) {
    // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
    // because Finalize() returns NULL if count is 0.
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(DecimalAvgState), dst->len);
    DecimalAvgState* avg = reinterpret_cast<DecimalAvgState*>(dst->ptr);

    DecimalValue v1 = DecimalValue::from_decimal_val(avg->sum);
    DecimalValue v2 = DecimalValue::from_decimal_val(src);
    DecimalValue v = v1 - v2;
    v.to_decimal_val(&avg->sum);

    --avg->count;
    DCHECK_GE(avg->count, 0);
}

void AggregateFunctions::decimalv2_avg_remove(doris_udf::FunctionContext* ctx,
        const DecimalV2Val& src,
        StringVal* dst) {
    // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
    // because Finalize() returns NULL if count is 0.
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(DecimalV2AvgState), dst->len);
    DecimalV2AvgState* avg = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);

    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(avg->sum);
    DecimalV2Value v2 = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value v = v1 - v2;
    v.to_decimal_val(&avg->sum);

    --avg->count;
    DCHECK_GE(avg->count, 0);
}

void AggregateFunctions::avg_merge(FunctionContext* ctx, const StringVal& src,
        StringVal* dst) {
    const AvgState* src_struct = reinterpret_cast<const AvgState*>(src.ptr);
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* dst_struct = reinterpret_cast<AvgState*>(dst->ptr);
    dst_struct->sum += src_struct->sum;
    dst_struct->count += src_struct->count;
}

void AggregateFunctions::decimal_avg_merge(FunctionContext* ctx, const StringVal& src,
        StringVal* dst) {
    const DecimalAvgState* src_struct = reinterpret_cast<const DecimalAvgState*>(src.ptr);
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(DecimalAvgState), dst->len);
    DecimalAvgState* dst_struct = reinterpret_cast<DecimalAvgState*>(dst->ptr);

    DecimalValue v1 = DecimalValue::from_decimal_val(dst_struct->sum);
    DecimalValue v2 = DecimalValue::from_decimal_val(src_struct->sum);
    DecimalValue v = v1 + v2;
    v.to_decimal_val(&dst_struct->sum);
    dst_struct->count += src_struct->count;
}

void AggregateFunctions::decimalv2_avg_merge(FunctionContext* ctx, const StringVal& src,
        StringVal* dst) {
    const DecimalV2AvgState* src_struct = reinterpret_cast<const DecimalV2AvgState*>(src.ptr);
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(DecimalV2AvgState), dst->len);
    DecimalV2AvgState* dst_struct = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);

    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(dst_struct->sum);
    DecimalV2Value v2 = DecimalV2Value::from_decimal_val(src_struct->sum);
    DecimalV2Value v = v1 + v2;
    v.to_decimal_val(&dst_struct->sum);
    dst_struct->count += src_struct->count;
}

DoubleVal AggregateFunctions::avg_get_value(FunctionContext* ctx, const StringVal& src) {
    AvgState* val_struct = reinterpret_cast<AvgState*>(src.ptr);
    if (val_struct->count == 0) {
        return DoubleVal::null();
    }
    return DoubleVal(val_struct->sum / val_struct->count);
}

DecimalVal AggregateFunctions::decimal_avg_get_value(FunctionContext* ctx, const StringVal& src) {
    DecimalAvgState* val_struct = reinterpret_cast<DecimalAvgState*>(src.ptr);
    if (val_struct->count == 0) {
        return DecimalVal::null();
    }
    DecimalValue v1 = DecimalValue::from_decimal_val(val_struct->sum);
    DecimalValue v = v1 / DecimalValue(val_struct->count);
    DecimalVal res;
    v.to_decimal_val(&res);

    return res;
}

DecimalV2Val AggregateFunctions::decimalv2_avg_get_value(FunctionContext* ctx, const StringVal& src) {
    DecimalV2AvgState* val_struct = reinterpret_cast<DecimalV2AvgState*>(src.ptr);
    if (val_struct->count == 0) {
        return DecimalV2Val::null();
    }
    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(val_struct->sum);
    DecimalV2Value v = v1 / DecimalV2Value(val_struct->count, 0);
    DecimalV2Val res;
    v.to_decimal_val(&res);

    return res;
}

DoubleVal AggregateFunctions::avg_finalize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return DoubleVal::null();
    }
    DoubleVal result = avg_get_value(ctx, src);
    ctx->free(src.ptr);
    return result;
}

DecimalVal AggregateFunctions::decimal_avg_finalize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return DecimalVal::null();
    }
    DecimalVal result = decimal_avg_get_value(ctx, src);
    ctx->free(src.ptr);
    return result;
}

DecimalV2Val AggregateFunctions::decimalv2_avg_finalize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return DecimalV2Val::null();
    }
    DecimalV2Val result = decimalv2_avg_get_value(ctx, src);
    ctx->free(src.ptr);
    return result;
}

void AggregateFunctions::timestamp_avg_update(FunctionContext* ctx,
        const DateTimeVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    double val = DateTimeValue::from_datetime_val(src);
    avg->sum += val;
    ++avg->count;
}

void AggregateFunctions::timestamp_avg_remove(FunctionContext* ctx,
        const DateTimeVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != NULL);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    double val = DateTimeValue::from_datetime_val(src);
    avg->sum -= val;
    --avg->count;
    DCHECK_GE(avg->count, 0);
}

DateTimeVal AggregateFunctions::timestamp_avg_get_value(FunctionContext* ctx,
        const StringVal& src) {
    AvgState* val_struct = reinterpret_cast<AvgState*>(src.ptr);
    if (val_struct->count == 0) {
        return DateTimeVal::null();
    }
    DateTimeValue tv(val_struct->sum / val_struct->count);
    DateTimeVal result;
    tv.to_datetime_val(&result);
    return result;
}

DateTimeVal AggregateFunctions::timestamp_avg_finalize(FunctionContext* ctx,
        const StringVal& src) {
    if (src.is_null) {
        return DateTimeVal::null();
    }
    DateTimeVal result = timestamp_avg_get_value(ctx, src);
    ctx->free(src.ptr);
    return result;
}

void AggregateFunctions::count_star_update(FunctionContext*, BigIntVal* dst) {
    DCHECK(!dst->is_null);
    ++dst->val;
}

void AggregateFunctions::count_star_remove(FunctionContext*, BigIntVal* dst) {
    DCHECK(!dst->is_null);
    --dst->val;
    DCHECK_GE(dst->val, 0);
}

template<typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::sum(FunctionContext* ctx, const SRC_VAL& src, DST_VAL* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        init_zero<DST_VAL>(ctx, dst);
    }

    dst->val += src.val;
}

template<>
void AggregateFunctions::sum(FunctionContext* ctx, const DecimalVal& src, DecimalVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        dst->is_null = false;
        dst->set_to_zero();
    }

    DecimalValue new_src = DecimalValue::from_decimal_val(src);
    DecimalValue new_dst = DecimalValue::from_decimal_val(*dst);
    new_dst = new_dst + new_src;
    new_dst.to_decimal_val(dst);
}

template<>
void AggregateFunctions::sum(FunctionContext* ctx, const DecimalV2Val& src, DecimalV2Val* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        dst->is_null = false;
        dst->set_to_zero();
    }

    DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value new_dst = DecimalV2Value::from_decimal_val(*dst);
    new_dst = new_dst + new_src;
    new_dst.to_decimal_val(dst);
}

template<>
void AggregateFunctions::sum(FunctionContext* ctx, const LargeIntVal& src, LargeIntVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        dst->is_null = false;
        dst->val = 0;
    }

    dst->val += src.val;
}

template<typename T>
void AggregateFunctions::min(FunctionContext*, const T& src, T* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null || src.val < dst->val) {
        *dst = src;
    }
}

template<typename T>
void AggregateFunctions::max(FunctionContext*, const T& src, T* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null || src.val > dst->val) {
        *dst = src;
    }
}

template<>
void AggregateFunctions::min(FunctionContext*, const DecimalVal& src, DecimalVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
    } else {
        DecimalValue new_src = DecimalValue::from_decimal_val(src);
        DecimalValue new_dst = DecimalValue::from_decimal_val(*dst);

        if (new_src < new_dst) {
            *dst = src;
        }
    }
}

template<>
void AggregateFunctions::min(FunctionContext*, const DecimalV2Val& src, DecimalV2Val* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
    } else {
        DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
        DecimalV2Value new_dst = DecimalV2Value::from_decimal_val(*dst);

        if (new_src < new_dst) {
            *dst = src;
        }
    }
}


template<>
void AggregateFunctions::min(FunctionContext*, const LargeIntVal& src, LargeIntVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
        return;
    }

    if (src.val < dst->val) {
        dst->val = src.val;
    }
}

template<>
void AggregateFunctions::max(FunctionContext*, const DecimalVal& src, DecimalVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
    } else {
        DecimalValue new_src = DecimalValue::from_decimal_val(src);
        DecimalValue new_dst = DecimalValue::from_decimal_val(*dst);

        if (new_src > new_dst) {
            *dst = src;
        }
    }
}

template<>
void AggregateFunctions::max(FunctionContext*, const DecimalV2Val& src, DecimalV2Val* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
    } else {
        DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
        DecimalV2Value new_dst = DecimalV2Value::from_decimal_val(*dst);

        if (new_src > new_dst) {
            *dst = src;
        }
    }
}


template<>
void AggregateFunctions::max(FunctionContext*, const LargeIntVal& src, LargeIntVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
        return;
    }

    if (src.val > dst->val) {
        dst->val = src.val;
    }
}

void AggregateFunctions::init_null_string(FunctionContext* c, StringVal* dst) {
    dst->is_null = true;
    dst->ptr = NULL;
    dst->len = 0;
}

template<>
void AggregateFunctions::min(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null ||
            StringValue::from_string_val(src) < StringValue::from_string_val(*dst)) {
        if (!dst->is_null) {
            ctx->free(dst->ptr);
        }
        uint8_t* copy = ctx->allocate(src.len);
        memcpy(copy, src.ptr, src.len);
        *dst = StringVal(copy, src.len);
    }
}

template<>
void AggregateFunctions::max(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null ||
            StringValue::from_string_val(src) > StringValue::from_string_val(*dst)) {
        if (!dst->is_null) {
            ctx->free(dst->ptr);
        }
        uint8_t* copy = ctx->allocate(src.len);
        memcpy(copy, src.ptr, src.len);
        *dst = StringVal(copy, src.len);
    }
}

template<>
void AggregateFunctions::min(FunctionContext*,
                             const DateTimeVal& src, DateTimeVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
        return;
    }

    DateTimeValue src_tv = DateTimeValue::from_datetime_val(src);
    DateTimeValue dst_tv = DateTimeValue::from_datetime_val(*dst);

    if (src_tv < dst_tv) {
        *dst = src;
    }
}

template<>
void AggregateFunctions::max(FunctionContext*,
                             const DateTimeVal& src, DateTimeVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        *dst = src;
        return;
    }

    DateTimeValue src_tv = DateTimeValue::from_datetime_val(src);
    DateTimeValue dst_tv = DateTimeValue::from_datetime_val(*dst);

    if (src_tv > dst_tv) {
        *dst = src;
    }
}

void AggregateFunctions::string_concat(FunctionContext* ctx, const StringVal& src,
                                      const StringVal& separator, StringVal* result) {
    if (src.is_null) {
        return;
    }

    if (result->is_null) {
        uint8_t* copy = ctx->allocate(src.len);
        memcpy(copy, src.ptr, src.len);
        *result = StringVal(copy, src.len);
        return;
    }

    const StringVal* sep_ptr = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM :
                               &separator;

    int new_size = result->len + sep_ptr->len + src.len;
    result->ptr = ctx->reallocate(result->ptr, new_size);
    memcpy(result->ptr + result->len, sep_ptr->ptr, sep_ptr->len);
    result->len += sep_ptr->len;
    memcpy(result->ptr + result->len, src.ptr, src.len);
    result->len += src.len;
}

// StringConcat intermediate state starts with the length of the first
// separator, followed by the accumulated string.  The accumulated
// string starts with the separator of the first value that arrived in
// StringConcatUpdate().
typedef int StringConcatHeader;
// Delimiter to use if the separator is NULL.

void AggregateFunctions::string_concat_update(FunctionContext* ctx,
        const StringVal& src, StringVal* result) {
    string_concat_update(ctx, src, DEFAULT_STRING_CONCAT_DELIM, result);
}

void AggregateFunctions::string_concat_update(FunctionContext* ctx,
        const StringVal& src, const StringVal& separator, StringVal* result) {
    if (src.is_null) {
        return;
    }
    const StringVal* sep = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM : &separator;
    if (result->is_null) {
        // Header of the intermediate state holds the length of the first separator.
        const int header_len = sizeof(StringConcatHeader);
        DCHECK(header_len == sizeof(sep->len));
        *result = StringVal(ctx->allocate(header_len), header_len);
        if (result->is_null) {
            return;
        }
        *reinterpret_cast<StringConcatHeader*>(result->ptr) = sep->len;
    }
    result->append(ctx, sep->ptr, sep->len, src.ptr, src.len);
}

void AggregateFunctions::string_concat_merge(FunctionContext* ctx,
    const StringVal& src, StringVal* result) {
    if (src.is_null) {
        return;
    }
    const int header_len = sizeof(StringConcatHeader);
    if (result->is_null) {
         // Copy the header from the first intermediate value.
        *result = StringVal(ctx->allocate(header_len), header_len);
        if (result->is_null) {
            return;
        }
        *reinterpret_cast<StringConcatHeader*>(result->ptr) =
            *reinterpret_cast<StringConcatHeader*>(src.ptr);
    }
    // Append the string portion of the intermediate src to result (omit src's header).
    result->append(ctx, src.ptr + header_len, src.len - header_len);
}

StringVal AggregateFunctions::string_concat_finalize(FunctionContext* ctx,
        const StringVal& src) {
    if (src.is_null) {
        return src;
    }
    const int header_len = sizeof(StringConcatHeader);
    DCHECK(src.len >= header_len);
    int sep_len = *reinterpret_cast<StringConcatHeader*>(src.ptr);
    DCHECK(src.len >= header_len + sep_len);
    // Remove the header and the first separator.
    StringVal result = StringVal::copy_from(ctx, src.ptr + header_len + sep_len,
            src.len - header_len - sep_len);
    ctx->free(src.ptr);
    return result;
}


// Compute distinctpc and distinctpcsa using Flajolet and Martin's algorithm
// (Probabilistic Counting Algorithms for Data Base Applications)
// We have implemented two variants here: one with stochastic averaging (with PCSA
// postfix) and one without.
// There are 4 phases to compute the aggregate:
//   1. allocate a bitmap, stored in the aggregation tuple's output string slot
//   2. update the bitmap per row (UpdateDistinctEstimateSlot)
//   3. for distributed plan, merge the bitmaps from all the nodes
//      (UpdateMergeEstimateSlot)
//   4. compute the estimate using the bitmaps when all the rows are processed
//      (FinalizeEstimateSlot)
const static int NUM_PC_BITMAPS = 64; // number of bitmaps
const static int PC_BITMAP_LENGTH = 32; // the length of each bit map
const static float PC_THETA = 0.77351f; // the magic number to compute the final result

void AggregateFunctions::pc_init(FunctionContext* c, StringVal* dst) {
    // Initialize the distinct estimate bit map - Probabilistic Counting Algorithms for Data
    // Base Applications (Flajolet and Martin)
    //
    // The bitmap is a 64bit(1st index) x 32bit(2nd index) matrix.
    // So, the string length of 256 byte is enough.
    // The layout is:
    //   row  1: 8bit 8bit 8bit 8bit
    //   row  2: 8bit 8bit 8bit 8bit
    //   ...     ..
    //   ...     ..
    //   row 64: 8bit 8bit 8bit 8bit
    //
    // Using 32bit length, we can count up to 10^8. This will not be enough for Fact table
    // primary key, but once we approach the limit, we could interpret the result as
    // "every row is distinct".
    //
    // We use "string" type for DISTINCT_PC function so that we can use the string
    // slot to hold the bitmaps.
    dst->is_null = false;
    int str_len = NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8;
    dst->ptr = c->allocate(str_len);
    dst->len = str_len;
    memset(dst->ptr, 0, str_len);
}

static inline void set_distinct_estimate_bit(uint8_t* bitmap,
        uint32_t row_index, uint32_t bit_index) {
    // We need to convert Bitmap[alpha,index] into the index of the string.
    // alpha tells which of the 32bit we've to jump to.
    // index then lead us to the byte and bit.
    uint32_t* int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
    int_bitmap[row_index] |= (1 << bit_index);
}

static inline bool get_distinct_estimate_bit(uint8_t* bitmap,
        uint32_t row_index, uint32_t bit_index) {
    uint32_t* int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
    return ((int_bitmap[row_index] & (1 << bit_index)) > 0);
}

template<typename T>
void AggregateFunctions::pc_update(FunctionContext* c, const T& input, StringVal* dst) {
    if (input.is_null) {
        return;
    }

    // Core of the algorithm. This is a direct translation of the code in the paper.
    // Please see the paper for details. For simple averaging, we need to compute hash
    // values NUM_PC_BITMAPS times using NUM_PC_BITMAPS different hash functions (by using a
    // different seed).
    for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
        uint32_t hash_value = AnyValUtil::hash(input, i);
        int bit_index = __builtin_ctz(hash_value);

        if (UNLIKELY(hash_value == 0)) {
            bit_index = PC_BITMAP_LENGTH - 1;
        }

        // Set bitmap[i, bit_index] to 1
        set_distinct_estimate_bit(dst->ptr, i, bit_index);
    }
}

template<typename T>
void AggregateFunctions::pcsa_update(FunctionContext* c, const T& input, StringVal* dst) {
    if (input.is_null) {
        return;
    }

    // Core of the algorithm. This is a direct translation of the code in the paper.
    // Please see the paper for details. Using stochastic averaging, we only need to
    // the hash value once for each row.
    uint32_t hash_value = AnyValUtil::hash(input, 0);
    uint32_t row_index = hash_value % NUM_PC_BITMAPS;

    // We want the zero-based position of the least significant 1-bit in binary
    // representation of hash_value. __builtin_ctz does exactly this because it returns
    // the number of trailing 0-bits in x (or undefined if x is zero).
    int bit_index = __builtin_ctz(hash_value / NUM_PC_BITMAPS);

    if (UNLIKELY(hash_value == 0)) {
        bit_index = PC_BITMAP_LENGTH - 1;
    }

    // Set bitmap[row_index, bit_index] to 1
    set_distinct_estimate_bit(dst->ptr, row_index, bit_index);
}

std::string distinct_estimate_bitmap_to_string(uint8_t* v) {
    std::stringstream debugstr;

    for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
        for (int j = 0; j < PC_BITMAP_LENGTH; ++j) {
            // print bitmap[i][j]
            debugstr << get_distinct_estimate_bit(v, i, j);
        }

        debugstr << "\n";
    }

    debugstr << "\n";
    return debugstr.str();
}

void AggregateFunctions::pc_merge(FunctionContext* c,
                                 const StringVal& src, StringVal* dst) {
    DCHECK(!src.is_null);
    DCHECK(!dst->is_null);
    DCHECK_EQ(src.len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);

    // Merge the bits
    // I think _mm_or_ps can do it, but perf doesn't really matter here. We call this only
    // once group per node.
    for (int i = 0; i < NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8; ++i) {
        *(dst->ptr + i) |= *(src.ptr + i);
    }

    VLOG_ROW << "UpdateMergeEstimateSlot Src Bit map:\n"
             << distinct_estimate_bitmap_to_string(src.ptr);
    VLOG_ROW << "UpdateMergeEstimateSlot Dst Bit map:\n"
             << distinct_estimate_bitmap_to_string(dst->ptr);
}

double distince_estimate_finalize(const StringVal& src) {
    DCHECK(!src.is_null);
    DCHECK_EQ(src.len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);
    VLOG_ROW << "FinalizeEstimateSlot Bit map:\n"
             << distinct_estimate_bitmap_to_string(src.ptr);

    // We haven't processed any rows if none of the bits are set. Therefore, we have zero
    // distinct rows. We're overwriting the result in the same string buffer we've
    // allocated.
    bool is_empty = true;

    for (int i = 0; i < NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8; ++i) {
        if (src.ptr[i] != 0) {
            is_empty = false;
            break;
        }
    }

    if (is_empty) {
        return 0;
    }

    // Convert the bitmap to a number, please see the paper for details
    // In short, we count the average number of leading 1s (per row) in the bit map.
    // The number is proportional to the log2(1/NUM_PC_BITMAPS of  the actual number of
    // distinct).
    // To get the actual number of distinct, we'll do 2^avg / PC_THETA.
    // PC_THETA is a magic number.
    int sum = 0;

    for (int i = 0; i < NUM_PC_BITMAPS; ++i) {
        int row_bit_count = 0;

        // Count the number of leading ones for each row in the bitmap
        // We could have used the build in __builtin_clz to count of number of leading zeros
        // but we first need to invert the 1 and 0.
        while (get_distinct_estimate_bit(src.ptr, i, row_bit_count)
                && row_bit_count < PC_BITMAP_LENGTH) {
            ++row_bit_count;
        }

        sum += row_bit_count;
    }

    double avg = static_cast<double>(sum) / static_cast<double>(NUM_PC_BITMAPS);
    double result = std::pow(static_cast<double>(2), avg) / PC_THETA;
    return result;
}

StringVal AggregateFunctions::pc_finalize(FunctionContext* c, const StringVal& src) {
    double estimate = distince_estimate_finalize(src);
    int64_t result = estimate;
    // TODO: this should return bigint. this is a hack
    std::stringstream ss;
    ss << result;
    std::string str = ss.str();
    StringVal dst = src;
    memcpy(dst.ptr, str.c_str(), str.length());
    dst.len = str.length();
    return dst;
}

StringVal AggregateFunctions::pcsa_finalize(FunctionContext* c, const StringVal& src) {
    // When using stochastic averaging, the result has to be multiplied by NUM_PC_BITMAPS.
    double estimate = distince_estimate_finalize(src) * NUM_PC_BITMAPS;
    int64_t result = estimate;
    // TODO: this should return bigint. this is a hack
    std::stringstream ss;
    ss << result;
    std::string str = ss.str();
    StringVal dst = src;
    memcpy(dst.ptr, str.c_str(), str.length());
    dst.len = str.length();
    return dst;
}

void AggregateFunctions::hll_init(FunctionContext* ctx, StringVal* dst) {
    int str_len = std::pow(2, HLL_COLUMN_PRECISION);
    dst->is_null = false;
    dst->ptr = ctx->allocate(str_len);
    dst->len = str_len;
    memset(dst->ptr, 0, str_len);
}

template <typename T>
void AggregateFunctions::hll_update(FunctionContext* ctx, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    DCHECK(!dst->is_null);
    DCHECK_EQ(dst->len, std::pow(2, HLL_COLUMN_PRECISION));
    uint64_t hash_value = AnyValUtil::hash64_murmur(src, HashUtil::MURMUR_SEED);

    if (hash_value != 0) {
        // Use the lower bits to index into the number of streams and then
        // find the first 1 bit after the index bits.
        int idx = hash_value % dst->len;
        // uint8_t first_one_bit = __buiHLL_LENltin_ctzl(hash_value >> HLL_PRECISION) + 1;
        uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;
        dst->ptr[idx] = std::max(dst->ptr[idx], first_one_bit);
    }
}

void AggregateFunctions::hll_merge(FunctionContext* ctx, const StringVal& src,
                                   StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    DCHECK_EQ(dst->len, std::pow(2, HLL_COLUMN_PRECISION));
    DCHECK_EQ(src.len, std::pow(2, HLL_COLUMN_PRECISION));

    for (int i = 0; i < src.len; ++i) {
        dst->ptr[i] = std::max(dst->ptr[i], src.ptr[i]);
    }
}

StringVal AggregateFunctions::hll_finalize(FunctionContext* ctx, const StringVal& src) {
    double estimate = hll_algorithm(src);
    // Output the estimate as ascii string
    std::stringstream out;
    out << (int64_t)estimate;
    std::string out_str = out.str();
    StringVal result_str(ctx, out_str.size());
    memcpy(result_str.ptr, out_str.c_str(), result_str.len);
    return result_str;
}
    
void AggregateFunctions::hll_union_agg_init(FunctionContext* ctx, HllVal* dst) {
    dst->init(ctx);
}

void AggregateFunctions::hll_union_agg_update(FunctionContext* ctx, 
                                              const HllVal& src, HllVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(!dst->is_null);
    
    dst->agg_parse_and_cal(src);
    return ;
}

void AggregateFunctions::hll_union_agg_merge(FunctionContext* ctx, const HllVal& src, HllVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    DCHECK_EQ(dst->len, HLL_COLUMN_DEFAULT_LEN);
    DCHECK_EQ(src.len, HLL_COLUMN_DEFAULT_LEN);
     
    dst->agg_merge(src);
}

doris_udf::BigIntVal AggregateFunctions::hll_union_agg_finalize(doris_udf::FunctionContext* ctx,
                                                               const HllVal& src) {
    double estimate = hll_algorithm(src);
    BigIntVal result((int64_t)estimate);
    return result;
}

void AggregateFunctions::hll_union_agg_init(doris_udf::FunctionContext* ctx, doris_udf::StringVal* slot) {
    hll_union_agg_init(ctx, static_cast<HllVal*> (slot));
}

void AggregateFunctions::hll_union_agg_update(doris_udf::FunctionContext* ctx, const doris_udf::StringVal& src,
                          doris_udf::StringVal* dst) {
    hll_union_agg_update(ctx, static_cast<const HllVal&> (src), static_cast<HllVal*> (dst));
}

void AggregateFunctions::hll_union_agg_merge(doris_udf::FunctionContext* ctx, const doris_udf::StringVal& src,
                         doris_udf::StringVal* dst) {
    hll_union_agg_merge(ctx, static_cast<const HllVal&> (src), static_cast<HllVal*> (dst));
}

doris_udf::StringVal AggregateFunctions::hll_union_agg_finalize(doris_udf::FunctionContext* ctx, const StringVal& src) {
    BigIntVal intVal = hll_union_agg_finalize(ctx, static_cast<const HllVal&> (src));
    return AnyValUtil::from_string_temp(ctx, std::to_string(intVal.val));;
}

int64_t AggregateFunctions::hll_algorithm(uint8_t *pdata, int data_len) {
    DCHECK_EQ(data_len, HLL_REGISTERS_COUNT);
    
    const int num_streams = HLL_REGISTERS_COUNT;
    // Empirical constants for the algorithm.
    float alpha = 0;
    
    if (num_streams == 16) {
        alpha = 0.673f;
    } else if (num_streams == 32) {
        alpha = 0.697f;
    } else if (num_streams == 64) {
        alpha = 0.709f;
    } else {
        alpha = 0.7213f / (1 + 1.079f / num_streams);
    }
    
    float harmonic_mean = 0;
    int num_zero_registers = 0;
    
    for (int i = 0; i < data_len; ++i) {
        harmonic_mean += powf(2.0f, -pdata[i]);
        
        if (pdata[i] == 0) {
            ++num_zero_registers;
        }
    }
    
    harmonic_mean = 1.0f / harmonic_mean;
    double estimate = alpha * num_streams * num_streams * harmonic_mean;
    // according to HerperLogLog current correction, if E is cardinal
    // E =< num_streams * 2.5 , LC has higher accuracy.
    // num_streams * 2.5 < E , HerperLogLog has higher accuracy.
    // Generally , we can use HerperLogLog to produce value as E.
    if (estimate <= num_streams * 2.5 && num_zero_registers != 0) {
        // Estimated cardinality is too low. Hll is too inaccurate here, instead use
        // linear counting.
        estimate = num_streams * log(static_cast<float>(num_streams) / num_zero_registers);
    } else if (num_streams == 16384 && estimate < 72000) {
        // when Linear Couint change to HerperLoglog according to HerperLogLog Correction,
        // there are relatively large fluctuations, we fixed the problem refer to redis.
        double bias = 5.9119 * 1.0e-18 * (estimate * estimate * estimate * estimate)
        - 1.4253 * 1.0e-12 * (estimate * estimate * estimate) +
        1.2940 * 1.0e-7 * (estimate * estimate)
        - 5.2921 * 1.0e-3 * estimate +
        83.3216;
        estimate -= estimate * (bias / 100);
    }
    return (int64_t)(estimate + 0.5);
}

void AggregateFunctions::hll_raw_agg_init(
        FunctionContext* ctx,
        HllVal* dst) {
    hll_union_agg_init(ctx, dst);
}

void AggregateFunctions::hll_raw_agg_update(
        FunctionContext* ctx,
        const HllVal& src,
        HllVal* dst) {
    hll_union_agg_update(ctx, src, dst);
}

void AggregateFunctions::hll_raw_agg_merge(
        FunctionContext* ctx,
        const HllVal& src,
        HllVal* dst) {
    hll_union_agg_merge(ctx, src, dst);
}

doris_udf::HllVal AggregateFunctions::hll_raw_agg_finalize(
        doris_udf::FunctionContext* ctx,
        const HllVal& src) {
    DCHECK(!src.is_null);
    DCHECK_EQ(src.len, HLL_COLUMN_DEFAULT_LEN);

    HllVal result;
    result.init(ctx);
    memcpy(result.ptr, src.ptr, src.len);
    return result;
}

// TODO chenhao , reduce memory copy
// multi distinct state for numertic
// serialize order type:value:value:value ...
template <typename T>
class MultiDistinctNumericState {
public:

    static void create(StringVal* dst) {
        dst->is_null = false;
        const int state_size = sizeof(MultiDistinctNumericState<T>);
        MultiDistinctNumericState<T>* state = new MultiDistinctNumericState<T>();
        if (std::is_same<T, TinyIntVal>::value) {
            state->_type = FunctionContext::TYPE_TINYINT;
        } else if (std::is_same<T, SmallIntVal>::value) {
            state->_type = FunctionContext::TYPE_SMALLINT;
        } else if (std::is_same<T, IntVal>::value) {
            state->_type = FunctionContext::TYPE_INT;
        } else if (std::is_same<T, BigIntVal>::value) {
            state->_type = FunctionContext::TYPE_BIGINT;
        } else if (std::is_same<T, LargeIntVal>::value) {
            state->_type = FunctionContext::TYPE_LARGEINT;
        } else if (std::is_same<T, DoubleVal>::value) {
            state->_type = FunctionContext::TYPE_DOUBLE;
        } else if (std::is_same<T, FloatVal>::value) {
            state->_type = FunctionContext::TYPE_FLOAT;
        } else {
            DCHECK(false);
        }
        dst->len = state_size;
        dst->ptr = (uint8_t*)state;
    }

    static void destory(const StringVal& dst) {
        delete (MultiDistinctNumericState<T>*)dst.ptr;
    }

    void update(T& t) {
        _set.insert(t);
    }

    // type:one byte  value:sizeof(T)
    StringVal serialize(FunctionContext* ctx) {
        size_t type_size = sizeof(((T*)0)->val);
        const size_t serialized_set_length = sizeof(uint8_t) + type_size * _set.size();
        StringVal result(ctx, serialized_set_length);
        uint8_t* type_writer = result.ptr;
        // type
        *type_writer = (uint8_t)_type;
        type_writer++;
        // value
        for (auto& value : _set) {
            memcpy(type_writer, &value.val, type_size);
            type_writer += type_size;
        }
        return result;
    }

    void unserialize(StringVal& src) {
        size_t type_size = sizeof(((T*)0)->val);
        const uint8_t* type_reader = src.ptr;
        const uint8_t* end = src.ptr + src.len;
        // type
        _type = (FunctionContext::Type)*type_reader;
        type_reader++;
        // value
        while (type_reader < end) {
            T value;
            value.is_null = false;
            memcpy(&value.val, type_reader, type_size);
            _set.insert(value);
            type_reader += type_size;
        }
    }

    // merge set
    void merge(MultiDistinctNumericState& state) {
        _set.insert(state._set.begin(), state._set.end());
    }

    // count
    BigIntVal count_finalize() {
        return BigIntVal(_set.size());
    }
  
    // sum for double, decimal
    DoubleVal sum_finalize_double() {
        double sum = 0;
        for (auto& value : _set) {
            sum += value.val;
        }
        return DoubleVal(sum);
    }

    // sum for largeint 
    LargeIntVal sum_finalize_largeint() {
        __int128 sum = 0;
        for (auto& value : _set) {
            sum += value.val;
        }
        return LargeIntVal(sum);
    }

    // sum for tinyint, smallint, int, bigint
    BigIntVal sum_finalize_bigint() {
        int64_t sum = 0;
        for (auto& value : _set) {
            sum += value.val;
        }
        return BigIntVal(sum);
    }

    FunctionContext::Type set_type() {
        return _type;
    }

private:

    class NumericHashHelper {   
    public:
        size_t operator()(const T& obj) const {
            size_t result = AnyValUtil::hash64_murmur(obj, HashUtil::MURMUR_SEED);   
            return result;
        }
    };

    std::unordered_set<T, NumericHashHelper> _set;
    // Because Anyval does not provide the hash function, in order 
    // to adopt the type different from the template, the pointer is used
    // HybirdSetBase* _set;
    // _type is serialized into buffer by one byte
    FunctionContext::Type _type;
};

// multi distinct state for string
// serialize order type:len:value:len:value ...
class MultiDistinctStringCountState {
public:

    static void create(StringVal* dst) {
        dst->is_null = false;
        const int state_size = sizeof(MultiDistinctStringCountState);
        MultiDistinctStringCountState* state = new MultiDistinctStringCountState();
        // type length
        state->_type = FunctionContext::TYPE_STRING;
        dst->len = state_size;
        dst->ptr = (uint8_t*)state;
    }

    static void destory(const StringVal& dst) {
        delete (MultiDistinctStringCountState*)dst.ptr;
    }

    inline void update(StringValue* sv) {
        _set.insert(sv);
    }

    StringVal serialize(FunctionContext* ctx) {
        // calculate total serialize buffer length
        int total_serialized_set_length = 1;
        HybirdSetBase::IteratorBase* iterator = _set.begin();
        while (iterator->has_next()) {
            const StringValue* value = 
                        reinterpret_cast<const StringValue*>(iterator->get_value());
            total_serialized_set_length += STRING_LENGTH_RECORD_LENGTH + value->len;
            iterator->next();
        }
        StringVal result(ctx, total_serialized_set_length);
        uint8_t* writer = result.ptr;
        // type
        *writer = _type;
        writer ++;
        iterator = _set.begin();
        while (iterator->has_next()) {
            const StringValue* value = reinterpret_cast<const StringValue*>
            (iterator->get_value());
            // length, it is unnecessary to consider little or big endian for
            // all running in little-endian.
            *(int*)writer = value->len;
            writer += STRING_LENGTH_RECORD_LENGTH;
            // value
            memcpy(writer, value->ptr, value->len);
            writer += value->len;
            iterator->next();
        }
        return result;
    }
    
    void unserialize(StringVal& src) {
        uint8_t* reader = src.ptr;
        // skip type ,no used now
        _type = (FunctionContext::Type)*reader;
        DCHECK(_type == FunctionContext::TYPE_STRING);
        reader ++;
        const uint8_t* end = src.ptr + src.len;
        while (reader < end) {
            const int length = *(int*)reader;
            reader += STRING_LENGTH_RECORD_LENGTH;
            StringValue value((char*)reader, length);
            _set.insert(&value);
            reader += length;
        }
        DCHECK(reader == end);
    }
   
    // merge set 
    void merge(MultiDistinctStringCountState& state) {
        _set.insert(&(state._set));
    }
    
    BigIntVal finalize() {
        return BigIntVal(_set.size());
    }

    FunctionContext::Type set_type() {
        return _type;
    }
  
    static const int STRING_LENGTH_RECORD_LENGTH = 4; 
private:
 
    StringValueSet _set;
    // _type is serialized into buffer by one byte
    FunctionContext::Type _type;
};

// multi distinct state for decimal
// serialize order type:int_len:frac_len:sign:int_len ... 
class MultiDistinctDecimalState {
public:

    static void create(StringVal* dst) {
        dst->is_null = false;
        const int state_size = sizeof(MultiDistinctDecimalState);
        MultiDistinctDecimalState* state = new MultiDistinctDecimalState();
        state->_type = FunctionContext::TYPE_DECIMAL;
        dst->len = state_size;
        dst->ptr = (uint8_t*)state;
    }

    static void destory(const StringVal& dst) {
        delete (MultiDistinctDecimalState*)dst.ptr;
    }

    void update(DecimalVal& t) {
        _set.insert(DecimalValue::from_decimal_val(t));
    }

    // type:one byte  value:sizeof(T)
    StringVal serialize(FunctionContext* ctx) {
        const int serialized_set_length = sizeof(uint8_t) 
                   + (DECIMAL_INT_LEN_BYTE_SIZE 
                     + DECIMAL_FRAC_BYTE_SIZE
                     + DECIMAL_SIGN_BYTE_SIZE
                     + DECIMAL_BUFFER_BYTE_SIZE) * _set.size();
        StringVal result(ctx, serialized_set_length);
        uint8_t* writer = result.ptr;
        *writer = (uint8_t)_type;
        writer++;
        // for int_length and frac_length, uint8_t will not overflow.
        for (auto& value : _set) {
            *writer = value._int_length;
            writer += DECIMAL_INT_LEN_BYTE_SIZE;
            *writer = value._frac_length;
            writer += DECIMAL_FRAC_BYTE_SIZE;
            *writer = value._sign;
            writer += DECIMAL_SIGN_BYTE_SIZE;
            memcpy(writer, value._buffer, DECIMAL_BUFFER_BYTE_SIZE);
            writer += DECIMAL_BUFFER_BYTE_SIZE;
        }    
        return result;
    }    

    void unserialize(StringVal& src) {
        const uint8_t* reader = src.ptr;
        // type
        _type = (FunctionContext::Type)*reader;
        reader++;
        const uint8_t* end = src.ptr + src.len;
        // value
        while (reader < end) {
            DecimalValue value;
            value._int_length = *reader;
            reader += DECIMAL_INT_LEN_BYTE_SIZE;
            value._frac_length = *reader;
            reader += DECIMAL_FRAC_BYTE_SIZE;
            value._sign = *reader;
            reader += DECIMAL_SIGN_BYTE_SIZE;
            value._buffer_length = DECIMAL_BUFF_LENGTH;
            memcpy(value._buffer, reader, DECIMAL_BUFFER_BYTE_SIZE);
            reader += DECIMAL_BUFFER_BYTE_SIZE;
            _set.insert(value);
        }    
    }    
 
    FunctionContext::Type set_type() {
        return _type;
    }

    // merge set
    void merge(MultiDistinctDecimalState& state) {
        _set.insert(state._set.begin(), state._set.end());
    }    

    // count
    BigIntVal count_finalize() {
        return BigIntVal(_set.size());
    }   

    DecimalVal sum_finalize() {
        DecimalValue sum;
        for (auto& value : _set) {
             sum += value;
        }
        DecimalVal result;
        sum.to_decimal_val(&result); 
        return result;
    }

private:

    const int DECIMAL_INT_LEN_BYTE_SIZE = 1; 
    const int DECIMAL_FRAC_BYTE_SIZE = 1; 
    const int DECIMAL_SIGN_BYTE_SIZE = 1; 
    const int DECIMAL_BUFFER_BYTE_SIZE = 36;

    std::unordered_set<DecimalValue> _set;
    FunctionContext::Type _type;
};

class MultiDistinctDecimalV2State {
public:

    static void create(StringVal* dst) {
        dst->is_null = false;
        const int state_size = sizeof(MultiDistinctDecimalV2State);
        MultiDistinctDecimalV2State* state = new MultiDistinctDecimalV2State();
        state->_type = FunctionContext::TYPE_DECIMALV2;
        dst->len = state_size;
        dst->ptr = (uint8_t*)state;
    }

    static void destory(const StringVal& dst) {
        delete (MultiDistinctDecimalV2State*)dst.ptr;
    }

    void update(DecimalV2Val& t) {
        _set.insert(DecimalV2Value::from_decimal_val(t));
    }

    // type:one byte  value:sizeof(T)
    StringVal serialize(FunctionContext* ctx) {
        const int serialized_set_length = sizeof(uint8_t) 
            + DECIMAL_BYTE_SIZE * _set.size();
        StringVal result(ctx, serialized_set_length);
        uint8_t* writer = result.ptr;
        *writer = (uint8_t)_type;
        writer++;
        // for int_length and frac_length, uint8_t will not overflow.
        for (auto& value : _set) {
            __int128 v = value.value();
            memcpy(writer, &v, DECIMAL_BYTE_SIZE);
            writer += DECIMAL_BYTE_SIZE;
        }    
        return result;
    }    

    void unserialize(StringVal& src) {
        const uint8_t* reader = src.ptr;
        // type
        _type = (FunctionContext::Type)*reader;
        reader++;
        const uint8_t* end = src.ptr + src.len;
        // value
        while (reader < end) {
            __int128 v = 0;
            memcpy(&v, reader, DECIMAL_BYTE_SIZE);
            DecimalV2Value value(v);
            reader += DECIMAL_BYTE_SIZE;
            _set.insert(value);
        }    
    }    
 
    FunctionContext::Type set_type() {
        return _type;
    }

    // merge set
    void merge(MultiDistinctDecimalV2State& state) {
        _set.insert(state._set.begin(), state._set.end());
    }    

    // count
    BigIntVal count_finalize() {
        return BigIntVal(_set.size());
    }   

    DecimalV2Val sum_finalize() {
        DecimalV2Value sum;
        for (auto& value : _set) {
             sum += value;
        }
        DecimalV2Val result;
        sum.to_decimal_val(&result); 
        return result;
    }

private:
    const int DECIMAL_BYTE_SIZE = 16;
    
    std::unordered_set<DecimalV2Value> _set;
    FunctionContext::Type _type;
};

// multi distinct state for date
// serialize order type:packed_time:type:packed_time:type ...
class MultiDistinctCountDateState {
public:
    
    static void create(StringVal* dst) {
        dst->is_null = false;
        const int state_size = sizeof(MultiDistinctCountDateState);
        MultiDistinctCountDateState* state = new MultiDistinctCountDateState();
        state->_type = FunctionContext::TYPE_DATETIME;
        dst->len = state_size;
        dst->ptr = (uint8_t*)state;
    }
    
    static void destory(const StringVal& dst) {
        delete (MultiDistinctCountDateState*)dst.ptr;
    }
    
    void update(DateTimeVal& t) {
        _set.insert(t);
    }
    
    // type:one byte  value:sizeof(T)
    StringVal serialize(FunctionContext* ctx) {
        const int serialized_set_length = sizeof(uint8_t) + 
                   (DATETIME_PACKED_TIME_BYTE_SIZE + DATETIME_TYPE_BYTE_SIZE) * _set.size();
        StringVal result(ctx, serialized_set_length);
        uint8_t* writer = result.ptr;
        // type
        *writer = (uint8_t)_type;
        writer++;
        // value
        for (auto& value : _set) {
            int64_t* packed_time_writer = (int64_t*)writer;
            *packed_time_writer = value.packed_time;
            writer += DATETIME_PACKED_TIME_BYTE_SIZE;
            int* type_writer = (int*)writer;
            *type_writer = value.type;
            writer += DATETIME_TYPE_BYTE_SIZE;
        }
        return result;
    }
    
    void unserialize(StringVal& src) {
        const uint8_t* reader = src.ptr;
        // type
        _type = (FunctionContext::Type)*reader;
        reader++;
        const uint8_t* end = src.ptr + src.len;
        // value
        while (reader < end) {
            DateTimeVal value;
            value.is_null = false;
            int64_t* packed_time_reader = (int64_t*)reader;
            value.packed_time = *packed_time_reader;
            reader += DATETIME_PACKED_TIME_BYTE_SIZE;
            int* type_reader = (int*)reader;
            value.type = *type_reader;
            reader += DATETIME_TYPE_BYTE_SIZE;
            _set.insert(value);
        }
    }
    
    // merge set
    void merge(MultiDistinctCountDateState& state) {
        _set.insert(state._set.begin(), state._set.end());
    }
    
    // count
    BigIntVal count_finalize() {
        return BigIntVal(_set.size());
    }
   
    FunctionContext::Type set_type() {
        return _type;
    }
 
private:
   
    class DateTimeHashHelper {    
    public:
        size_t operator()(const DateTimeVal& obj) const {
            size_t result = AnyValUtil::hash64_murmur(obj, HashUtil::MURMUR_SEED);   
            return result;
        }
    }; 
 
    const int DATETIME_PACKED_TIME_BYTE_SIZE = 8;
    const int DATETIME_TYPE_BYTE_SIZE = 4;

    std::unordered_set<DateTimeVal, DateTimeHashHelper> _set;
    FunctionContext::Type _type;
};
    
template <typename T>
void AggregateFunctions::count_or_sum_distinct_numeric_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctNumericState<T>::create(dst);
}
    
void AggregateFunctions::count_distinct_string_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctStringCountState::create(dst);
}
    
void AggregateFunctions::count_or_sum_distinct_decimal_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctDecimalState::create(dst);
}

void AggregateFunctions::count_or_sum_distinct_decimalv2_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctDecimalV2State::create(dst);
}
    
void AggregateFunctions::count_distinct_date_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctCountDateState::create(dst);
}

template <typename T>
void AggregateFunctions::count_or_sum_distinct_numeric_update(FunctionContext* ctx, T& src,
                         StringVal* dst) {
    DCHECK(!dst->is_null);
    if (src.is_null) return;
    MultiDistinctNumericState<T>* state = reinterpret_cast<MultiDistinctNumericState<T>*>(dst->ptr);
    state->update(src);
}
    
void AggregateFunctions::count_distinct_string_update(FunctionContext* ctx, StringVal& src,
                           StringVal* dst) {
    DCHECK(!dst->is_null);
    if (src.is_null) return;
    MultiDistinctStringCountState* state = reinterpret_cast<MultiDistinctStringCountState*>(dst->ptr);
    StringValue sv = StringValue::from_string_val(src);
    state->update(&sv);
}
    
void AggregateFunctions::count_or_sum_distinct_decimal_update(FunctionContext* ctx, DecimalVal& src,
                                                              StringVal* dst) {
    DCHECK(!dst->is_null);
    if (src.is_null) return;
    MultiDistinctDecimalState* state = reinterpret_cast<MultiDistinctDecimalState*>(dst->ptr);
    state->update(src);
}

void AggregateFunctions::count_or_sum_distinct_decimalv2_update(FunctionContext* ctx, DecimalV2Val& src,
                                                              StringVal* dst) {
    DCHECK(!dst->is_null);
    if (src.is_null) return;
    MultiDistinctDecimalV2State* state = reinterpret_cast<MultiDistinctDecimalV2State*>(dst->ptr);
    state->update(src);
}
 
void AggregateFunctions::count_distinct_date_update(FunctionContext* ctx, DateTimeVal& src,
                                                              StringVal* dst) {
    DCHECK(!dst->is_null);
    if (src.is_null) return;
    MultiDistinctCountDateState* state = reinterpret_cast<MultiDistinctCountDateState*>(dst->ptr);
    state->update(src);
}

template <typename T>
void AggregateFunctions::count_or_sum_distinct_numeric_merge(FunctionContext* ctx, StringVal& src,
                         StringVal* dst) {
   DCHECK(!dst->is_null);
   DCHECK(!src.is_null);
   MultiDistinctNumericState<T>* dst_state = reinterpret_cast<MultiDistinctNumericState<T>*>(dst->ptr);
   // unserialize src
   StringVal src_state_val;
   MultiDistinctNumericState<T>::create(&src_state_val); 
   MultiDistinctNumericState<T>* src_state = reinterpret_cast<MultiDistinctNumericState<T>*>(src_state_val.ptr);
   src_state->unserialize(src);
   DCHECK(dst_state->set_type() == src_state->set_type());
   dst_state->merge(*src_state);
   MultiDistinctNumericState<T>::destory(src_state_val);
}
    
void AggregateFunctions::count_distinct_string_merge(FunctionContext* ctx, StringVal& src,
                          StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctStringCountState* dst_state = reinterpret_cast<MultiDistinctStringCountState*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctStringCountState::create(&src_state_val);
    MultiDistinctStringCountState* src_state = reinterpret_cast<MultiDistinctStringCountState*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctStringCountState::destory(src_state_val); 
}


void AggregateFunctions::count_or_sum_distinct_decimal_merge(FunctionContext* ctx, StringVal& src,
                                                             StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctDecimalState* dst_state = reinterpret_cast<MultiDistinctDecimalState*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctDecimalState::create(&src_state_val);
    MultiDistinctDecimalState* src_state = reinterpret_cast<MultiDistinctDecimalState*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctDecimalState::destory(src_state_val);
}

void AggregateFunctions::count_or_sum_distinct_decimalv2_merge(FunctionContext* ctx, StringVal& src,
                                                             StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctDecimalV2State* dst_state = reinterpret_cast<MultiDistinctDecimalV2State*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctDecimalV2State::create(&src_state_val);
    MultiDistinctDecimalV2State* src_state = reinterpret_cast<MultiDistinctDecimalV2State*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctDecimalV2State::destory(src_state_val);
}
    
void AggregateFunctions::count_distinct_date_merge(FunctionContext* ctx, StringVal& src,
                                                             StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctCountDateState* dst_state = reinterpret_cast<MultiDistinctCountDateState*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctCountDateState::create(&src_state_val);
    MultiDistinctCountDateState* src_state = reinterpret_cast<MultiDistinctCountDateState*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctCountDateState::destory(src_state_val);
}
    
template <typename T>
StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctNumericState<T>* state = reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctNumericState<T>::destory(state_sv);
    return result;
}
    
StringVal AggregateFunctions::count_distinct_string_serialize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctStringCountState* state = reinterpret_cast<MultiDistinctStringCountState*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctStringCountState::destory(state_sv);
    return result;
}

StringVal AggregateFunctions::count_or_sum_distinct_decimal_serialize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalState* state = reinterpret_cast<MultiDistinctDecimalState*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctDecimalState::destory(state_sv);
    return result;
}
    
StringVal AggregateFunctions::count_or_sum_distinct_decimalv2_serialize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalV2State* state = reinterpret_cast<MultiDistinctDecimalV2State*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctDecimalV2State::destory(state_sv);
    return result;
}

StringVal AggregateFunctions::count_distinct_date_serialize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctCountDateState* state = reinterpret_cast<MultiDistinctCountDateState*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctCountDateState::destory(state_sv);
    return result;
}
    
template <typename T>
BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctNumericState<T>* state = reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctNumericState<T>::destory(state_sv);
    return result;
}
    
BigIntVal AggregateFunctions::count_distinct_string_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctStringCountState* state = reinterpret_cast<MultiDistinctStringCountState*>(state_sv.ptr);
    BigIntVal result = state->finalize();
    MultiDistinctStringCountState::destory(state_sv);
    return result;
}

template <typename T>
DoubleVal AggregateFunctions::sum_distinct_double_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctNumericState<T>* state = reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    DoubleVal result = state->sum_finalize_double();
    MultiDistinctNumericState<T>::destory(state_sv);
    return result;
}

template <typename T>
LargeIntVal AggregateFunctions::sum_distinct_largeint_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctNumericState<T>* state = reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    LargeIntVal result = state->sum_finalize_largeint();
    MultiDistinctNumericState<T>::destory(state_sv);
    return result;
}

template <typename T>
BigIntVal AggregateFunctions::sum_distinct_bigint_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctNumericState<T>* state = reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    BigIntVal result = state->sum_finalize_bigint();
    MultiDistinctNumericState<T>::destory(state_sv);
    return result;
}

BigIntVal AggregateFunctions::count_distinct_decimal_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalState* state = reinterpret_cast<MultiDistinctDecimalState*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctDecimalState::destory(state_sv);
    return result;
}

BigIntVal AggregateFunctions::count_distinct_decimalv2_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalV2State* state = reinterpret_cast<MultiDistinctDecimalV2State*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctDecimalV2State::destory(state_sv);
    return result;
}
    
DecimalVal AggregateFunctions::sum_distinct_decimal_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalState* state = reinterpret_cast<MultiDistinctDecimalState*>(state_sv.ptr);
    DecimalVal result = state->sum_finalize();
    MultiDistinctDecimalState::destory(state_sv);
    return result;
}

DecimalV2Val AggregateFunctions::sum_distinct_decimalv2_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalV2State* state = reinterpret_cast<MultiDistinctDecimalV2State*>(state_sv.ptr);
    DecimalV2Val result = state->sum_finalize();
    MultiDistinctDecimalV2State::destory(state_sv);
    return result;
}
    
BigIntVal AggregateFunctions::count_distinct_date_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctCountDateState* state = reinterpret_cast<MultiDistinctCountDateState*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctCountDateState::destory(state_sv);
    return result;
}
    
// An implementation of a simple single pass variance algorithm. A standard UDA must
// be single pass (i.e. does not scan the table more than once), so the most canonical
// two pass approach is not practical.
struct KnuthVarianceState {
    double mean;
    double m2;
    int64_t count;
};

// Set pop=true for population variance, false for sample variance
static double compute_knuth_variance(const KnuthVarianceState& state, bool pop) {
    // Return zero for 1 tuple specified by
    // http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions212.htm
    if (state.count == 1) return 0.0;
    if (pop) return state.m2 / state.count;
    return state.m2 / (state.count - 1);
}

void AggregateFunctions::knuth_var_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    // TODO(zc)
    dst->len = sizeof(KnuthVarianceState);
    dst->ptr = ctx->allocate(dst->len);
    DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
    memset(dst->ptr, 0, dst->len);
}

template <typename T>
void AggregateFunctions::knuth_var_update(FunctionContext* ctx, const T& src,
                                        StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
    if (src.is_null) return;
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(dst->ptr);
    double temp = 1 + state->count;
    double delta = src.val - state->mean;
    double r = delta / temp;
    state->mean += r;
    state->m2 += state->count * delta * r;
    state->count = temp;
}

void AggregateFunctions::knuth_var_merge(FunctionContext* ctx, const StringVal& src,
                                       StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
    DCHECK(!src.is_null);
    DCHECK_EQ(src.len, sizeof(KnuthVarianceState));
    // Reference implementation:
    // http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
    KnuthVarianceState* src_state = reinterpret_cast<KnuthVarianceState*>(src.ptr);
    KnuthVarianceState* dst_state = reinterpret_cast<KnuthVarianceState*>(dst->ptr);
    if (src_state->count == 0) return;
    double delta = dst_state->mean - src_state->mean;
    double sum_count = dst_state->count + src_state->count;
    dst_state->mean = src_state->mean + delta * (dst_state->count / sum_count);
    dst_state->m2 = (src_state->m2) + dst_state->m2 +
        (delta * delta) * (src_state->count * dst_state->count / sum_count);
    dst_state->count = sum_count;
}

DoubleVal AggregateFunctions::knuth_var_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0 || state->count == 1) return DoubleVal::null();
    double variance = compute_knuth_variance(*state, false);
    ctx->free(state_sv.ptr);
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_var_pop_finalize(FunctionContext* ctx,
                                                  const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0) return DoubleVal::null();
    double variance = compute_knuth_variance(*state, true);
    ctx->free(state_sv.ptr);
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_stddev_finalize(FunctionContext* ctx, 
                                                  const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0 || state->count == 1) return DoubleVal::null();
    double variance = sqrt(compute_knuth_variance(*state, false));
    ctx->free(state_sv.ptr);
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_stddev_pop_finalize(FunctionContext* ctx,
                                                     const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0) return DoubleVal::null();
    double variance = sqrt(compute_knuth_variance(*state, true));
    ctx->free(state_sv.ptr);
    return DoubleVal(variance);
}

struct RankState {
    int64_t rank;
    int64_t count;
    RankState() : rank(1), count(0) { }
};

void AggregateFunctions::rank_init(FunctionContext* ctx, StringVal* dst) {
    int str_len = sizeof(RankState);
    dst->is_null = false;
    dst->ptr = ctx->allocate(str_len);
    dst->len = str_len;
    *reinterpret_cast<RankState*>(dst->ptr) = RankState();
}

void AggregateFunctions::rank_update(FunctionContext* ctx, StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK_EQ(dst->len, sizeof(RankState));
    RankState* state = reinterpret_cast<RankState*>(dst->ptr);
    ++state->count;
}

void AggregateFunctions::dense_rank_update(FunctionContext* ctx, StringVal* dst) { }

BigIntVal AggregateFunctions::rank_get_value(FunctionContext* ctx,
        StringVal& src_val) {
    DCHECK(!src_val.is_null);
    DCHECK_EQ(src_val.len, sizeof(RankState));
    RankState* state = reinterpret_cast<RankState*>(src_val.ptr);
    DCHECK_GT(state->count, 0);
    DCHECK_GT(state->rank, 0);
    int64_t result = state->rank;

    // Prepares future calls for the next rank
    state->rank += state->count;
    state->count = 0;
    return BigIntVal(result);
}

BigIntVal AggregateFunctions::dense_rank_get_value(FunctionContext* ctx,
    StringVal& src_val) {
    DCHECK(!src_val.is_null);
    DCHECK_EQ(src_val.len, sizeof(RankState));
    RankState* state = reinterpret_cast<RankState*>(src_val.ptr);
    DCHECK_EQ(state->count, 0);
    DCHECK_GT(state->rank, 0);
    int64_t result = state->rank;

    // Prepares future calls for the next rank
    ++state->rank;
    return BigIntVal(result);
}

BigIntVal AggregateFunctions::rank_finalize(FunctionContext* ctx,
        StringVal& src_val) {
    DCHECK(!src_val.is_null);
    DCHECK_EQ(src_val.len, sizeof(RankState));
    RankState* state = reinterpret_cast<RankState*>(src_val.ptr);
    int64_t result = state->rank;
    ctx->free(src_val.ptr);
    return BigIntVal(result);
}

template <typename T>
void AggregateFunctions::last_val_update(FunctionContext* ctx, const T& src, T* dst) {
    *dst = src;
}

template <>
void AggregateFunctions::last_val_update(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
    if (src.is_null) {
        if (!dst->is_null) {
            ctx->free(dst->ptr);
        }
        *dst = StringVal::null();
        return;
    }

    if (dst->is_null) {
        dst->ptr = ctx->allocate(src.len);
        dst->is_null = false;
    } else {
        dst->ptr = ctx->reallocate(dst->ptr, src.len);
    }
    memcpy(dst->ptr, src.ptr, src.len);
    dst->len = src.len;
}

template <typename T>
void AggregateFunctions::last_val_remove(FunctionContext* ctx, const T& src, T* dst) {
    if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
        *dst = T::null();
    }
}

template <>
void AggregateFunctions::last_val_remove(FunctionContext* ctx, const StringVal& src,
    StringVal* dst) {
    if (ctx->impl()->num_removes() >= ctx->impl()->num_updates()) {
        if (!dst->is_null) {
            ctx->free(dst->ptr);
        }
        *dst = StringVal::null();
    }
}

template <typename T>
void AggregateFunctions::first_val_update(FunctionContext* ctx, const T& src, T* dst) {
    // The first call to first_val_update sets the value of dst.
    if (ctx->impl()->num_updates() > 1) {
        return;
    }
    // num_updates is incremented before calling Update(), so it should never be 0.
    // Remove() should never be called for FIRST_VALUE.
    DCHECK_GT(ctx->impl()->num_updates(), 0);
    DCHECK_EQ(ctx->impl()->num_removes(), 0);
    *dst = src;
}

template <>
void AggregateFunctions::first_val_update(FunctionContext* ctx, const IntVal& src, IntVal* dst) {
    // The first call to FirstValUpdate sets the value of dst.
    if (ctx->impl()->num_updates() > 1) {
        return;
    }
    // num_updates is incremented before calling Update(), so it should never be 0.
    // Remove() should never be called for FIRST_VALUE.
    DCHECK_GT(ctx->impl()->num_updates(), 0);
    DCHECK_EQ(ctx->impl()->num_removes(), 0);
    *dst = src;
}

template <>
void AggregateFunctions::first_val_update(FunctionContext* ctx, const StringVal& src,
        StringVal* dst) {
    if (ctx->impl()->num_updates() > 1) {
        return;
    }
    DCHECK_GT(ctx->impl()->num_updates(), 0);
    DCHECK_EQ(ctx->impl()->num_removes(), 0);
    if (src.is_null) {
        *dst = StringVal::null();
        return;
    }
    *dst = StringVal(ctx->allocate(src.len), src.len);
    memcpy(dst->ptr, src.ptr, src.len);
}

template <typename T>
void AggregateFunctions::first_val_rewrite_update(FunctionContext* ctx, const T& src,
        const BigIntVal&, T* dst) {
    last_val_update<T>(ctx, src, dst);
}

template <typename T>
void AggregateFunctions::offset_fn_init(FunctionContext* ctx, T* dst) {
    DCHECK_EQ(ctx->get_num_args(), 3);
    DCHECK(ctx->is_arg_constant(1));
    DCHECK(ctx->is_arg_constant(2));
    DCHECK_EQ(ctx->get_arg_type(0)->type, ctx->get_arg_type(2)->type);
    *dst = *static_cast<T*>(ctx->get_constant_arg(2));
}
/*
template <>
void AggregateFunctions::offset_fn_init(FunctionContext* ctx, IntVal* dst) {
    DCHECK_EQ(ctx->get_num_args(), 3);
    DCHECK(ctx->is_arg_constant(1));
    DCHECK(ctx->is_arg_constant(2));

    //  DCHECK_EQ(*ctx->GetArgType(0), *ctx->GetArgType(2));
    *dst = *static_cast<IntVal*>(ctx->get_constant_arg(2));
}
*/
template <typename T>
void AggregateFunctions::offset_fn_update(FunctionContext* ctx, const T& src,
    const BigIntVal&, const T& default_value, T* dst) {
    *dst = src;
}

template <>
void AggregateFunctions::offset_fn_update(FunctionContext* ctx, const IntVal& src,
        const BigIntVal&, const IntVal& default_value, IntVal* dst) {
    *dst = src;
}

// Stamp out the templates for the types we need.
template void AggregateFunctions::init_zero<BigIntVal>(FunctionContext*, BigIntVal* dst);

template void AggregateFunctions::sum_remove<BooleanVal, BigIntVal>(
    FunctionContext*, const BooleanVal& src, BigIntVal* dst);
template void AggregateFunctions::sum_remove<TinyIntVal, BigIntVal>(
    FunctionContext*, const TinyIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum_remove<SmallIntVal, BigIntVal>(
    FunctionContext*, const SmallIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum_remove<IntVal, BigIntVal>(
    FunctionContext*, const IntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum_remove<BigIntVal, BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum_remove<FloatVal, DoubleVal>(
    FunctionContext*, const FloatVal& src, DoubleVal* dst);
template void AggregateFunctions::sum_remove<DoubleVal, DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::sum_remove<DecimalVal, DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::sum_remove<DecimalV2Val, DecimalV2Val>(
    FunctionContext*, const DecimalV2Val& src, DecimalV2Val* dst);
template void AggregateFunctions::sum_remove<LargeIntVal, LargeIntVal>(
    FunctionContext*, const LargeIntVal& src, LargeIntVal* dst);

template void AggregateFunctions::avg_update<doris_udf::BooleanVal>(
    doris_udf::FunctionContext*, doris_udf::BooleanVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::IntVal>(
    doris_udf::FunctionContext*, doris_udf::IntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::IntVal>(
    doris_udf::FunctionContext*, doris_udf::IntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::BigIntVal>(
    doris_udf::FunctionContext*, doris_udf::BigIntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::BigIntVal>(
    doris_udf::FunctionContext*, doris_udf::BigIntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::FloatVal>(
    doris_udf::FunctionContext*, doris_udf::FloatVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::FloatVal>(
    doris_udf::FunctionContext*, doris_udf::FloatVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::DoubleVal>(
    doris_udf::FunctionContext*, doris_udf::DoubleVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::DoubleVal>(
    doris_udf::FunctionContext*, doris_udf::DoubleVal const&, doris_udf::StringVal*);
//template void AggregateFunctions::AvgUpdate<doris_udf::LargeIntVal>(
//doris_udf::FunctionContext*, doris_udf::LargeIntVal const&, doris_udf::StringVal*);
//template void AggregateFunctions::AvgRemove<doris_udf::LargeIntVal>(
//doris_udf::FunctionContext*, doris_udf::LargeIntVal const&, doris_udf::StringVal*);

template void AggregateFunctions::sum<BooleanVal, BigIntVal>(
    FunctionContext*, const BooleanVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<TinyIntVal, BigIntVal>(
    FunctionContext*, const TinyIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<SmallIntVal, BigIntVal>(
    FunctionContext*, const SmallIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<IntVal, BigIntVal>(
    FunctionContext*, const IntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<BigIntVal, BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<FloatVal, DoubleVal>(
    FunctionContext*, const FloatVal& src, DoubleVal* dst);
template void AggregateFunctions::sum<DoubleVal, DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);

template void AggregateFunctions::min<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::min<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::min<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::min<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::min<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::min<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::min<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::min<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);

template void AggregateFunctions::avg_remove<doris_udf::BooleanVal>(
    doris_udf::FunctionContext*, doris_udf::BooleanVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::TinyIntVal>(
    doris_udf::FunctionContext*, doris_udf::TinyIntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::TinyIntVal>(
    doris_udf::FunctionContext*, doris_udf::TinyIntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::SmallIntVal>(
    doris_udf::FunctionContext*, doris_udf::SmallIntVal const&, doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::SmallIntVal>(
    doris_udf::FunctionContext*, doris_udf::SmallIntVal const&, doris_udf::StringVal*);

template void AggregateFunctions::max<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::max<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::max<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::max<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::max<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::max<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::max<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::max<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);

template void AggregateFunctions::pc_update(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::pc_update(
    FunctionContext*, const DateTimeVal&, StringVal*);

template void AggregateFunctions::pcsa_update(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::pcsa_update(
    FunctionContext*, const DateTimeVal&, StringVal*);

template void AggregateFunctions::hll_update(
    FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const DateTimeVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const LargeIntVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const DecimalVal&, StringVal*);
template void AggregateFunctions::hll_update(
    FunctionContext*, const DecimalV2Val&, StringVal*);

template void AggregateFunctions::count_or_sum_distinct_numeric_init<TinyIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<SmallIntVal>(
    FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<IntVal>(
    FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<BigIntVal>(
      FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<FloatVal>(
    FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<DoubleVal>(
    FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<LargeIntVal>(
    FunctionContext* ctx, StringVal* dst);


template void AggregateFunctions::count_or_sum_distinct_numeric_update<TinyIntVal>(
    FunctionContext* ctx, TinyIntVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<SmallIntVal>(
    FunctionContext* ctx, SmallIntVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<IntVal>(
    FunctionContext* ctx, IntVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<BigIntVal>(
    FunctionContext* ctx, BigIntVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<FloatVal>(
    FunctionContext* ctx, FloatVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<DoubleVal>(
    FunctionContext* ctx, DoubleVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<LargeIntVal>(
    FunctionContext* ctx, LargeIntVal& src, StringVal* dst);

template void AggregateFunctions::count_or_sum_distinct_numeric_merge<TinyIntVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<SmallIntVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<IntVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<BigIntVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<FloatVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<DoubleVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<LargeIntVal>(
    FunctionContext* ctx, StringVal& src, StringVal* dst);

template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<TinyIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<SmallIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<IntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<BigIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<FloatVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<DoubleVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize<LargeIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);

template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<TinyIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<SmallIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<IntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<BigIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<FloatVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<DoubleVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize<LargeIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);

template BigIntVal AggregateFunctions::sum_distinct_bigint_finalize<TinyIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::sum_distinct_bigint_finalize<SmallIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::sum_distinct_bigint_finalize<IntVal>(
    FunctionContext* ctx, const StringVal& state_sv);
template BigIntVal AggregateFunctions::sum_distinct_bigint_finalize<BigIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);

template DoubleVal AggregateFunctions::sum_distinct_double_finalize<DoubleVal>(
    FunctionContext* ctx, const StringVal& state_sv);

template LargeIntVal AggregateFunctions::sum_distinct_largeint_finalize<LargeIntVal>(
    FunctionContext* ctx, const StringVal& state_sv);

template void AggregateFunctions::knuth_var_update(
        FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(
        FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(
        FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(
        FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(
        FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(
        FunctionContext*, const DoubleVal&, StringVal*);

template void AggregateFunctions::first_val_update<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::first_val_update<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::first_val_update<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::first_val_update<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::first_val_update<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::first_val_update<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::first_val_update<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::first_val_update<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::first_val_update<DateTimeVal>(
    FunctionContext*, const DateTimeVal& src, DateTimeVal* dst);

template void AggregateFunctions::first_val_rewrite_update<BooleanVal>(
    FunctionContext*, const BooleanVal& src, const BigIntVal&, BooleanVal* dst);
template void AggregateFunctions::first_val_rewrite_update<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, const BigIntVal&, TinyIntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, const BigIntVal&, SmallIntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<IntVal>(
    FunctionContext*, const IntVal& src, const BigIntVal&, IntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<BigIntVal>(
    FunctionContext*, const BigIntVal& src, const BigIntVal&, BigIntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<FloatVal>(
    FunctionContext*, const FloatVal& src, const BigIntVal&, FloatVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DoubleVal>(
    FunctionContext*, const DoubleVal& src, const BigIntVal&, DoubleVal* dst);
template void AggregateFunctions::first_val_rewrite_update<StringVal>(
    FunctionContext*, const StringVal& src, const BigIntVal&, StringVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DateTimeVal>(
    FunctionContext*, const DateTimeVal& src, const BigIntVal&, DateTimeVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DecimalVal>(
    FunctionContext*, const DecimalVal& src, const BigIntVal&, DecimalVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DecimalV2Val>(
    FunctionContext*, const DecimalV2Val& src, const BigIntVal&, DecimalV2Val* dst);

//template void AggregateFunctions::FirstValUpdate<impala::StringValue>(
//    doris_udf::FunctionContext*, impala::StringValue const&, impala::StringValue*);
template void AggregateFunctions::first_val_update<doris_udf::DecimalVal>(
    doris_udf::FunctionContext*, doris_udf::DecimalVal const&, doris_udf::DecimalVal*);

template void AggregateFunctions::first_val_update<doris_udf::DecimalV2Val>(
    doris_udf::FunctionContext*, doris_udf::DecimalV2Val const&, doris_udf::DecimalV2Val*);

template void AggregateFunctions::last_val_update<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::last_val_update<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::last_val_update<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::last_val_update<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::last_val_update<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::last_val_update<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::last_val_update<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::last_val_update<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::last_val_update<DateTimeVal>(
    FunctionContext*, const DateTimeVal& src, DateTimeVal* dst);
template void AggregateFunctions::last_val_update<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::last_val_update<DecimalV2Val>(
    FunctionContext*, const DecimalV2Val& src, DecimalV2Val* dst);

template void AggregateFunctions::last_val_remove<BooleanVal>(
    FunctionContext*, const BooleanVal& src, BooleanVal* dst);
template void AggregateFunctions::last_val_remove<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, TinyIntVal* dst);
template void AggregateFunctions::last_val_remove<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, SmallIntVal* dst);
template void AggregateFunctions::last_val_remove<IntVal>(
    FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::last_val_remove<BigIntVal>(
    FunctionContext*, const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::last_val_remove<FloatVal>(
    FunctionContext*, const FloatVal& src, FloatVal* dst);
template void AggregateFunctions::last_val_remove<DoubleVal>(
    FunctionContext*, const DoubleVal& src, DoubleVal* dst);
template void AggregateFunctions::last_val_remove<StringVal>(
    FunctionContext*, const StringVal& src, StringVal* dst);
template void AggregateFunctions::last_val_remove<DateTimeVal>(
    FunctionContext*, const DateTimeVal& src, DateTimeVal* dst);
template void AggregateFunctions::last_val_remove<DecimalVal>(
    FunctionContext*, const DecimalVal& src, DecimalVal* dst);
template void AggregateFunctions::last_val_remove<DecimalV2Val>(
    FunctionContext*, const DecimalV2Val& src, DecimalV2Val* dst);

template void AggregateFunctions::offset_fn_init<BooleanVal>(
    FunctionContext*, BooleanVal*);
template void AggregateFunctions::offset_fn_init<TinyIntVal>(
    FunctionContext*, TinyIntVal*);
template void AggregateFunctions::offset_fn_init<SmallIntVal>(
    FunctionContext*, SmallIntVal*);
template void AggregateFunctions::offset_fn_init<IntVal>(
    FunctionContext*, IntVal*);
template void AggregateFunctions::offset_fn_init<BigIntVal>(
    FunctionContext*, BigIntVal*);
template void AggregateFunctions::offset_fn_init<FloatVal>(
    FunctionContext*, FloatVal*);
template void AggregateFunctions::offset_fn_init<DoubleVal>(
    FunctionContext*, DoubleVal*);
template void AggregateFunctions::offset_fn_init<StringVal>(
    FunctionContext*, StringVal*);
template void AggregateFunctions::offset_fn_init<DateTimeVal>(
    FunctionContext*, DateTimeVal*);
template void AggregateFunctions::offset_fn_init<DecimalVal>(
    FunctionContext*, DecimalVal*);
template void AggregateFunctions::offset_fn_init<DecimalV2Val>(
    FunctionContext*, DecimalV2Val*);

template void AggregateFunctions::offset_fn_update<BooleanVal>(
    FunctionContext*, const BooleanVal& src, const BigIntVal&, const BooleanVal&,
    BooleanVal* dst);
template void AggregateFunctions::offset_fn_update<TinyIntVal>(
    FunctionContext*, const TinyIntVal& src, const BigIntVal&, const TinyIntVal&,
    TinyIntVal* dst);
template void AggregateFunctions::offset_fn_update<SmallIntVal>(
    FunctionContext*, const SmallIntVal& src, const BigIntVal&, const SmallIntVal&,
    SmallIntVal* dst);
template void AggregateFunctions::offset_fn_update<IntVal>(
    FunctionContext*, const IntVal& src, const BigIntVal&, const IntVal&, IntVal* dst);
template void AggregateFunctions::offset_fn_update<BigIntVal>(
    FunctionContext*, const BigIntVal& src, const BigIntVal&, const BigIntVal&,
    BigIntVal* dst);
template void AggregateFunctions::offset_fn_update<FloatVal>(
    FunctionContext*, const FloatVal& src, const BigIntVal&, const FloatVal&,
    FloatVal* dst);
template void AggregateFunctions::offset_fn_update<DoubleVal>(
    FunctionContext*, const DoubleVal& src, const BigIntVal&, const DoubleVal&,
    DoubleVal* dst);
template void AggregateFunctions::offset_fn_update<StringVal>(
    FunctionContext*, const StringVal& src, const BigIntVal&, const StringVal&,
    StringVal* dst);
template void AggregateFunctions::offset_fn_update<DateTimeVal>(
    FunctionContext*, const DateTimeVal& src, const BigIntVal&, const DateTimeVal&,
    DateTimeVal* dst);
template void AggregateFunctions::offset_fn_update<DecimalVal>(
    FunctionContext*, const DecimalVal& src, const BigIntVal&, const DecimalVal&,
    DecimalVal* dst);
template void AggregateFunctions::offset_fn_update<DecimalV2Val>(
    FunctionContext*, const DecimalV2Val& src, const BigIntVal&, const DecimalV2Val&,
    DecimalV2Val* dst);

template void AggregateFunctions::percentile_approx_update<doris_udf::DoubleVal>(
    FunctionContext* ctx, const doris_udf::DoubleVal&, const doris_udf::DoubleVal&, doris_udf::StringVal*);
}
