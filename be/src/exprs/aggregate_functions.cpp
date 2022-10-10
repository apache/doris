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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/aggregate-functions.cpp
// and modified by Doris

// include aggregate_functions.h first to make sure that all need includes is written in header files
#include "exprs/aggregate_functions.h"

#include <math.h>

#include <sstream>
#include <unordered_set>

#include "common/logging.h"
#include "exprs/anyval_util.h"
#include "exprs/hybrid_set.h"
#include "olap/hll.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/string_value.h"
#include "udf/udf_internal.h"
#include "util/counts.h"
#include "util/tdigest.h"

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
using doris_udf::DecimalV2Val;
using doris_udf::DateTimeVal;
using doris_udf::StringVal;
using doris_udf::AnyVal;

// Delimiter to use if the separator is nullptr.
static const StringVal DEFAULT_STRING_CONCAT_DELIM((uint8_t*)", ", 2);

void AggregateFunctions::init_null(FunctionContext*, AnyVal* dst) {
    dst->is_null = true;
}

template <typename T>
void AggregateFunctions::init_zero_not_null(FunctionContext*, T* dst) {
    dst->is_null = false;
    dst->val = 0;
}

template <>
void AggregateFunctions::init_zero_not_null(FunctionContext*, DecimalV2Val* dst) {
    dst->is_null = false;
    dst->set_to_zero();
}

template <typename T>
void AggregateFunctions::init_zero(FunctionContext*, T* dst) {
    dst->is_null = false;
    dst->val = 0;
}

template <>
void AggregateFunctions::init_zero(FunctionContext*, DecimalV2Val* dst) {
    dst->is_null = false;
    dst->set_to_zero();
}

template <typename T>
void AggregateFunctions::init_zero_null(FunctionContext*, T* dst) {
    dst->is_null = true;
    dst->val = 0;
}

template <>
void AggregateFunctions::init_zero_null(FunctionContext*, DecimalV2Val* dst) {
    dst->is_null = true;
    dst->set_to_zero();
}

template <typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::sum_remove(FunctionContext* ctx, const SRC_VAL& src, DST_VAL* dst) {
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
        init_zero_not_null<DST_VAL>(ctx, dst);
    }
    dst->val -= src.val;
}

template <>
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
        init_zero_not_null<DecimalV2Val>(ctx, dst);
    }

    DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value new_dst = DecimalV2Value::from_decimal_val(*dst);
    new_dst = new_dst - new_src;
    new_dst.to_decimal_val(dst);
}

StringVal AggregateFunctions::string_val_get_value(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return src;
    }
    StringVal result(ctx, src.len);
    memcpy(result.ptr, src.ptr, src.len);
    return result;
}

StringVal AggregateFunctions::string_val_serialize_or_finalize(FunctionContext* ctx,
                                                               const StringVal& src) {
    StringVal result = string_val_get_value(ctx, src);
    if (!src.is_null) {
        ctx->free(src.ptr);
    }
    return result;
}

void AggregateFunctions::count_update(FunctionContext*, const AnyVal& src, BigIntVal* dst) {
    DCHECK(!dst->is_null);

    if (!src.is_null) {
        ++dst->val;
    }
}

void AggregateFunctions::count_merge(FunctionContext*, const BigIntVal& src, BigIntVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    dst->val += src.val;
}

void AggregateFunctions::count_remove(FunctionContext*, const AnyVal& src, BigIntVal* dst) {
    DCHECK(!dst->is_null);
    if (!src.is_null) {
        --dst->val;
        DCHECK_GE(dst->val, 0);
    }
}

struct PercentileState {
    Counts counts;
    double quantile = -1.0;
};

void AggregateFunctions::percentile_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(PercentileState);
    dst->ptr = (uint8_t*)new PercentileState();
}

template <typename T>
void AggregateFunctions::percentile_update(FunctionContext* ctx, const T& src,
                                           const DoubleVal& quantile, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(PercentileState), dst->len);

    PercentileState* percentile = reinterpret_cast<PercentileState*>(dst->ptr);
    percentile->counts.increment(src.val, 1);
    percentile->quantile = quantile.val;
}

void AggregateFunctions::percentile_merge(FunctionContext* ctx, const StringVal& src,
                                          StringVal* dst) {
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(PercentileState), dst->len);

    double quantile;
    memcpy(&quantile, src.ptr, sizeof(double));

    PercentileState* src_percentile = new PercentileState();
    src_percentile->quantile = quantile;
    src_percentile->counts.unserialize(src.ptr + sizeof(double));

    PercentileState* dst_percentile = reinterpret_cast<PercentileState*>(dst->ptr);
    dst_percentile->counts.merge(&src_percentile->counts);
    if (dst_percentile->quantile == -1.0) {
        dst_percentile->quantile = quantile;
    }

    delete src_percentile;
}

StringVal AggregateFunctions::percentile_serialize(FunctionContext* ctx, const StringVal& src) {
    DCHECK(!src.is_null);

    PercentileState* percentile = reinterpret_cast<PercentileState*>(src.ptr);
    uint32_t serialize_size = percentile->counts.serialized_size();
    StringVal result(ctx, sizeof(double) + serialize_size);
    memcpy(result.ptr, &percentile->quantile, sizeof(double));
    percentile->counts.serialize(result.ptr + sizeof(double));

    delete percentile;
    return result;
}

DoubleVal AggregateFunctions::percentile_finalize(FunctionContext* ctx, const StringVal& src) {
    PercentileState* percentile = reinterpret_cast<PercentileState*>(src.ptr);
    double quantile = percentile->quantile;
    auto result = percentile->counts.terminate(quantile);

    delete percentile;
    return result;
}

struct PercentileApproxState {
public:
    PercentileApproxState() : digest(new TDigest()) {}
    PercentileApproxState(double compression) : digest(new TDigest(compression)) {}
    ~PercentileApproxState() { delete digest; }
    static constexpr double INIT_QUANTILE = -1.0;

    TDigest* digest = nullptr;
    double targetQuantile = INIT_QUANTILE;
};

void AggregateFunctions::percentile_approx_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(PercentileApproxState);
    const AnyVal* digest_compression = ctx->get_constant_arg(2);
    if (digest_compression != nullptr) {
        double compression = reinterpret_cast<const DoubleVal*>(digest_compression)->val;
        if (compression >= 2048 && compression <= 10000) {
            dst->ptr = (uint8_t*)new PercentileApproxState(compression);
            return;
        }
    }

    dst->ptr = (uint8_t*)new PercentileApproxState();
};

template <typename T>
void AggregateFunctions::percentile_approx_update(FunctionContext* ctx, const T& src,
                                                  const DoubleVal& quantile, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(PercentileApproxState), dst->len);

    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState*>(dst->ptr);
    percentile->digest->add(src.val);
    percentile->targetQuantile = quantile.val;
}

template <typename T>
void AggregateFunctions::percentile_approx_update(FunctionContext* ctx, const T& src,
                                                  const DoubleVal& quantile,
                                                  const DoubleVal& digest_compression,
                                                  StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(PercentileApproxState), dst->len);

    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState*>(dst->ptr);
    percentile->digest->add(src.val);
    percentile->targetQuantile = quantile.val;
}

StringVal AggregateFunctions::percentile_approx_serialize(FunctionContext* ctx,
                                                          const StringVal& src) {
    DCHECK(!src.is_null);

    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState*>(src.ptr);
    uint32_t serialized_size = percentile->digest->serialized_size();
    StringVal result(ctx, sizeof(double) + serialized_size);
    memcpy(result.ptr, &percentile->targetQuantile, sizeof(double));
    percentile->digest->serialize(result.ptr + sizeof(double));

    delete percentile;
    return result;
}

void AggregateFunctions::percentile_approx_merge(FunctionContext* ctx, const StringVal& src,
                                                 StringVal* dst) {
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(PercentileApproxState), dst->len);

    double quantile;
    memcpy(&quantile, src.ptr, sizeof(double));

    PercentileApproxState* src_percentile = new PercentileApproxState();
    src_percentile->targetQuantile = quantile;
    src_percentile->digest->unserialize(src.ptr + sizeof(double));

    PercentileApproxState* dst_percentile = reinterpret_cast<PercentileApproxState*>(dst->ptr);
    dst_percentile->digest->merge(src_percentile->digest);
    // dst_percentile->targetQuantile only need set once from child result
    // for example:
    //    child result targetQuantile is (0.5, -1), we should set 0.5 once to make sure correct result
    if (dst_percentile->targetQuantile == PercentileApproxState::INIT_QUANTILE) {
        dst_percentile->targetQuantile = quantile;
    }

    delete src_percentile;
}

DoubleVal AggregateFunctions::percentile_approx_finalize(FunctionContext* ctx,
                                                         const StringVal& src) {
    PercentileApproxState* percentile = reinterpret_cast<PercentileApproxState*>(src.ptr);
    double quantile = percentile->targetQuantile;
    double result = percentile->digest->quantile(quantile);

    delete percentile;
    if (isnan(result)) {
        return DoubleVal(result).null();
    } else {
        return DoubleVal(result);
    }
}

struct AvgState {
    double sum = 0;
    int64_t count = 0;
};

struct DecimalV2AvgState {
    DecimalV2Val sum;
    int64_t count = 0;
};

void AggregateFunctions::avg_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(AvgState);
    dst->ptr = ctx->allocate(dst->len);
    new (dst->ptr) AvgState;
}

void AggregateFunctions::decimalv2_avg_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(DecimalV2AvgState);
    // The memory for int128 need to be aligned by 16.
    // So the constructor has been used instead of allocating memory.
    // Also, it will be release in finalize.
    dst->ptr = (uint8_t*)new DecimalV2AvgState;
}

template <typename T>
void AggregateFunctions::avg_update(FunctionContext* ctx, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    avg->sum += src.val;
    ++avg->count;
}

void AggregateFunctions::decimalv2_avg_update(FunctionContext* ctx, const DecimalV2Val& src,
                                              StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(DecimalV2AvgState), dst->len);
    DecimalV2AvgState* avg = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);

    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(avg->sum);
    DecimalV2Value v2 = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value v = v1 + v2;
    v.to_decimal_val(&avg->sum);

    ++avg->count;
}

StringVal AggregateFunctions::decimalv2_avg_serialize(FunctionContext* ctx, const StringVal& src) {
    DCHECK(!src.is_null);
    StringVal result(ctx, src.len);
    memcpy(result.ptr, src.ptr, src.len);
    delete (DecimalV2AvgState*)src.ptr;
    return result;
}

template <typename T>
void AggregateFunctions::avg_remove(FunctionContext* ctx, const T& src, StringVal* dst) {
    // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
    // because Finalize() returns nullptr if count is 0.
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    avg->sum -= src.val;
    --avg->count;
    DCHECK_GE(avg->count, 0);
}

void AggregateFunctions::decimalv2_avg_remove(doris_udf::FunctionContext* ctx,
                                              const DecimalV2Val& src, StringVal* dst) {
    // Remove doesn't need to explicitly check the number of calls to Update() or Remove()
    // because Finalize() returns nullptr if count is 0.
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(DecimalV2AvgState), dst->len);
    DecimalV2AvgState* avg = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);

    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(avg->sum);
    DecimalV2Value v2 = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value v = v1 - v2;
    v.to_decimal_val(&avg->sum);

    --avg->count;
    DCHECK_GE(avg->count, 0);
}

void AggregateFunctions::avg_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    const AvgState* src_struct = reinterpret_cast<const AvgState*>(src.ptr);
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* dst_struct = reinterpret_cast<AvgState*>(dst->ptr);
    dst_struct->sum += src_struct->sum;
    dst_struct->count += src_struct->count;
}

void AggregateFunctions::decimalv2_avg_merge(FunctionContext* ctx, const StringVal& src,
                                             StringVal* dst) {
    DecimalV2AvgState src_struct;
    memcpy(&src_struct, src.ptr, sizeof(DecimalV2AvgState));
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(DecimalV2AvgState), dst->len);
    DecimalV2AvgState* dst_struct = reinterpret_cast<DecimalV2AvgState*>(dst->ptr);

    DecimalV2Value v1 = DecimalV2Value::from_decimal_val(dst_struct->sum);
    DecimalV2Value v2 = DecimalV2Value::from_decimal_val(src_struct.sum);
    DecimalV2Value v = v1 + v2;
    v.to_decimal_val(&dst_struct->sum);
    dst_struct->count += src_struct.count;
}

DoubleVal AggregateFunctions::avg_get_value(FunctionContext* ctx, const StringVal& src) {
    AvgState* val_struct = reinterpret_cast<AvgState*>(src.ptr);
    if (val_struct->count == 0) {
        return DoubleVal::null();
    }
    return DoubleVal(val_struct->sum / val_struct->count);
}

DecimalV2Val AggregateFunctions::decimalv2_avg_get_value(FunctionContext* ctx,
                                                         const StringVal& src) {
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

DecimalV2Val AggregateFunctions::decimalv2_avg_finalize(FunctionContext* ctx,
                                                        const StringVal& src) {
    DecimalV2Val result = decimalv2_avg_get_value(ctx, src);
    delete (DecimalV2AvgState*)src.ptr;
    return result;
}

void AggregateFunctions::timestamp_avg_update(FunctionContext* ctx, const DateTimeVal& src,
                                              StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(AvgState), dst->len);
    AvgState* avg = reinterpret_cast<AvgState*>(dst->ptr);
    double val = DateTimeValue::from_datetime_val(src);
    avg->sum += val;
    ++avg->count;
}

void AggregateFunctions::timestamp_avg_remove(FunctionContext* ctx, const DateTimeVal& src,
                                              StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(dst->ptr != nullptr);
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

DateTimeVal AggregateFunctions::timestamp_avg_finalize(FunctionContext* ctx, const StringVal& src) {
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

template <typename SRC_VAL, typename DST_VAL>
void AggregateFunctions::sum(FunctionContext* ctx, const SRC_VAL& src, DST_VAL* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        init_zero_not_null<DST_VAL>(ctx, dst);
    }
    dst->val += src.val;
}

template <>
void AggregateFunctions::sum(FunctionContext* ctx, const DecimalV2Val& src, DecimalV2Val* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null) {
        init_zero_not_null<DecimalV2Val>(ctx, dst);
    }
    DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value new_dst = DecimalV2Value::from_decimal_val(*dst);
    new_dst = new_dst + new_src;
    new_dst.to_decimal_val(dst);
}

template <typename T>
void AggregateFunctions::min_init(FunctionContext* ctx, T* dst) {
    auto val = AnyValUtil::max_val<T>(ctx);
    // set to null when intermediate slot is nullable
    val.is_null = true;
    *dst = val;
}

template <typename T>
void AggregateFunctions::min(FunctionContext*, const T& src, T* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null || src.val < dst->val) {
        *dst = src;
    }
}

template <typename T>
void AggregateFunctions::max_init(FunctionContext* ctx, T* dst) {
    auto val = AnyValUtil::min_val<T>(ctx);
    // set to null when intermediate slot is nullable
    val.is_null = true;
    *dst = val;
}

template <typename T>
void AggregateFunctions::max(FunctionContext*, const T& src, T* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null || src.val > dst->val) {
        *dst = src;
    }
}

template <typename T>
void AggregateFunctions::any_init(FunctionContext* ctx, T* dst) {
    T val {};
    // set to null when intermediate slot is nullable
    val.is_null = true;
    *dst = val;
}

template <typename T>
void AggregateFunctions::any(FunctionContext*, const T& src, T* dst) {
    if (LIKELY(!dst->is_null || src.is_null)) {
        return;
    }

    *dst = src;
}

template <>
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

template <>
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

void AggregateFunctions::init_null_string(FunctionContext* c, StringVal* dst) {
    dst->is_null = true;
    dst->ptr = nullptr;
    dst->len = 0;
}

template <>
void AggregateFunctions::min(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null || StringValue::from_string_val(src) < StringValue::from_string_val(*dst)) {
        if (!dst->is_null) {
            ctx->free(dst->ptr);
        }
        uint8_t* copy = ctx->allocate(src.len);
        memcpy(copy, src.ptr, src.len);
        *dst = StringVal(copy, src.len);
    }
}

template <>
void AggregateFunctions::max(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }

    if (dst->is_null || StringValue::from_string_val(src) > StringValue::from_string_val(*dst)) {
        if (!dst->is_null) {
            ctx->free(dst->ptr);
        }
        uint8_t* copy = ctx->allocate(src.len);
        memcpy(copy, src.ptr, src.len);
        *dst = StringVal(copy, src.len);
    }
}

template <>
void AggregateFunctions::any(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    if (LIKELY(src.is_null || !dst->is_null)) {
        return;
    }

    uint8_t* copy = ctx->allocate(src.len);
    memcpy(copy, src.ptr, src.len);
    *dst = StringVal(copy, src.len);
}

template <>
void AggregateFunctions::min(FunctionContext*, const DateTimeVal& src, DateTimeVal* dst) {
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

template <>
void AggregateFunctions::max(FunctionContext*, const DateTimeVal& src, DateTimeVal* dst) {
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
    if (src.is_null || separator.is_null) {
        return;
    }

    if (result->is_null) {
        uint8_t* copy = ctx->allocate(src.len);
        memcpy(copy, src.ptr, src.len);
        *result = StringVal(copy, src.len);
        return;
    }

    const StringVal* sep_ptr = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM : &separator;

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
using StringConcatHeader = int64_t;
// Delimiter to use if the separator is nullptr.

void AggregateFunctions::string_concat_update(FunctionContext* ctx, const StringVal& src,
                                              StringVal* result) {
    string_concat_update(ctx, src, DEFAULT_STRING_CONCAT_DELIM, result);
}

void AggregateFunctions::string_concat_update(FunctionContext* ctx, const StringVal& src,
                                              const StringVal& separator, StringVal* result) {
    if (src.is_null || separator.is_null) {
        return;
    }
    const StringVal* sep = separator.is_null ? &DEFAULT_STRING_CONCAT_DELIM : &separator;
    if (result->is_null || !result->ptr) {
        // Header of the intermediate state holds the length of the first separator.
        const auto header_len = sizeof(StringConcatHeader);
        DCHECK(header_len == sizeof(sep->len));
        *result = StringVal(ctx->allocate(header_len), header_len);
        *reinterpret_cast<StringConcatHeader*>(result->ptr) = sep->len;
    }
    result->append(ctx, sep->ptr, sep->len, src.ptr, src.len);
}

void AggregateFunctions::string_concat_merge(FunctionContext* ctx, const StringVal& src,
                                             StringVal* result) {
    if (src.is_null) {
        return;
    }
    const auto header_len = sizeof(StringConcatHeader);
    if (result->is_null || !result->ptr) {
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

StringVal AggregateFunctions::string_concat_finalize(FunctionContext* ctx, const StringVal& src) {
    if (src.is_null) {
        return src;
    }
    const auto header_len = sizeof(StringConcatHeader);
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
const static int NUM_PC_BITMAPS = 64;   // number of bitmaps
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

static inline void set_distinct_estimate_bit(uint8_t* bitmap, uint32_t row_index,
                                             uint32_t bit_index) {
    // We need to convert Bitmap[alpha,index] into the index of the string.
    // alpha tells which of the 32bit we've to jump to.
    // index then lead us to the byte and bit.
    uint32_t* int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
    int_bitmap[row_index] |= (1 << bit_index);
}

static inline bool get_distinct_estimate_bit(uint8_t* bitmap, uint32_t row_index,
                                             uint32_t bit_index) {
    uint32_t* int_bitmap = reinterpret_cast<uint32_t*>(bitmap);
    return ((int_bitmap[row_index] & (1 << bit_index)) > 0);
}

template <typename T>
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

template <typename T>
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

void AggregateFunctions::pc_merge(FunctionContext* c, const StringVal& src, StringVal* dst) {
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

double distinct_estimate_finalize(const StringVal& src) {
    DCHECK(!src.is_null);
    DCHECK_EQ(src.len, NUM_PC_BITMAPS * PC_BITMAP_LENGTH / 8);
    VLOG_ROW << "FinalizeEstimateSlot Bit map:\n" << distinct_estimate_bitmap_to_string(src.ptr);

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
        while (get_distinct_estimate_bit(src.ptr, i, row_bit_count) &&
               row_bit_count < PC_BITMAP_LENGTH) {
            ++row_bit_count;
        }

        sum += row_bit_count;
    }

    double avg = static_cast<double>(sum) / static_cast<double>(NUM_PC_BITMAPS);
    double result = std::pow(static_cast<double>(2), avg) / PC_THETA;
    return result;
}

StringVal AggregateFunctions::pc_finalize(FunctionContext* c, const StringVal& src) {
    double estimate = distinct_estimate_finalize(src);
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
    double estimate = distinct_estimate_finalize(src) * NUM_PC_BITMAPS;
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
    DCHECK_EQ(dst->len, HLL_REGISTERS_COUNT);
    uint64_t hash_value = AnyValUtil::hash64_murmur(src, HashUtil::MURMUR_SEED);

    if (hash_value != 0) {
        int idx = hash_value % dst->len;
        uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;
        dst->ptr[idx] = (dst->ptr[idx] < first_one_bit ? first_one_bit : dst->ptr[idx]);
    }
}

void AggregateFunctions::hll_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    DCHECK_EQ(dst->len, std::pow(2, HLL_COLUMN_PRECISION));
    DCHECK_EQ(src.len, std::pow(2, HLL_COLUMN_PRECISION));

    for (int i = 0; i < src.len; ++i) {
        dst->ptr[i] = (dst->ptr[i] < src.ptr[i] ? src.ptr[i] : dst->ptr[i]);
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

void AggregateFunctions::hll_union_agg_update(FunctionContext* ctx, const HllVal& src,
                                              HllVal* dst) {
    if (src.is_null) {
        return;
    }
    DCHECK(!dst->is_null);

    dst->agg_parse_and_cal(ctx, src);
    return;
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

int64_t AggregateFunctions::hll_algorithm(uint8_t* pdata, int data_len) {
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
    // according to HyperLogLog current correction, if E is cardinal
    // E =< num_streams * 2.5 , LC has higher accuracy.
    // num_streams * 2.5 < E , HyperLogLog has higher accuracy.
    // Generally , we can use HyperLogLog to produce value as E.
    if (estimate <= num_streams * 2.5 && num_zero_registers != 0) {
        // Estimated cardinality is too low. Hll is too inaccurate here, instead use
        // linear counting.
        estimate = num_streams * log(static_cast<float>(num_streams) / num_zero_registers);
    } else if (num_streams == 16384 && estimate < 72000) {
        // when Linear Count change to HyperLoglog according to HyperLogLog Correction,
        // there are relatively large fluctuations, we fixed the problem refer to redis.
        double bias = 5.9119 * 1.0e-18 * (estimate * estimate * estimate * estimate) -
                      1.4253 * 1.0e-12 * (estimate * estimate * estimate) +
                      1.2940 * 1.0e-7 * (estimate * estimate) - 5.2921 * 1.0e-3 * estimate +
                      83.3216;
        estimate -= estimate * (bias / 100);
    }
    return (int64_t)(estimate + 0.5);
}

void AggregateFunctions::hll_raw_agg_init(FunctionContext* ctx, HllVal* dst) {
    hll_union_agg_init(ctx, dst);
}

void AggregateFunctions::hll_raw_agg_update(FunctionContext* ctx, const HllVal& src, HllVal* dst) {
    hll_union_agg_update(ctx, src, dst);
}

void AggregateFunctions::hll_raw_agg_merge(FunctionContext* ctx, const HllVal& src, HllVal* dst) {
    hll_union_agg_merge(ctx, src, dst);
}

doris_udf::HllVal AggregateFunctions::hll_raw_agg_finalize(doris_udf::FunctionContext* ctx,
                                                           const HllVal& src) {
    DCHECK(!src.is_null);
    DCHECK_EQ(src.len, HLL_COLUMN_DEFAULT_LEN);

    HllVal result;
    result.init(ctx);
    memcpy(result.ptr, src.ptr, src.len);
    return result;
}

// TODO chenhao , reduce memory copy
// multi distinct state for numeric
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

    static void destroy(const StringVal& dst) { delete (MultiDistinctNumericState<T>*)dst.ptr; }

    void update(T& t) { _set.insert(t); }

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
    BigIntVal count_finalize() { return BigIntVal(_set.size()); }

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

    FunctionContext::Type set_type() { return _type; }

private:
    class NumericHashHelper {
    public:
        size_t operator()(const T& obj) const {
            size_t result = AnyValUtil::hash64_murmur(obj, HashUtil::MURMUR_SEED);
            return result;
        }
    };

    phmap::flat_hash_set<T, NumericHashHelper> _set;

    // Because Anyval does not provide the hash function, in order
    // to adopt the type different from the template, the pointer is used
    // HybridSetBase* _set;
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

    static void destroy(const StringVal& dst) { delete (MultiDistinctStringCountState*)dst.ptr; }

    void update(StringValue* sv) { _set.insert(sv); }

    StringVal serialize(FunctionContext* ctx) {
        // calculate total serialize buffer length
        int total_serialized_set_length = 1;
        HybridSetBase::IteratorBase* iterator = _set.begin();
        while (iterator->has_next()) {
            const StringValue* value = reinterpret_cast<const StringValue*>(iterator->get_value());
            total_serialized_set_length += STRING_LENGTH_RECORD_LENGTH + value->len;
            iterator->next();
        }
        StringVal result(ctx, total_serialized_set_length);
        uint8_t* writer = result.ptr;
        // type
        *writer = _type;
        writer++;
        iterator = _set.begin();
        while (iterator->has_next()) {
            const StringValue* value = reinterpret_cast<const StringValue*>(iterator->get_value());
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
        reader++;
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
    void merge(MultiDistinctStringCountState& state) { _set.insert(&(state._set)); }

    BigIntVal finalize() { return BigIntVal(_set.size()); }

    FunctionContext::Type set_type() { return _type; }

    static const int STRING_LENGTH_RECORD_LENGTH = 4;

private:
    StringSet _set;
    // _type is serialized into buffer by one byte
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

    static void destroy(const StringVal& dst) { delete (MultiDistinctDecimalV2State*)dst.ptr; }

    void update(DecimalV2Val& t) { _set.insert(DecimalV2Value::from_decimal_val(t)); }

    // type:one byte  value:sizeof(T)
    StringVal serialize(FunctionContext* ctx) {
        const int serialized_set_length = sizeof(uint8_t) + DECIMAL_BYTE_SIZE * _set.size();
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

    FunctionContext::Type set_type() { return _type; }

    // merge set
    void merge(MultiDistinctDecimalV2State& state) {
        _set.insert(state._set.begin(), state._set.end());
    }

    // count
    BigIntVal count_finalize() { return BigIntVal(_set.size()); }

    DecimalV2Val sum_finalize() {
        DecimalV2Value sum(0);
        for (auto& value : _set) {
            sum += value;
        }
        DecimalV2Val result;
        sum.to_decimal_val(&result);
        return result;
    }

private:
    const int DECIMAL_BYTE_SIZE = 16;

    phmap::flat_hash_set<DecimalV2Value> _set;

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

    static void destroy(const StringVal& dst) { delete (MultiDistinctCountDateState*)dst.ptr; }

    void update(DateTimeVal& t) { _set.insert(t); }

    // type:one byte  value:sizeof(T)
    StringVal serialize(FunctionContext* ctx) {
        const int serialized_set_length =
                sizeof(uint8_t) +
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
    BigIntVal count_finalize() { return BigIntVal(_set.size()); }

    FunctionContext::Type set_type() { return _type; }

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

    phmap::flat_hash_set<DateTimeVal, DateTimeHashHelper> _set;

    FunctionContext::Type _type;
};

template <typename T>
void AggregateFunctions::count_or_sum_distinct_numeric_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctNumericState<T>::create(dst);
}

void AggregateFunctions::count_distinct_string_init(FunctionContext* ctx, StringVal* dst) {
    MultiDistinctStringCountState::create(dst);
}

void AggregateFunctions::count_or_sum_distinct_decimalv2_init(FunctionContext* ctx,
                                                              StringVal* dst) {
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
    MultiDistinctStringCountState* state =
            reinterpret_cast<MultiDistinctStringCountState*>(dst->ptr);
    StringValue sv = StringValue::from_string_val(src);
    state->update(&sv);
}

void AggregateFunctions::count_or_sum_distinct_decimalv2_update(FunctionContext* ctx,
                                                                DecimalV2Val& src, StringVal* dst) {
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
    MultiDistinctNumericState<T>* dst_state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctNumericState<T>::create(&src_state_val);
    MultiDistinctNumericState<T>* src_state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctNumericState<T>::destroy(src_state_val);
}

void AggregateFunctions::count_distinct_string_merge(FunctionContext* ctx, StringVal& src,
                                                     StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctStringCountState* dst_state =
            reinterpret_cast<MultiDistinctStringCountState*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctStringCountState::create(&src_state_val);
    MultiDistinctStringCountState* src_state =
            reinterpret_cast<MultiDistinctStringCountState*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctStringCountState::destroy(src_state_val);
}

void AggregateFunctions::count_or_sum_distinct_decimalv2_merge(FunctionContext* ctx, StringVal& src,
                                                               StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctDecimalV2State* dst_state =
            reinterpret_cast<MultiDistinctDecimalV2State*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctDecimalV2State::create(&src_state_val);
    MultiDistinctDecimalV2State* src_state =
            reinterpret_cast<MultiDistinctDecimalV2State*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctDecimalV2State::destroy(src_state_val);
}

void AggregateFunctions::count_distinct_date_merge(FunctionContext* ctx, StringVal& src,
                                                   StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK(!src.is_null);
    MultiDistinctCountDateState* dst_state =
            reinterpret_cast<MultiDistinctCountDateState*>(dst->ptr);
    // unserialize src
    StringVal src_state_val;
    MultiDistinctCountDateState::create(&src_state_val);
    MultiDistinctCountDateState* src_state =
            reinterpret_cast<MultiDistinctCountDateState*>(src_state_val.ptr);
    src_state->unserialize(src);
    DCHECK(dst_state->set_type() == src_state->set_type());
    dst_state->merge(*src_state);
    MultiDistinctCountDateState::destroy(src_state_val);
}

template <typename T>
StringVal AggregateFunctions::count_or_sum_distinct_numeric_serialize(FunctionContext* ctx,
                                                                      const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctNumericState<T>* state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctNumericState<T>::destroy(state_sv);
    return result;
}

StringVal AggregateFunctions::count_distinct_string_serialize(FunctionContext* ctx,
                                                              const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctStringCountState* state =
            reinterpret_cast<MultiDistinctStringCountState*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctStringCountState::destroy(state_sv);
    return result;
}

StringVal AggregateFunctions::count_or_sum_distinct_decimalv2_serialize(FunctionContext* ctx,
                                                                        const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctDecimalV2State* state =
            reinterpret_cast<MultiDistinctDecimalV2State*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctDecimalV2State::destroy(state_sv);
    return result;
}

StringVal AggregateFunctions::count_distinct_date_serialize(FunctionContext* ctx,
                                                            const StringVal& state_sv) {
    DCHECK(!state_sv.is_null);
    MultiDistinctCountDateState* state =
            reinterpret_cast<MultiDistinctCountDateState*>(state_sv.ptr);
    StringVal result = state->serialize(ctx);
    // release original object
    MultiDistinctCountDateState::destroy(state_sv);
    return result;
}

template <typename T>
BigIntVal AggregateFunctions::count_or_sum_distinct_numeric_finalize(FunctionContext* ctx,
                                                                     const StringVal& state_sv) {
    MultiDistinctNumericState<T>* state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctNumericState<T>::destroy(state_sv);
    return result;
}

BigIntVal AggregateFunctions::count_distinct_string_finalize(FunctionContext* ctx,
                                                             const StringVal& state_sv) {
    MultiDistinctStringCountState* state =
            reinterpret_cast<MultiDistinctStringCountState*>(state_sv.ptr);
    BigIntVal result = state->finalize();
    MultiDistinctStringCountState::destroy(state_sv);
    return result;
}

template <typename T>
DoubleVal AggregateFunctions::sum_distinct_double_finalize(FunctionContext* ctx,
                                                           const StringVal& state_sv) {
    MultiDistinctNumericState<T>* state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    DoubleVal result = state->sum_finalize_double();
    MultiDistinctNumericState<T>::destroy(state_sv);
    return result;
}

template <typename T>
LargeIntVal AggregateFunctions::sum_distinct_largeint_finalize(FunctionContext* ctx,
                                                               const StringVal& state_sv) {
    MultiDistinctNumericState<T>* state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    LargeIntVal result = state->sum_finalize_largeint();
    MultiDistinctNumericState<T>::destroy(state_sv);
    return result;
}

template <typename T>
BigIntVal AggregateFunctions::sum_distinct_bigint_finalize(FunctionContext* ctx,
                                                           const StringVal& state_sv) {
    MultiDistinctNumericState<T>* state =
            reinterpret_cast<MultiDistinctNumericState<T>*>(state_sv.ptr);
    BigIntVal result = state->sum_finalize_bigint();
    MultiDistinctNumericState<T>::destroy(state_sv);
    return result;
}

BigIntVal AggregateFunctions::count_distinct_decimalv2_finalize(FunctionContext* ctx,
                                                                const StringVal& state_sv) {
    MultiDistinctDecimalV2State* state =
            reinterpret_cast<MultiDistinctDecimalV2State*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctDecimalV2State::destroy(state_sv);
    return result;
}

DecimalV2Val AggregateFunctions::sum_distinct_decimalv2_finalize(FunctionContext* ctx,
                                                                 const StringVal& state_sv) {
    MultiDistinctDecimalV2State* state =
            reinterpret_cast<MultiDistinctDecimalV2State*>(state_sv.ptr);
    DecimalV2Val result = state->sum_finalize();
    MultiDistinctDecimalV2State::destroy(state_sv);
    return result;
}

BigIntVal AggregateFunctions::count_distinct_date_finalize(FunctionContext* ctx,
                                                           const StringVal& state_sv) {
    MultiDistinctCountDateState* state =
            reinterpret_cast<MultiDistinctCountDateState*>(state_sv.ptr);
    BigIntVal result = state->count_finalize();
    MultiDistinctCountDateState::destroy(state_sv);
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

// Use Decimal to store the intermediate results of the variance algorithm
struct DecimalV2KnuthVarianceState {
    DecimalV2Val mean;
    DecimalV2Val m2;
    int64_t count = 0;
};

// Set pop=true for population variance, false for sample variance
static double compute_knuth_variance(const KnuthVarianceState& state, bool pop) {
    // Return zero for 1 tuple specified by
    // http://docs.oracle.com/cd/B19306_01/server.102/b14200/functions212.htm
    if (state.count == 1) return 0.0;
    if (pop) return state.m2 / state.count;
    return state.m2 / (state.count - 1);
}

// The algorithm is the same as above, using decimal as the intermediate variable
static DecimalV2Value decimalv2_compute_knuth_variance(const DecimalV2KnuthVarianceState& state,
                                                       bool pop) {
    DecimalV2Value new_count = DecimalV2Value();
    if (state.count == 1) return new_count;
    new_count.assign_from_double(state.count);
    DecimalV2Value new_m2 = DecimalV2Value::from_decimal_val(state.m2);
    if (pop)
        return new_m2 / new_count;
    else
        return new_m2 / new_count.assign_from_double(state.count - 1);
}

void AggregateFunctions::knuth_var_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    // TODO(zc)
    dst->len = sizeof(KnuthVarianceState);
    dst->ptr = ctx->allocate(dst->len);
    DCHECK_EQ(dst->len, sizeof(KnuthVarianceState));
    memset(dst->ptr, 0, dst->len);
}

void AggregateFunctions::decimalv2_knuth_var_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(DecimalV2KnuthVarianceState);
    // The memory for int128 need to be aligned by 16.
    // So the constructor has been used instead of allocating memory.
    // Also, it will be release in finalize.
    dst->ptr = (uint8_t*)new DecimalV2KnuthVarianceState;
}

template <typename T>
void AggregateFunctions::knuth_var_update(FunctionContext* ctx, const T& src, StringVal* dst) {
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

template <typename T>
void AggregateFunctions::knuth_var_remove(FunctionContext* context, const T& src, StringVal* dst) {
    if (src.is_null) {
        return;
    }
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(dst->ptr);
    double count = state->count - 1;
    double mean = (state->mean * (count + 1) - src.val) / count;
    double m2 = state->m2 - ((count * (src.val - mean) * (src.val - mean)) / (count + 1));
    state->m2 = m2;
    state->mean = mean;
    state->count = count;
}

void AggregateFunctions::knuth_var_remove(FunctionContext* ctx, const DecimalV2Val& src,
                                          StringVal* dst) {
    if (src.is_null) {
        return;
    }
    DecimalV2KnuthVarianceState* state = reinterpret_cast<DecimalV2KnuthVarianceState*>(dst->ptr);

    DecimalV2Value now_src = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value now_mean = DecimalV2Value::from_decimal_val(state->mean);
    DecimalV2Value now_m2 = DecimalV2Value::from_decimal_val(state->m2);
    DecimalV2Value now_count = DecimalV2Value();
    now_count.assign_from_double(state->count);
    DecimalV2Value now_count_minus = DecimalV2Value();
    now_count_minus.assign_from_double(state->count - 1);

    DecimalV2Value decimal_mean = (now_mean * now_count - now_src) / now_count_minus;
    DecimalV2Value decimal_m2 =
            now_m2 -
            ((now_count_minus * (now_src - decimal_mean) * (now_src - decimal_mean)) / now_count);

    decimal_m2.to_decimal_val(&state->m2);
    decimal_mean.to_decimal_val(&state->mean);
    --state->count;
}

void AggregateFunctions::knuth_var_update(FunctionContext* ctx, const DecimalV2Val& src,
                                          StringVal* dst) {
    DCHECK(!dst->is_null);
    DCHECK_EQ(dst->len, sizeof(DecimalV2KnuthVarianceState));
    if (src.is_null) return;
    DecimalV2KnuthVarianceState* state = reinterpret_cast<DecimalV2KnuthVarianceState*>(dst->ptr);

    DecimalV2Value new_src = DecimalV2Value::from_decimal_val(src);
    DecimalV2Value new_mean = DecimalV2Value::from_decimal_val(state->mean);
    DecimalV2Value new_m2 = DecimalV2Value::from_decimal_val(state->m2);
    DecimalV2Value new_count = DecimalV2Value();
    new_count.assign_from_double(state->count);

    DecimalV2Value temp = DecimalV2Value();
    temp.assign_from_double(1 + state->count);
    DecimalV2Value delta = new_src - new_mean;
    DecimalV2Value r = delta / temp;
    new_mean += r;
    // This may cause Decimal to overflow. When it overflows, m2 will be equal to 9223372036854775807999999999,
    // which is the maximum value that DecimalV2Value can represent. When using double to store the intermediate result m2,
    // it can be expressed by scientific and technical methods and will not overflow.
    // Spark's handling of decimal overflow is to return null or report an error, which can be controlled by parameters.
    // Spark's handling of decimal reference: https://cloud.tencent.com/developer/news/483615
    new_m2 += new_count * delta * r;
    ++state->count;
    new_mean.to_decimal_val(&state->mean);
    new_m2.to_decimal_val(&state->m2);
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

void AggregateFunctions::decimalv2_knuth_var_merge(FunctionContext* ctx, const StringVal& src,
                                                   StringVal* dst) {
    DecimalV2KnuthVarianceState src_state;
    memcpy(&src_state, src.ptr, sizeof(DecimalV2KnuthVarianceState));
    DCHECK(!dst->is_null);
    DCHECK_EQ(dst->len, sizeof(DecimalV2KnuthVarianceState));
    DecimalV2KnuthVarianceState* dst_state =
            reinterpret_cast<DecimalV2KnuthVarianceState*>(dst->ptr);
    if (src_state.count == 0) return;

    DecimalV2Value new_src_mean = DecimalV2Value::from_decimal_val(src_state.mean);
    DecimalV2Value new_dst_mean = DecimalV2Value::from_decimal_val(dst_state->mean);
    DecimalV2Value new_src_count = DecimalV2Value();
    new_src_count.assign_from_double(src_state.count);
    DecimalV2Value new_dst_count = DecimalV2Value();
    new_dst_count.assign_from_double(dst_state->count);
    DecimalV2Value new_src_m2 = DecimalV2Value::from_decimal_val(src_state.m2);
    DecimalV2Value new_dst_m2 = DecimalV2Value::from_decimal_val(dst_state->m2);

    DecimalV2Value delta = new_dst_mean - new_src_mean;
    DecimalV2Value sum_count = new_dst_count + new_src_count;
    new_dst_mean = new_src_mean + delta * (new_dst_count / sum_count);
    new_dst_m2 = (new_src_m2) + new_dst_m2 +
                 (delta * delta) * (new_src_count * new_dst_count / sum_count);
    dst_state->count += src_state.count;
    new_dst_mean.to_decimal_val(&dst_state->mean);
    new_dst_m2.to_decimal_val(&dst_state->m2);
}

DoubleVal AggregateFunctions::knuth_var_get_value(FunctionContext* ctx, const StringVal& state_sv) {
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0 || state->count == 1) {
        return DoubleVal::null();
    }
    double variance = compute_knuth_variance(*state, false);
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_var_finalize(FunctionContext* ctx, const StringVal& state_sv) {
    DoubleVal result = knuth_var_get_value(ctx, state_sv);
    ctx->free(state_sv.ptr);
    return result;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_var_get_value(FunctionContext* ctx,
                                                               const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(DecimalV2KnuthVarianceState));
    DecimalV2KnuthVarianceState* state =
            reinterpret_cast<DecimalV2KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0 || state->count == 1) {
        return DecimalV2Val::null();
    }
    DecimalV2Value variance = decimalv2_compute_knuth_variance(*state, false);
    DecimalV2Val res;
    variance.to_decimal_val(&res);
    return res;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_var_finalize(FunctionContext* ctx,
                                                              const StringVal& state_sv) {
    DecimalV2Val result = decimalv2_knuth_var_get_value(ctx, state_sv);
    delete (DecimalV2KnuthVarianceState*)state_sv.ptr;
    return result;
}

DoubleVal AggregateFunctions::knuth_var_pop_get_value(FunctionContext* ctx,
                                                      const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0) {
        return DoubleVal::null();
    }
    double variance = compute_knuth_variance(*state, true);
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_var_pop_finalize(FunctionContext* ctx,
                                                     const StringVal& state_sv) {
    DoubleVal result = knuth_var_pop_get_value(ctx, state_sv);
    ctx->free(state_sv.ptr);
    return result;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_var_pop_get_value(FunctionContext* ctx,
                                                                   const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(DecimalV2KnuthVarianceState));
    DecimalV2KnuthVarianceState* state =
            reinterpret_cast<DecimalV2KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0) {
        return DecimalV2Val::null();
    }
    DecimalV2Value variance = decimalv2_compute_knuth_variance(*state, true);
    DecimalV2Val res;
    variance.to_decimal_val(&res);
    return res;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_var_pop_finalize(FunctionContext* ctx,
                                                                  const StringVal& state_sv) {
    DecimalV2Val result = decimalv2_knuth_var_pop_get_value(ctx, state_sv);
    delete (DecimalV2KnuthVarianceState*)state_sv.ptr;
    return result;
}

DoubleVal AggregateFunctions::knuth_stddev_get_value(FunctionContext* ctx,
                                                     const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0 || state->count == 1) {
        return DoubleVal::null();
    }
    double variance = sqrt(compute_knuth_variance(*state, false));
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_stddev_finalize(FunctionContext* ctx,
                                                    const StringVal& state_sv) {
    DoubleVal result = knuth_stddev_get_value(ctx, state_sv);
    ctx->free(state_sv.ptr);
    return result;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_stddev_get_value(FunctionContext* ctx,
                                                                  const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(DecimalV2KnuthVarianceState));
    DecimalV2KnuthVarianceState* state =
            reinterpret_cast<DecimalV2KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0 || state->count == 1) {
        return DecimalV2Val::null();
    }
    DecimalV2Value variance = decimalv2_compute_knuth_variance(*state, false);
    variance = DecimalV2Value::sqrt(variance);
    DecimalV2Val res;
    variance.to_decimal_val(&res);
    return res;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_stddev_finalize(FunctionContext* ctx,
                                                                 const StringVal& state_sv) {
    DecimalV2Val result = decimalv2_knuth_stddev_get_value(ctx, state_sv);
    delete (DecimalV2KnuthVarianceState*)state_sv.ptr;
    return result;
}

DoubleVal AggregateFunctions::knuth_stddev_pop_get_value(FunctionContext* ctx,
                                                         const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(KnuthVarianceState));
    KnuthVarianceState* state = reinterpret_cast<KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0) {
        return DoubleVal::null();
    }
    double variance = sqrt(compute_knuth_variance(*state, true));
    return DoubleVal(variance);
}

DoubleVal AggregateFunctions::knuth_stddev_pop_finalize(FunctionContext* ctx,
                                                        const StringVal& state_sv) {
    DoubleVal result = knuth_stddev_pop_get_value(ctx, state_sv);
    ctx->free(state_sv.ptr);
    return result;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_stddev_pop_get_value(FunctionContext* ctx,
                                                                      const StringVal& state_sv) {
    DCHECK_EQ(state_sv.len, sizeof(DecimalV2KnuthVarianceState));
    DecimalV2KnuthVarianceState* state =
            reinterpret_cast<DecimalV2KnuthVarianceState*>(state_sv.ptr);
    if (state->count == 0) {
        return DecimalV2Val::null();
    }
    DecimalV2Value variance = decimalv2_compute_knuth_variance(*state, true);
    variance = DecimalV2Value::sqrt(variance);
    DecimalV2Val res;
    variance.to_decimal_val(&res);
    return res;
}

DecimalV2Val AggregateFunctions::decimalv2_knuth_stddev_pop_finalize(FunctionContext* ctx,
                                                                     const StringVal& state_sv) {
    DecimalV2Val result = decimalv2_knuth_stddev_pop_get_value(ctx, state_sv);
    delete (DecimalV2KnuthVarianceState*)state_sv.ptr;
    return result;
}

struct RankState {
    int64_t rank;
    int64_t count;
    RankState() : rank(1), count(0) {}
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

void AggregateFunctions::dense_rank_update(FunctionContext* ctx, StringVal* dst) {}

BigIntVal AggregateFunctions::rank_get_value(FunctionContext* ctx, StringVal& src_val) {
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

BigIntVal AggregateFunctions::dense_rank_get_value(FunctionContext* ctx, StringVal& src_val) {
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

BigIntVal AggregateFunctions::rank_finalize(FunctionContext* ctx, StringVal& src_val) {
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
    T src = *static_cast<T*>(ctx->get_constant_arg(2));
    // The literal null is sometimes incorrectly converted to int, so *dst = src may cause SEGV
    // if src length is larger than int, for example DatetimeVal
    if (UNLIKELY(src.is_null)) {
        dst->is_null = src.is_null;
    } else {
        *dst = src;
    }
}

template <>
void AggregateFunctions::offset_fn_init(FunctionContext* ctx, StringVal* dst) {
    DCHECK_EQ(ctx->get_num_args(), 3);
    DCHECK(ctx->is_arg_constant(1));
    DCHECK(ctx->is_arg_constant(2));
    DCHECK_EQ(ctx->get_arg_type(0)->type, ctx->get_arg_type(2)->type);
    StringVal src = *static_cast<StringVal*>(ctx->get_constant_arg(2));
    if (src.is_null) {
        *dst = StringVal::null();
    } else {
        uint8_t* copy = ctx->allocate(src.len);
        if (UNLIKELY(copy == nullptr)) {
            *dst = StringVal::null();
        } else {
            *dst = StringVal(copy, src.len);
            memcpy(dst->ptr, src.ptr, src.len);
        }
    }
}

template <typename T>
void AggregateFunctions::offset_fn_update(FunctionContext* ctx, const T& src, const BigIntVal&,
                                          const T& default_value, T* dst) {
    *dst = src;
}

// Refer to AggregateFunctionWindowFunnel.h in https://github.com/ClickHouse/ClickHouse.git
struct WindowFunnelState {
    std::vector<std::pair<DateTimeValue, int>> events;
    int max_event_level;
    bool sorted;
    int64_t window;

    WindowFunnelState() {
        sorted = true;
        max_event_level = 0;
        window = 0;
    }

    void add(DateTimeValue& timestamp, int event_idx, int event_num) {
        max_event_level = event_num;
        if (sorted && events.size() > 0) {
            if (events.back().first == timestamp) {
                sorted = events.back().second <= event_idx;
            } else {
                sorted = events.back().first < timestamp;
            }
        }
        events.emplace_back(timestamp, event_idx);
    }

    void sort() {
        if (sorted) {
            return;
        }
        std::stable_sort(events.begin(), events.end());
    }

    int get_event_level() {
        std::vector<std::optional<DateTimeValue>> events_timestamp(max_event_level);
        for (int64_t i = 0; i < events.size(); i++) {
            int& event_idx = events[i].second;
            DateTimeValue& timestamp = events[i].first;
            if (event_idx == 0) {
                events_timestamp[0] = timestamp;
                continue;
            }
            if (events_timestamp[event_idx - 1].has_value()) {
                DateTimeValue& first_timestamp = events_timestamp[event_idx - 1].value();
                DateTimeValue last_timestamp = first_timestamp;
                TimeInterval interval(SECOND, window, false);
                last_timestamp.date_add_interval(interval, SECOND);

                if (timestamp <= last_timestamp) {
                    events_timestamp[event_idx] = first_timestamp;
                    if (event_idx + 1 == max_event_level) {
                        // Usually, max event level is small.
                        return max_event_level;
                    }
                }
            }
        }

        for (int64_t i = events_timestamp.size() - 1; i >= 0; i--) {
            if (events_timestamp[i].has_value()) {
                return i + 1;
            }
        }

        return 0;
    }

    void merge(WindowFunnelState* other) {
        if (other->events.empty()) {
            return;
        }

        int64_t orig_size = events.size();
        events.insert(std::end(events), std::begin(other->events), std::end(other->events));
        const auto begin = std::begin(events);
        const auto middle = std::next(events.begin(), orig_size);
        const auto end = std::end(events);
        if (!other->sorted) {
            std::stable_sort(middle, end);
        }

        if (!sorted) {
            std::stable_sort(begin, middle);
        }
        std::inplace_merge(begin, middle, end);
        max_event_level = max_event_level > 0 ? max_event_level : other->max_event_level;
        window = window > 0 ? window : other->window;

        sorted = true;
    }

    int64_t serialized_size() {
        return sizeof(int) + sizeof(int64_t) + sizeof(uint64_t) +
               events.size() * (sizeof(int64_t) + sizeof(int));
    }

    void serialize(uint8_t* buf) {
        memcpy(buf, &max_event_level, sizeof(int));
        buf += sizeof(int);
        memcpy(buf, &window, sizeof(int64_t));
        buf += sizeof(int64_t);

        uint64_t event_num = events.size();
        memcpy(buf, &event_num, sizeof(uint64_t));
        buf += sizeof(uint64_t);
        for (int64_t i = 0; i < events.size(); i++) {
            int64_t timestamp = events[i].first;
            int event_idx = events[i].second;
            memcpy(buf, &timestamp, sizeof(int64_t));
            buf += sizeof(int64_t);
            memcpy(buf, &event_idx, sizeof(int));
            buf += sizeof(int);
        }
    }

    void deserialize(uint8_t* buf) {
        uint64_t size;

        memcpy(&max_event_level, buf, sizeof(int));
        buf += sizeof(int);
        memcpy(&window, buf, sizeof(int64_t));
        buf += sizeof(int64_t);
        memcpy(&size, buf, sizeof(uint64_t));
        buf += sizeof(uint64_t);
        for (int64_t i = 0; i < size; i++) {
            int64_t timestamp;
            int event_idx;

            memcpy(&timestamp, buf, sizeof(int64_t));
            buf += sizeof(int64_t);
            memcpy(&event_idx, buf, sizeof(int));
            buf += sizeof(int);
            DateTimeValue time_value;
            time_value.from_date_int64(timestamp);
            add(time_value, event_idx, max_event_level);
        }
    }
};

void AggregateFunctions::window_funnel_init(FunctionContext* ctx, StringVal* dst) {
    dst->is_null = false;
    dst->len = sizeof(WindowFunnelState);
    WindowFunnelState* state = new WindowFunnelState();
    dst->ptr = (uint8_t*)state;
    // constant args at index 0 and 1
    if (ctx->is_arg_constant(0)) {
        BigIntVal* window = reinterpret_cast<BigIntVal*>(ctx->get_constant_arg(0));
        state->window = window->val;
    }
    // TODO handle mode in the future
}

void AggregateFunctions::window_funnel_update(FunctionContext* ctx, const BigIntVal& window,
                                              const StringVal& mode, const DateTimeVal& timestamp,
                                              int num_cond, const BooleanVal* conds,
                                              StringVal* dst) {
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(WindowFunnelState), dst->len);

    if (timestamp.is_null) {
        return;
    }

    WindowFunnelState* state = reinterpret_cast<WindowFunnelState*>(dst->ptr);
    for (int i = 0; i < num_cond; i++) {
        if (conds[i].is_null) {
            continue;
        }
        if (conds[i].val) {
            DateTimeValue time_value = DateTimeValue::from_datetime_val(timestamp);
            state->add(time_value, i, num_cond);
        }
    }
}

StringVal AggregateFunctions::window_funnel_serialize(FunctionContext* ctx, const StringVal& src) {
    WindowFunnelState* state = reinterpret_cast<WindowFunnelState*>(src.ptr);
    int64_t serialized_size = state->serialized_size();
    StringVal result(ctx, sizeof(double) + serialized_size);
    state->serialize(result.ptr);

    delete state;
    return result;
}

void AggregateFunctions::window_funnel_merge(FunctionContext* ctx, const StringVal& src,
                                             StringVal* dst) {
    DCHECK(dst->ptr != nullptr);
    DCHECK_EQ(sizeof(WindowFunnelState), dst->len);
    WindowFunnelState* dst_state = reinterpret_cast<WindowFunnelState*>(dst->ptr);

    WindowFunnelState* src_state = new WindowFunnelState;

    src_state->deserialize(src.ptr);
    dst_state->merge(src_state);
    delete src_state;
}

IntVal AggregateFunctions::window_funnel_finalize(FunctionContext* ctx, const StringVal& src) {
    DCHECK(!src.is_null);

    WindowFunnelState* state = reinterpret_cast<WindowFunnelState*>(src.ptr);
    state->sort();
    int val = state->get_event_level();
    delete state;
    return doris_udf::IntVal(val);
}

// Stamp out the templates for the types we need.
template void AggregateFunctions::init_zero_null<BigIntVal>(FunctionContext*, BigIntVal* dst);
template void AggregateFunctions::init_zero_null<LargeIntVal>(FunctionContext*, LargeIntVal* dst);
template void AggregateFunctions::init_zero_null<DoubleVal>(FunctionContext*, DoubleVal* dst);

// Stamp out the templates for the types we need.
template void AggregateFunctions::init_zero<BigIntVal>(FunctionContext*, BigIntVal* dst);
template void AggregateFunctions::init_zero<LargeIntVal>(FunctionContext*, LargeIntVal* dst);
template void AggregateFunctions::init_zero<DoubleVal>(FunctionContext*, DoubleVal* dst);

template void AggregateFunctions::init_zero_not_null<BigIntVal>(FunctionContext*, BigIntVal* dst);

template void AggregateFunctions::sum_remove<BooleanVal, BigIntVal>(FunctionContext*,
                                                                    const BooleanVal& src,
                                                                    BigIntVal* dst);
template void AggregateFunctions::sum_remove<TinyIntVal, BigIntVal>(FunctionContext*,
                                                                    const TinyIntVal& src,
                                                                    BigIntVal* dst);
template void AggregateFunctions::sum_remove<SmallIntVal, BigIntVal>(FunctionContext*,
                                                                     const SmallIntVal& src,
                                                                     BigIntVal* dst);
template void AggregateFunctions::sum_remove<IntVal, BigIntVal>(FunctionContext*, const IntVal& src,
                                                                BigIntVal* dst);
template void AggregateFunctions::sum_remove<BigIntVal, BigIntVal>(FunctionContext*,
                                                                   const BigIntVal& src,
                                                                   BigIntVal* dst);
template void AggregateFunctions::sum_remove<FloatVal, DoubleVal>(FunctionContext*,
                                                                  const FloatVal& src,
                                                                  DoubleVal* dst);
template void AggregateFunctions::sum_remove<DoubleVal, DoubleVal>(FunctionContext*,
                                                                   const DoubleVal& src,
                                                                   DoubleVal* dst);
template void AggregateFunctions::sum_remove<LargeIntVal, LargeIntVal>(FunctionContext*,
                                                                       const LargeIntVal& src,
                                                                       LargeIntVal* dst);

template void AggregateFunctions::avg_update<doris_udf::BooleanVal>(doris_udf::FunctionContext*,
                                                                    doris_udf::BooleanVal const&,
                                                                    doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::IntVal>(doris_udf::FunctionContext*,
                                                                doris_udf::IntVal const&,
                                                                doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::IntVal>(doris_udf::FunctionContext*,
                                                                doris_udf::IntVal const&,
                                                                doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::BigIntVal>(doris_udf::FunctionContext*,
                                                                   doris_udf::BigIntVal const&,
                                                                   doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::BigIntVal>(doris_udf::FunctionContext*,
                                                                   doris_udf::BigIntVal const&,
                                                                   doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::FloatVal>(doris_udf::FunctionContext*,
                                                                  doris_udf::FloatVal const&,
                                                                  doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::FloatVal>(doris_udf::FunctionContext*,
                                                                  doris_udf::FloatVal const&,
                                                                  doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::DoubleVal>(doris_udf::FunctionContext*,
                                                                   doris_udf::DoubleVal const&,
                                                                   doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::DoubleVal>(doris_udf::FunctionContext*,
                                                                   doris_udf::DoubleVal const&,
                                                                   doris_udf::StringVal*);
//template void AggregateFunctions::AvgUpdate<doris_udf::LargeIntVal>(
//doris_udf::FunctionContext*, doris_udf::LargeIntVal const&, doris_udf::StringVal*);
//template void AggregateFunctions::AvgRemove<doris_udf::LargeIntVal>(
//doris_udf::FunctionContext*, doris_udf::LargeIntVal const&, doris_udf::StringVal*);

template void AggregateFunctions::sum<BooleanVal, BigIntVal>(FunctionContext*,
                                                             const BooleanVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<TinyIntVal, BigIntVal>(FunctionContext*,
                                                             const TinyIntVal& src, BigIntVal* dst);
template void AggregateFunctions::sum<SmallIntVal, BigIntVal>(FunctionContext*,
                                                              const SmallIntVal& src,
                                                              BigIntVal* dst);
template void AggregateFunctions::sum<IntVal, BigIntVal>(FunctionContext*, const IntVal& src,
                                                         BigIntVal* dst);
template void AggregateFunctions::sum<BigIntVal, BigIntVal>(FunctionContext*, const BigIntVal& src,
                                                            BigIntVal* dst);
template void AggregateFunctions::sum<LargeIntVal, LargeIntVal>(FunctionContext*,
                                                                const LargeIntVal& src,
                                                                LargeIntVal* dst);
template void AggregateFunctions::sum<FloatVal, DoubleVal>(FunctionContext*, const FloatVal& src,
                                                           DoubleVal* dst);
template void AggregateFunctions::sum<DoubleVal, DoubleVal>(FunctionContext*, const DoubleVal& src,
                                                            DoubleVal* dst);

template void AggregateFunctions::min_init<BooleanVal>(doris_udf::FunctionContext*,
                                                       BooleanVal* dst);
template void AggregateFunctions::min_init<TinyIntVal>(doris_udf::FunctionContext*,
                                                       TinyIntVal* dst);
template void AggregateFunctions::min_init<SmallIntVal>(doris_udf::FunctionContext*,
                                                        SmallIntVal* dst);
template void AggregateFunctions::min_init<IntVal>(doris_udf::FunctionContext*, IntVal* dst);
template void AggregateFunctions::min_init<BigIntVal>(doris_udf::FunctionContext*, BigIntVal* dst);
template void AggregateFunctions::min_init<LargeIntVal>(doris_udf::FunctionContext*,
                                                        LargeIntVal* dst);
template void AggregateFunctions::min_init<FloatVal>(doris_udf::FunctionContext*, FloatVal* dst);
template void AggregateFunctions::min_init<DoubleVal>(doris_udf::FunctionContext*, DoubleVal* dst);
template void AggregateFunctions::min_init<DateTimeVal>(doris_udf::FunctionContext*,
                                                        DateTimeVal* dst);
template void AggregateFunctions::min_init<DecimalV2Val>(doris_udf::FunctionContext*,
                                                         DecimalV2Val* dst);
template void AggregateFunctions::min_init<StringVal>(doris_udf::FunctionContext*, StringVal* dst);

template void AggregateFunctions::min<BooleanVal>(FunctionContext*, const BooleanVal& src,
                                                  BooleanVal* dst);
template void AggregateFunctions::min<TinyIntVal>(FunctionContext*, const TinyIntVal& src,
                                                  TinyIntVal* dst);
template void AggregateFunctions::min<SmallIntVal>(FunctionContext*, const SmallIntVal& src,
                                                   SmallIntVal* dst);
template void AggregateFunctions::min<IntVal>(FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::min<BigIntVal>(FunctionContext*, const BigIntVal& src,
                                                 BigIntVal* dst);
template void AggregateFunctions::min<LargeIntVal>(FunctionContext*, const LargeIntVal& src,
                                                   LargeIntVal* dst);
template void AggregateFunctions::min<FloatVal>(FunctionContext*, const FloatVal& src,
                                                FloatVal* dst);
template void AggregateFunctions::min<DoubleVal>(FunctionContext*, const DoubleVal& src,
                                                 DoubleVal* dst);

template void AggregateFunctions::avg_remove<doris_udf::BooleanVal>(doris_udf::FunctionContext*,
                                                                    doris_udf::BooleanVal const&,
                                                                    doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::TinyIntVal>(doris_udf::FunctionContext*,
                                                                    doris_udf::TinyIntVal const&,
                                                                    doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::TinyIntVal>(doris_udf::FunctionContext*,
                                                                    doris_udf::TinyIntVal const&,
                                                                    doris_udf::StringVal*);
template void AggregateFunctions::avg_update<doris_udf::SmallIntVal>(doris_udf::FunctionContext*,
                                                                     doris_udf::SmallIntVal const&,
                                                                     doris_udf::StringVal*);
template void AggregateFunctions::avg_remove<doris_udf::SmallIntVal>(doris_udf::FunctionContext*,
                                                                     doris_udf::SmallIntVal const&,
                                                                     doris_udf::StringVal*);

template void AggregateFunctions::max_init<BooleanVal>(doris_udf::FunctionContext*,
                                                       BooleanVal* dst);
template void AggregateFunctions::max_init<TinyIntVal>(doris_udf::FunctionContext*,
                                                       TinyIntVal* dst);
template void AggregateFunctions::max_init<SmallIntVal>(doris_udf::FunctionContext*,
                                                        SmallIntVal* dst);
template void AggregateFunctions::max_init<IntVal>(doris_udf::FunctionContext*, IntVal* dst);
template void AggregateFunctions::max_init<BigIntVal>(doris_udf::FunctionContext*, BigIntVal* dst);
template void AggregateFunctions::max_init<LargeIntVal>(doris_udf::FunctionContext*,
                                                        LargeIntVal* dst);
template void AggregateFunctions::max_init<FloatVal>(doris_udf::FunctionContext*, FloatVal* dst);
template void AggregateFunctions::max_init<DoubleVal>(doris_udf::FunctionContext*, DoubleVal* dst);
template void AggregateFunctions::max_init<DateTimeVal>(doris_udf::FunctionContext*,
                                                        DateTimeVal* dst);
template void AggregateFunctions::max_init<DecimalV2Val>(doris_udf::FunctionContext*,
                                                         DecimalV2Val* dst);
template void AggregateFunctions::max_init<StringVal>(doris_udf::FunctionContext*, StringVal* dst);

template void AggregateFunctions::max<BooleanVal>(FunctionContext*, const BooleanVal& src,
                                                  BooleanVal* dst);
template void AggregateFunctions::max<TinyIntVal>(FunctionContext*, const TinyIntVal& src,
                                                  TinyIntVal* dst);
template void AggregateFunctions::max<SmallIntVal>(FunctionContext*, const SmallIntVal& src,
                                                   SmallIntVal* dst);
template void AggregateFunctions::max<IntVal>(FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::max<BigIntVal>(FunctionContext*, const BigIntVal& src,
                                                 BigIntVal* dst);
template void AggregateFunctions::max<LargeIntVal>(FunctionContext*, const LargeIntVal& src,
                                                   LargeIntVal* dst);
template void AggregateFunctions::max<FloatVal>(FunctionContext*, const FloatVal& src,
                                                FloatVal* dst);
template void AggregateFunctions::max<DoubleVal>(FunctionContext*, const DoubleVal& src,
                                                 DoubleVal* dst);

template void AggregateFunctions::any_init<BooleanVal>(doris_udf::FunctionContext*,
                                                       BooleanVal* dst);
template void AggregateFunctions::any_init<TinyIntVal>(doris_udf::FunctionContext*,
                                                       TinyIntVal* dst);
template void AggregateFunctions::any_init<SmallIntVal>(doris_udf::FunctionContext*,
                                                        SmallIntVal* dst);
template void AggregateFunctions::any_init<IntVal>(doris_udf::FunctionContext*, IntVal* dst);
template void AggregateFunctions::any_init<BigIntVal>(doris_udf::FunctionContext*, BigIntVal* dst);
template void AggregateFunctions::any_init<LargeIntVal>(doris_udf::FunctionContext*,
                                                        LargeIntVal* dst);
template void AggregateFunctions::any_init<FloatVal>(doris_udf::FunctionContext*, FloatVal* dst);
template void AggregateFunctions::any_init<DoubleVal>(doris_udf::FunctionContext*, DoubleVal* dst);
template void AggregateFunctions::any_init<DateTimeVal>(doris_udf::FunctionContext*,
                                                        DateTimeVal* dst);
template void AggregateFunctions::any_init<DecimalV2Val>(doris_udf::FunctionContext*,
                                                         DecimalV2Val* dst);
template void AggregateFunctions::any_init<StringVal>(doris_udf::FunctionContext*, StringVal* dst);

template void AggregateFunctions::any<BooleanVal>(FunctionContext*, const BooleanVal& src,
                                                  BooleanVal* dst);
template void AggregateFunctions::any<TinyIntVal>(FunctionContext*, const TinyIntVal& src,
                                                  TinyIntVal* dst);
template void AggregateFunctions::any<SmallIntVal>(FunctionContext*, const SmallIntVal& src,
                                                   SmallIntVal* dst);
template void AggregateFunctions::any<IntVal>(FunctionContext*, const IntVal& src, IntVal* dst);
template void AggregateFunctions::any<BigIntVal>(FunctionContext*, const BigIntVal& src,
                                                 BigIntVal* dst);
template void AggregateFunctions::any<LargeIntVal>(FunctionContext*, const LargeIntVal& src,
                                                   LargeIntVal* dst);
template void AggregateFunctions::any<FloatVal>(FunctionContext*, const FloatVal& src,
                                                FloatVal* dst);
template void AggregateFunctions::any<DoubleVal>(FunctionContext*, const DoubleVal& src,
                                                 DoubleVal* dst);

template void AggregateFunctions::pc_update(FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::pc_update(FunctionContext*, const DateTimeVal&, StringVal*);

template void AggregateFunctions::pcsa_update(FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::pcsa_update(FunctionContext*, const DateTimeVal&, StringVal*);

template void AggregateFunctions::hll_update(FunctionContext*, const BooleanVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const SmallIntVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const DoubleVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const StringVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const DateTimeVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const LargeIntVal&, StringVal*);
template void AggregateFunctions::hll_update(FunctionContext*, const DecimalV2Val&, StringVal*);

template void AggregateFunctions::count_or_sum_distinct_numeric_init<TinyIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<SmallIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<IntVal>(FunctionContext* ctx,
                                                                             StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<BigIntVal>(
        FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<FloatVal>(FunctionContext* ctx,
                                                                               StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<DoubleVal>(
        FunctionContext* ctx, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_init<LargeIntVal>(
        FunctionContext* ctx, StringVal* dst);

template void AggregateFunctions::count_or_sum_distinct_numeric_update<TinyIntVal>(
        FunctionContext* ctx, TinyIntVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<SmallIntVal>(
        FunctionContext* ctx, SmallIntVal& src, StringVal* dst);
template void AggregateFunctions::count_or_sum_distinct_numeric_update<IntVal>(FunctionContext* ctx,
                                                                               IntVal& src,
                                                                               StringVal* dst);
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
template void AggregateFunctions::count_or_sum_distinct_numeric_merge<IntVal>(FunctionContext* ctx,
                                                                              StringVal& src,
                                                                              StringVal* dst);
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

template void AggregateFunctions::knuth_var_update(FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(FunctionContext*, const SmallIntVal&,
                                                   StringVal*);
template void AggregateFunctions::knuth_var_update(FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::knuth_var_update(FunctionContext*, const DoubleVal&, StringVal*);

template void AggregateFunctions::knuth_var_remove(FunctionContext*, const TinyIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_remove(FunctionContext*, const SmallIntVal&,
                                                   StringVal*);
template void AggregateFunctions::knuth_var_remove(FunctionContext*, const IntVal&, StringVal*);
template void AggregateFunctions::knuth_var_remove(FunctionContext*, const BigIntVal&, StringVal*);
template void AggregateFunctions::knuth_var_remove(FunctionContext*, const FloatVal&, StringVal*);
template void AggregateFunctions::knuth_var_remove(FunctionContext*, const DoubleVal&, StringVal*);

template void AggregateFunctions::first_val_update<BooleanVal>(FunctionContext*,
                                                               const BooleanVal& src,
                                                               BooleanVal* dst);
template void AggregateFunctions::first_val_update<TinyIntVal>(FunctionContext*,
                                                               const TinyIntVal& src,
                                                               TinyIntVal* dst);
template void AggregateFunctions::first_val_update<SmallIntVal>(FunctionContext*,
                                                                const SmallIntVal& src,
                                                                SmallIntVal* dst);
template void AggregateFunctions::first_val_update<IntVal>(FunctionContext*, const IntVal& src,
                                                           IntVal* dst);
template void AggregateFunctions::first_val_update<BigIntVal>(FunctionContext*,
                                                              const BigIntVal& src, BigIntVal* dst);
template void AggregateFunctions::first_val_update<FloatVal>(FunctionContext*, const FloatVal& src,
                                                             FloatVal* dst);
template void AggregateFunctions::first_val_update<DoubleVal>(FunctionContext*,
                                                              const DoubleVal& src, DoubleVal* dst);

template void AggregateFunctions::first_val_update<DateTimeVal>(FunctionContext*,
                                                                const DateTimeVal& src,
                                                                DateTimeVal* dst);

template void AggregateFunctions::first_val_rewrite_update<BooleanVal>(FunctionContext*,
                                                                       const BooleanVal& src,
                                                                       const BigIntVal&,
                                                                       BooleanVal* dst);
template void AggregateFunctions::first_val_rewrite_update<TinyIntVal>(FunctionContext*,
                                                                       const TinyIntVal& src,
                                                                       const BigIntVal&,
                                                                       TinyIntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<SmallIntVal>(FunctionContext*,
                                                                        const SmallIntVal& src,
                                                                        const BigIntVal&,
                                                                        SmallIntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<IntVal>(FunctionContext*,
                                                                   const IntVal& src,
                                                                   const BigIntVal&, IntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<BigIntVal>(FunctionContext*,
                                                                      const BigIntVal& src,
                                                                      const BigIntVal&,
                                                                      BigIntVal* dst);
template void AggregateFunctions::first_val_rewrite_update<FloatVal>(FunctionContext*,
                                                                     const FloatVal& src,
                                                                     const BigIntVal&,
                                                                     FloatVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DoubleVal>(FunctionContext*,
                                                                      const DoubleVal& src,
                                                                      const BigIntVal&,
                                                                      DoubleVal* dst);
template void AggregateFunctions::first_val_rewrite_update<StringVal>(FunctionContext*,
                                                                      const StringVal& src,
                                                                      const BigIntVal&,
                                                                      StringVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DateTimeVal>(FunctionContext*,
                                                                        const DateTimeVal& src,
                                                                        const BigIntVal&,
                                                                        DateTimeVal* dst);
template void AggregateFunctions::first_val_rewrite_update<DecimalV2Val>(FunctionContext*,
                                                                         const DecimalV2Val& src,
                                                                         const BigIntVal&,
                                                                         DecimalV2Val* dst);

template void AggregateFunctions::first_val_update<doris_udf::DecimalV2Val>(
        doris_udf::FunctionContext*, doris_udf::DecimalV2Val const&, doris_udf::DecimalV2Val*);

template void AggregateFunctions::last_val_update<BooleanVal>(FunctionContext*,
                                                              const BooleanVal& src,
                                                              BooleanVal* dst);
template void AggregateFunctions::last_val_update<TinyIntVal>(FunctionContext*,
                                                              const TinyIntVal& src,
                                                              TinyIntVal* dst);
template void AggregateFunctions::last_val_update<SmallIntVal>(FunctionContext*,
                                                               const SmallIntVal& src,
                                                               SmallIntVal* dst);
template void AggregateFunctions::last_val_update<IntVal>(FunctionContext*, const IntVal& src,
                                                          IntVal* dst);
template void AggregateFunctions::last_val_update<BigIntVal>(FunctionContext*, const BigIntVal& src,
                                                             BigIntVal* dst);
template void AggregateFunctions::last_val_update<FloatVal>(FunctionContext*, const FloatVal& src,
                                                            FloatVal* dst);
template void AggregateFunctions::last_val_update<DoubleVal>(FunctionContext*, const DoubleVal& src,
                                                             DoubleVal* dst);
template void AggregateFunctions::last_val_update<DateTimeVal>(FunctionContext*,
                                                               const DateTimeVal& src,
                                                               DateTimeVal* dst);
template void AggregateFunctions::last_val_update<DecimalV2Val>(FunctionContext*,
                                                                const DecimalV2Val& src,
                                                                DecimalV2Val* dst);

template void AggregateFunctions::last_val_remove<BooleanVal>(FunctionContext*,
                                                              const BooleanVal& src,
                                                              BooleanVal* dst);
template void AggregateFunctions::last_val_remove<TinyIntVal>(FunctionContext*,
                                                              const TinyIntVal& src,
                                                              TinyIntVal* dst);
template void AggregateFunctions::last_val_remove<SmallIntVal>(FunctionContext*,
                                                               const SmallIntVal& src,
                                                               SmallIntVal* dst);
template void AggregateFunctions::last_val_remove<IntVal>(FunctionContext*, const IntVal& src,
                                                          IntVal* dst);
template void AggregateFunctions::last_val_remove<BigIntVal>(FunctionContext*, const BigIntVal& src,
                                                             BigIntVal* dst);
template void AggregateFunctions::last_val_remove<FloatVal>(FunctionContext*, const FloatVal& src,
                                                            FloatVal* dst);
template void AggregateFunctions::last_val_remove<DoubleVal>(FunctionContext*, const DoubleVal& src,
                                                             DoubleVal* dst);
template void AggregateFunctions::last_val_remove<DateTimeVal>(FunctionContext*,
                                                               const DateTimeVal& src,
                                                               DateTimeVal* dst);
template void AggregateFunctions::last_val_remove<DecimalV2Val>(FunctionContext*,
                                                                const DecimalV2Val& src,
                                                                DecimalV2Val* dst);

template void AggregateFunctions::offset_fn_init<BooleanVal>(FunctionContext*, BooleanVal*);
template void AggregateFunctions::offset_fn_init<TinyIntVal>(FunctionContext*, TinyIntVal*);
template void AggregateFunctions::offset_fn_init<SmallIntVal>(FunctionContext*, SmallIntVal*);
template void AggregateFunctions::offset_fn_init<IntVal>(FunctionContext*, IntVal*);
template void AggregateFunctions::offset_fn_init<BigIntVal>(FunctionContext*, BigIntVal*);
template void AggregateFunctions::offset_fn_init<FloatVal>(FunctionContext*, FloatVal*);
template void AggregateFunctions::offset_fn_init<DoubleVal>(FunctionContext*, DoubleVal*);
template void AggregateFunctions::offset_fn_init<DateTimeVal>(FunctionContext*, DateTimeVal*);
template void AggregateFunctions::offset_fn_init<DecimalV2Val>(FunctionContext*, DecimalV2Val*);

template void AggregateFunctions::offset_fn_update<BooleanVal>(FunctionContext*,
                                                               const BooleanVal& src,
                                                               const BigIntVal&, const BooleanVal&,
                                                               BooleanVal* dst);
template void AggregateFunctions::offset_fn_update<TinyIntVal>(FunctionContext*,
                                                               const TinyIntVal& src,
                                                               const BigIntVal&, const TinyIntVal&,
                                                               TinyIntVal* dst);
template void AggregateFunctions::offset_fn_update<SmallIntVal>(FunctionContext*,
                                                                const SmallIntVal& src,
                                                                const BigIntVal&,
                                                                const SmallIntVal&,
                                                                SmallIntVal* dst);
template void AggregateFunctions::offset_fn_update<IntVal>(FunctionContext*, const IntVal& src,
                                                           const BigIntVal&, const IntVal&,
                                                           IntVal* dst);
template void AggregateFunctions::offset_fn_update<BigIntVal>(FunctionContext*,
                                                              const BigIntVal& src,
                                                              const BigIntVal&, const BigIntVal&,
                                                              BigIntVal* dst);
template void AggregateFunctions::offset_fn_update<FloatVal>(FunctionContext*, const FloatVal& src,
                                                             const BigIntVal&, const FloatVal&,
                                                             FloatVal* dst);
template void AggregateFunctions::offset_fn_update<DoubleVal>(FunctionContext*,
                                                              const DoubleVal& src,
                                                              const BigIntVal&, const DoubleVal&,
                                                              DoubleVal* dst);
template void AggregateFunctions::offset_fn_update<StringVal>(FunctionContext*,
                                                              const StringVal& src,
                                                              const BigIntVal&, const StringVal&,
                                                              StringVal* dst);
template void AggregateFunctions::offset_fn_update<DateTimeVal>(FunctionContext*,
                                                                const DateTimeVal& src,
                                                                const BigIntVal&,
                                                                const DateTimeVal&,
                                                                DateTimeVal* dst);
template void AggregateFunctions::offset_fn_update<DecimalV2Val>(FunctionContext*,
                                                                 const DecimalV2Val& src,
                                                                 const BigIntVal&,
                                                                 const DecimalV2Val&,
                                                                 DecimalV2Val* dst);

template void AggregateFunctions::percentile_update<BigIntVal>(FunctionContext* ctx,
                                                               const BigIntVal&, const DoubleVal&,
                                                               StringVal*);

template void AggregateFunctions::percentile_approx_update<doris_udf::DoubleVal>(
        FunctionContext* ctx, const doris_udf::DoubleVal&, const doris_udf::DoubleVal&,
        doris_udf::StringVal*);

template void AggregateFunctions::percentile_approx_update<doris_udf::DoubleVal>(
        FunctionContext* ctx, const doris_udf::DoubleVal&, const doris_udf::DoubleVal&,
        const doris_udf::DoubleVal&, doris_udf::StringVal*);

} // namespace doris
