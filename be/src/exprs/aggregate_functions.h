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
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/aggregate-functions.h
// and modified by Doris

#pragma once

#include "udf/udf.h"

namespace doris {

class HllSetResolver;
class HybridSetBase;

// Collection of builtin aggregate functions. Aggregate functions implement
// the various phases of the aggregation: Init(), Update(), Serialize(), Merge(),
// and Finalize(). Not all functions need to implement all of the steps and
// some of the parts can be reused across different aggregate functions.
// This functions are implemented using the UDA interface.

class AggregateFunctions {
public:
    // Initializes dst to nullptr.
    static void init_null(doris_udf::FunctionContext*, doris_udf::AnyVal* dst);
    // Initializes dst to nullptr and sets dst->ptr to nullptr.
    static void init_null_string(doris_udf::FunctionContext* c, doris_udf::StringVal* dst);

    // Initializes dst to 0 and is_null = true.
    template <typename T>
    static void init_zero(doris_udf::FunctionContext*, T* dst);

    // Initializes dst to 0 and is_null = true.
    template <typename T>
    static void init_zero_null(doris_udf::FunctionContext*, T* dst);

    // Initializes dst to 0.
    template <typename T>
    static void init_zero_not_null(doris_udf::FunctionContext*, T* dst);

    template <typename SRC_VAL, typename DST_VAL>
    static void sum_remove(doris_udf::FunctionContext* ctx, const SRC_VAL& src, DST_VAL* dst);

    // doris_udf::StringVal GetValue() function that returns a copy of src
    static doris_udf::StringVal string_val_get_value(doris_udf::FunctionContext* ctx,
                                                     const doris_udf::StringVal& src);
    static doris_udf::StringVal string_val_serialize_or_finalize(doris_udf::FunctionContext* ctx,
                                                                 const doris_udf::StringVal& src);

    // Implementation of Count and Count(*)
    static void count_update(doris_udf::FunctionContext*, const doris_udf::AnyVal& src,
                             doris_udf::BigIntVal* dst);
    static void count_merge(doris_udf::FunctionContext*, const doris_udf::BigIntVal& src,
                            doris_udf::BigIntVal* dst);
    static void count_remove(doris_udf::FunctionContext*, const doris_udf::AnyVal& src,
                             doris_udf::BigIntVal* dst);
    static void count_star_update(doris_udf::FunctionContext*, doris_udf::BigIntVal* dst);

    static void count_star_remove(FunctionContext*, BigIntVal* dst);

    // Implementation of percentile
    static void percentile_init(FunctionContext* ctx, StringVal* dst);

    template <typename T>
    static void percentile_update(FunctionContext* ctx, const T& src, const DoubleVal& quantile,
                                  StringVal* dst);

    static void percentile_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst);

    static StringVal percentile_serialize(FunctionContext* ctx, const StringVal& state_sv);

    static DoubleVal percentile_finalize(FunctionContext* ctx, const StringVal& src);

    // Implementation of percentile_approx
    static void percentile_approx_init(doris_udf::FunctionContext* ctx, doris_udf::StringVal* dst);

    template <typename T>
    static void percentile_approx_update(FunctionContext* ctx, const T& src,
                                         const DoubleVal& quantile, StringVal* dst);

    template <typename T>
    static void percentile_approx_update(FunctionContext* ctx, const T& src,
                                         const DoubleVal& quantile,
                                         const DoubleVal& digest_compression, StringVal* dst);

    static void percentile_approx_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst);

    static DoubleVal percentile_approx_finalize(FunctionContext* ctx, const StringVal& src);

    static StringVal percentile_approx_serialize(FunctionContext* ctx, const StringVal& state_sv);

    // Implementation of Avg.
    // TODO: Change this to use a fixed-sized BufferVal as intermediate type.
    static void avg_init(doris_udf::FunctionContext* ctx, doris_udf::StringVal* dst);
    template <typename T>
    static void avg_update(doris_udf::FunctionContext* ctx, const T& src,
                           doris_udf::StringVal* dst);
    template <typename T>
    static void avg_remove(doris_udf::FunctionContext* ctx, const T& src,
                           doris_udf::StringVal* dst);
    static void avg_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static doris_udf::DoubleVal avg_get_value(doris_udf::FunctionContext* ctx,
                                              const doris_udf::StringVal& val);
    static doris_udf::DoubleVal avg_finalize(doris_udf::FunctionContext* ctx,
                                             const doris_udf::StringVal& val);

    // Avg for timestamp. Uses avg_init() and AvgMerge().
    static void timestamp_avg_update(doris_udf::FunctionContext* ctx,
                                     const doris_udf::DateTimeVal& src, doris_udf::StringVal* dst);
    static void timestamp_avg_remove(doris_udf::FunctionContext* ctx,
                                     const doris_udf::DateTimeVal& src, doris_udf::StringVal* dst);
    static doris_udf::DateTimeVal timestamp_avg_get_value(doris_udf::FunctionContext* ctx,
                                                          const doris_udf::StringVal& val);
    static doris_udf::DateTimeVal timestamp_avg_finalize(doris_udf::FunctionContext* ctx,
                                                         const doris_udf::StringVal& val);

    // Avg for decimals.
    static void decimalv2_avg_init(doris_udf::FunctionContext* ctx, doris_udf::StringVal* dst);
    static void decimalv2_avg_update(doris_udf::FunctionContext* ctx,
                                     const doris_udf::DecimalV2Val& src, doris_udf::StringVal* dst);
    static void decimalv2_avg_merge(FunctionContext* ctx, const doris_udf::StringVal& src,
                                    doris_udf::StringVal* dst);
    static doris_udf::StringVal decimalv2_avg_serialize(doris_udf::FunctionContext* ctx,
                                                        const doris_udf::StringVal& src);
    static void decimalv2_avg_remove(doris_udf::FunctionContext* ctx,
                                     const doris_udf::DecimalV2Val& src, doris_udf::StringVal* dst);

    static doris_udf::DecimalV2Val decimalv2_avg_get_value(doris_udf::FunctionContext* ctx,
                                                           const doris_udf::StringVal& val);
    static doris_udf::DecimalV2Val decimalv2_avg_finalize(doris_udf::FunctionContext* ctx,
                                                          const doris_udf::StringVal& val);
    // SumUpdate, SumMerge
    template <typename SRC_VAL, typename DST_VAL>
    static void sum(doris_udf::FunctionContext*, const SRC_VAL& src, DST_VAL* dst);

    // MinInit
    template <typename T>
    static void min_init(doris_udf::FunctionContext*, T* dst);

    // MinUpdate/MinMerge
    template <typename T>
    static void min(doris_udf::FunctionContext*, const T& src, T* dst);

    // MaxInit
    template <typename T>
    static void max_init(doris_udf::FunctionContext*, T* dst);

    // MaxUpdate/MaxMerge
    template <typename T>
    static void max(doris_udf::FunctionContext*, const T& src, T* dst);

    // AnyInit
    template <typename T>
    static void any_init(doris_udf::FunctionContext*, T* dst);

    // AnyUpdate/AnyMerge
    template <typename T>
    static void any(doris_udf::FunctionContext*, const T& src, T* dst);

    // String concat
    static void string_concat(doris_udf::FunctionContext*, const doris_udf::StringVal& src,
                              const doris_udf::StringVal& separator, doris_udf::StringVal* result);

    /// String concat
    static void string_concat_update(FunctionContext*, const StringVal& src, StringVal* result);
    static void string_concat_update(FunctionContext*, const StringVal& src,
                                     const StringVal& separator, StringVal* result);
    static void string_concat_merge(FunctionContext*, const StringVal& src, StringVal* result);
    static StringVal string_concat_finalize(FunctionContext*, const StringVal& src);

    // Probabilistic Counting (PC), a distinct estimate algorithms.
    // Probabilistic Counting with Stochastic Averaging (PCSA) is a variant
    // of PC that runs faster and usually gets equally accurate results.
    static void pc_init(doris_udf::FunctionContext*, doris_udf::StringVal* slot);

    template <typename T>
    static void pc_update(doris_udf::FunctionContext*, const T& src, doris_udf::StringVal* dst);
    template <typename T>
    static void pcsa_update(doris_udf::FunctionContext*, const T& src, doris_udf::StringVal* dst);

    static void pc_merge(doris_udf::FunctionContext*, const doris_udf::StringVal& src,
                         doris_udf::StringVal* dst);

    static doris_udf::StringVal pc_finalize(doris_udf::FunctionContext*,
                                            const doris_udf::StringVal& src);

    static doris_udf::StringVal pcsa_finalize(doris_udf::FunctionContext*,
                                              const doris_udf::StringVal& src);

    // count and sum distinct algorithm in multi distinct
    template <typename T>
    static void count_or_sum_distinct_numeric_init(doris_udf::FunctionContext* ctx,
                                                   doris_udf::StringVal* dst);
    template <typename T>
    static void count_or_sum_distinct_numeric_update(FunctionContext* ctx, T& src, StringVal* dst);
    template <typename T>
    static void count_or_sum_distinct_numeric_merge(FunctionContext* ctx, StringVal& src,
                                                    StringVal* dst);
    template <typename T>
    static StringVal count_or_sum_distinct_numeric_serialize(FunctionContext* ctx,
                                                             const StringVal& state_sv);
    template <typename T>
    static BigIntVal count_or_sum_distinct_numeric_finalize(FunctionContext* ctx,
                                                            const StringVal& state_sv);

    // count distinct in multi distinct for string
    static void count_distinct_string_init(doris_udf::FunctionContext* ctx,
                                           doris_udf::StringVal* dst);
    static void count_distinct_string_update(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static void count_distinct_string_merge(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static StringVal count_distinct_string_serialize(FunctionContext* ctx,
                                                     const StringVal& state_sv);
    static BigIntVal count_distinct_string_finalize(FunctionContext* ctx,
                                                    const StringVal& state_sv);

    // count distinct in multi distinct for decimal
    static void count_or_sum_distinct_decimal_init(doris_udf::FunctionContext* ctx,
                                                   doris_udf::StringVal* dst);
    static void count_or_sum_distinct_decimalv2_init(doris_udf::FunctionContext* ctx,
                                                     doris_udf::StringVal* dst);
    static void count_or_sum_distinct_decimalv2_update(FunctionContext* ctx, DecimalV2Val& src,
                                                       StringVal* dst);
    static void count_or_sum_distinct_decimal_merge(FunctionContext* ctx, StringVal& src,
                                                    StringVal* dst);
    static void count_or_sum_distinct_decimalv2_merge(FunctionContext* ctx, StringVal& src,
                                                      StringVal* dst);

    static StringVal count_or_sum_distinct_decimalv2_serialize(FunctionContext* ctx,
                                                               const StringVal& state_sv);

    static BigIntVal count_distinct_decimalv2_finalize(FunctionContext* ctx,
                                                       const StringVal& state_sv);
    static DecimalV2Val sum_distinct_decimalv2_finalize(FunctionContext* ctx,
                                                        const StringVal& state_sv);

    // count distinct in multi distinct for Date
    static void count_distinct_date_init(doris_udf::FunctionContext* ctx,
                                         doris_udf::StringVal* dst);
    static void count_distinct_date_update(FunctionContext* ctx, DateTimeVal& src, StringVal* dst);
    static void count_distinct_date_merge(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static StringVal count_distinct_date_serialize(FunctionContext* ctx, const StringVal& state_sv);
    static BigIntVal count_distinct_date_finalize(FunctionContext* ctx, const StringVal& state_sv);

    template <typename T>
    static BigIntVal sum_distinct_bigint_finalize(FunctionContext* ctx, const StringVal& state_sv);
    template <typename T>
    static LargeIntVal sum_distinct_largeint_finalize(FunctionContext* ctx,
                                                      const StringVal& state_sv);
    template <typename T>
    static DoubleVal sum_distinct_double_finalize(FunctionContext* ctx, const StringVal& state_sv);

    /// Knuth's variance algorithm, more numerically stable than canonical stddev
    /// algorithms; reference implementation:
    /// http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    static void knuth_var_init(FunctionContext* context, StringVal* val);
    template <typename T>
    static void knuth_var_update(FunctionContext* context, const T& input, StringVal* val);
    template <typename T>
    static void knuth_var_remove(FunctionContext* context, const T& src, StringVal* dst);
    static void knuth_var_merge(FunctionContext* context, const StringVal& src, StringVal* dst);
    static DoubleVal knuth_var_finalize(FunctionContext* context, const StringVal& val);

    /// Calculates the biased variance, uses KnuthVar Init-Update-Merge functions
    static DoubleVal knuth_var_pop_finalize(FunctionContext* context, const StringVal& val);

    /// Calculates STDDEV, uses KnuthVar Init-Update-Merge functions
    static DoubleVal knuth_stddev_finalize(FunctionContext* context, const StringVal& val);

    /// Calculates the biased STDDEV, uses KnuthVar Init-Update-Merge functions
    static DoubleVal knuth_stddev_pop_finalize(FunctionContext* context, const StringVal& val);

    static DoubleVal knuth_var_get_value(FunctionContext* ctx, const StringVal& state_sv);
    static DoubleVal knuth_var_pop_get_value(FunctionContext* context, const StringVal& val);
    static DoubleVal knuth_stddev_get_value(FunctionContext* ctx, const StringVal& state_sv);
    static DoubleVal knuth_stddev_pop_get_value(FunctionContext* context, const StringVal& val);

    // variance/stddev for decimals.
    static void decimalv2_knuth_var_init(FunctionContext* context, StringVal* val);
    static void knuth_var_remove(FunctionContext* ctx, const DecimalV2Val& src, StringVal* dst);
    static void knuth_var_update(FunctionContext* context, const DecimalV2Val& src, StringVal* val);
    static void decimalv2_knuth_var_merge(FunctionContext* context, const StringVal& src,
                                          StringVal* val);
    static DecimalV2Val decimalv2_knuth_var_finalize(FunctionContext* context,
                                                     const StringVal& val);
    static DecimalV2Val decimalv2_knuth_var_pop_finalize(FunctionContext* context,
                                                         const StringVal& val);
    static DecimalV2Val decimalv2_knuth_stddev_finalize(FunctionContext* context,
                                                        const StringVal& val);
    static DecimalV2Val decimalv2_knuth_stddev_pop_finalize(FunctionContext* context,
                                                            const StringVal& val);

    static DecimalV2Val decimalv2_knuth_var_get_value(FunctionContext* ctx,
                                                      const StringVal& state_sv);
    static DecimalV2Val decimalv2_knuth_var_pop_get_value(FunctionContext* context,
                                                          const StringVal& val);
    static DecimalV2Val decimalv2_knuth_stddev_get_value(FunctionContext* context,
                                                         const StringVal& val);
    static DecimalV2Val decimalv2_knuth_stddev_pop_get_value(FunctionContext* context,
                                                             const StringVal& val);

    /// ----------------------------- Analytic Functions ---------------------------------
    /// Analytic functions implement the UDA interface (except Merge(), Serialize()) and are
    /// used internally by the AnalyticEvalNode. Some analytic functions store intermediate
    /// state as a StringVal which is needed for multiple calls to Finalize(), so some fns
    /// also implement a (private) GetValue() method to just return the value. In that
    /// case, Finalize() is only called at the end to clean up.

    // Initializes the state for RANK and DENSE_RANK
    static void rank_init(doris_udf::FunctionContext*, doris_udf::StringVal* slot);

    // Update state for RANK
    static void rank_update(doris_udf::FunctionContext*, doris_udf::StringVal* dst);

    // Update state for DENSE_RANK
    static void dense_rank_update(doris_udf::FunctionContext*, doris_udf::StringVal* dst);

    // Returns the result for RANK and prepares the state for the next Update().
    static doris_udf::BigIntVal rank_get_value(doris_udf::FunctionContext*,
                                               doris_udf::StringVal& src);

    // Returns the result for DENSE_RANK and prepares the state for the next Update().
    // TODO: Implement DENSE_RANK with a single doris_udf::BigIntVal. Requires src can be modified,
    // AggFnEvaluator would need to handle copying the src doris_udf::AnyVal back into the src slot.
    static doris_udf::BigIntVal dense_rank_get_value(doris_udf::FunctionContext*,
                                                     doris_udf::StringVal& src);

    // Returns the result for RANK and DENSE_RANK and cleans up intermediate state in src.
    static doris_udf::BigIntVal rank_finalize(doris_udf::FunctionContext*,
                                              doris_udf::StringVal& src);

    // Implements LAST_VALUE.
    template <typename T>
    static void last_val_update(doris_udf::FunctionContext*, const T& src, T* dst);
    template <typename T>
    static void last_val_remove(doris_udf::FunctionContext*, const T& src, T* dst);

    // Implements FIRST_VALUE.
    template <typename T>
    static void first_val_update(doris_udf::FunctionContext*, const T& src, T* dst);
    // Implements FIRST_VALUE for some windows that require rewrites during planning.
    // The doris_udf::BigIntVal is unused by first_val_rewrite_update() (it is used by the
    // AnalyticEvalNode).
    template <typename T>
    static void first_val_rewrite_update(doris_udf::FunctionContext*, const T& src,
                                         const doris_udf::BigIntVal&, T* dst);

    // OffsetFn*() implement LAG and LEAD. Init() sets the default value (the last
    // constant parameter) as dst.
    template <typename T>
    static void offset_fn_init(doris_udf::FunctionContext*, T* dst);

    // Update() takes all the parameters to LEAD/LAG, including the integer offset and
    // the default value, neither which are needed by Update(). (The offset is already
    // used in the window for the analytic fn evaluation and the default value is set
    // in Init().
    template <typename T>
    static void offset_fn_update(doris_udf::FunctionContext*, const T& src,
                                 const doris_udf::BigIntVal&, const T&, T* dst);

    // windowFunnel
    static void window_funnel_init(FunctionContext* ctx, StringVal* dst);
    static void window_funnel_update(FunctionContext* ctx, const BigIntVal& window,
                                     const StringVal& mode, const DateTimeVal& timestamp,
                                     int num_cond, const BooleanVal* conds, StringVal* dst);
    static void window_funnel_merge(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static StringVal window_funnel_serialize(FunctionContext* ctx, const StringVal& src);
    static IntVal window_funnel_finalize(FunctionContext* ctx, const StringVal& src);

    // todo(kks): keep following HLL methods only for backward compatibility, we should remove these methods
    //            when doris 0.12 release
    static void hll_init(doris_udf::FunctionContext*, doris_udf::StringVal* slot);
    template <typename T>
    static void hll_update(doris_udf::FunctionContext*, const T& src, doris_udf::StringVal* dst);
    static void hll_merge(doris_udf::FunctionContext*, const doris_udf::StringVal& src,
                          doris_udf::StringVal* dst);
    static doris_udf::StringVal hll_finalize(doris_udf::FunctionContext*,
                                             const doris_udf::StringVal& src);

    static void hll_union_agg_init(doris_udf::FunctionContext*, doris_udf::HllVal* slot);
    // fill all register according to hll set type
    static void hll_union_agg_update(doris_udf::FunctionContext*, const doris_udf::HllVal& src,
                                     doris_udf::HllVal* dst);
    // merge the register value
    static void hll_union_agg_merge(doris_udf::FunctionContext*, const doris_udf::HllVal& src,
                                    doris_udf::HllVal* dst);
    // return result
    static doris_udf::BigIntVal hll_union_agg_finalize(doris_udf::FunctionContext*,
                                                       const doris_udf::HllVal& src);

    // calculate result
    static int64_t hll_algorithm(uint8_t* pdata, int data_len);
    static int64_t hll_algorithm(const StringVal& dst) { return hll_algorithm(dst.ptr, dst.len); }
    static int64_t hll_algorithm(const HllVal& dst) {
        return hll_algorithm(dst.ptr + 1, dst.len - 1);
    }

    //  HLL value type aggregate to HLL value type
    static void hll_raw_agg_init(doris_udf::FunctionContext*, doris_udf::HllVal* slot);
    static void hll_raw_agg_update(doris_udf::FunctionContext*, const doris_udf::HllVal& src,
                                   doris_udf::HllVal* dst);
    static void hll_raw_agg_merge(doris_udf::FunctionContext*, const doris_udf::HllVal& src,
                                  doris_udf::HllVal* dst);
    // return result which is HLL type
    static doris_udf::HllVal hll_raw_agg_finalize(doris_udf::FunctionContext*,
                                                  const doris_udf::HllVal& src);
};

} // namespace doris
