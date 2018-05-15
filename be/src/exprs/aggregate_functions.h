// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#ifndef BDG_PALO_BE_SRC_QUERY_EXPRS_AGGREGATE_FUNCTIONS_H
#define BDG_PALO_BE_SRC_QUERY_EXPRS_AGGREGATE_FUNCTIONS_H

//#include "exprs/opcode_registry.h"
#include "udf/udf.h"
#include "udf/udf_internal.h"
#include "olap/hll.h"

namespace palo {

class HllSetResolver;
class HybirdSetBase;

// Collection of builtin aggregate functions. Aggregate functions implement
// the various phases of the aggregation: Init(), Update(), Serialize(), Merge(),
// and Finalize(). Not all functions need to implement all of the steps and
// some of the parts can be reused across different aggregate functions.
// This functions are implemented using the UDA interface.

class AggregateFunctions {
public:
    // Initializes dst to NULL.
    static void init_null(palo_udf::FunctionContext*, palo_udf::AnyVal* dst);
    // Initializes dst to NULL and sets dst->ptr to NULL.
    static void init_null_string(palo_udf::FunctionContext* c, palo_udf::StringVal* dst);

    // Initializes dst to 0.
    template <typename T>
    static void init_zero(palo_udf::FunctionContext*, T* dst);

    template<typename SRC_VAL, typename DST_VAL>
    static void sum_remove(palo_udf::FunctionContext* ctx, const SRC_VAL& src, DST_VAL* dst);

    // palo_udf::StringVal GetValue() function that returns a copy of src
    static palo_udf::StringVal string_val_get_value(palo_udf::FunctionContext* ctx,
        const palo_udf::StringVal& src);
    static palo_udf::StringVal string_val_serialize_or_finalize(palo_udf::FunctionContext* ctx,
         const palo_udf::StringVal& src);

    // Implementation of Count and Count(*)
    static void count_update(palo_udf::FunctionContext*, const palo_udf::AnyVal& src,
        palo_udf::BigIntVal* dst);
    static void count_merge(palo_udf::FunctionContext*,
        const palo_udf::BigIntVal& src, palo_udf::BigIntVal* dst);
    static void count_remove(palo_udf::FunctionContext*,
        const palo_udf::AnyVal& src, palo_udf::BigIntVal* dst);
    static void count_star_update(palo_udf::FunctionContext*, palo_udf::BigIntVal* dst);

    static void count_star_remove(FunctionContext*, BigIntVal* dst);
    // Implementation of Avg.
    // TODO: Change this to use a fixed-sized BufferVal as intermediate type.
    static void avg_init(palo_udf::FunctionContext* ctx, palo_udf::StringVal* dst);
    template <typename T>
    static void avg_update(palo_udf::FunctionContext* ctx, const T& src,
        palo_udf::StringVal* dst);
    template <typename T>
    static void avg_remove(palo_udf::FunctionContext* ctx, const T& src,
        palo_udf::StringVal* dst);
    static void avg_merge(FunctionContext* ctx, const StringVal& src, StringVal*
dst);
    static palo_udf::DoubleVal avg_get_value(palo_udf::FunctionContext* ctx,
         const palo_udf::StringVal& val);
    static palo_udf::DoubleVal avg_finalize(palo_udf::FunctionContext* ctx,
         const palo_udf::StringVal& val);

    // Avg for timestamp. Uses avg_init() and AvgMerge().
    static void timestamp_avg_update(palo_udf::FunctionContext* ctx,
            const palo_udf::DateTimeVal& src,
            palo_udf::StringVal* dst);
    static void timestamp_avg_remove(palo_udf::FunctionContext* ctx,
            const palo_udf::DateTimeVal& src,
            palo_udf::StringVal* dst);
    static palo_udf::DateTimeVal timestamp_avg_get_value(palo_udf::FunctionContext* ctx,
            const palo_udf::StringVal& val);
    static palo_udf::DateTimeVal timestamp_avg_finalize(palo_udf::FunctionContext* ctx,
            const palo_udf::StringVal& val);

    // Avg for decimals.
    static void decimal_avg_init(palo_udf::FunctionContext* ctx, palo_udf::StringVal* dst);
    static void decimal_avg_update(palo_udf::FunctionContext* ctx,
            const palo_udf::DecimalVal& src,
            palo_udf::StringVal* dst);
    static void decimal_avg_merge(FunctionContext* ctx, const palo_udf::StringVal& src,
            palo_udf::StringVal* dst);
    static void decimal_avg_remove(palo_udf::FunctionContext* ctx,
            const palo_udf::DecimalVal& src,
            palo_udf::StringVal* dst);

    // static void decimal_avg_add_or_remove(palo_udf::FunctionContext* ctx,
    //        const palo_udf::DecimalVal& src,
    //        palo_udf::StringVal* dst, bool remove);
    // static void decimal_avg_add_or_remove(palo_udf::FunctionContext* ctx,
    //        const palo_udf::DecimalVal& src,
    //        palo_udf::StringVal* dst) {
    //    return decimal_avg_add_or_remove(ctx, src, dst, false);
    // }
    static palo_udf::DecimalVal decimal_avg_get_value(palo_udf::FunctionContext* ctx,
         const palo_udf::StringVal& val);
    static palo_udf::DecimalVal decimal_avg_finalize(palo_udf::FunctionContext* ctx,
         const palo_udf::StringVal& val);

    // SumUpdate, SumMerge
    template <typename SRC_VAL, typename DST_VAL>
    static void sum(palo_udf::FunctionContext*, const SRC_VAL& src, DST_VAL* dst);

    // MinUpdate/MinMerge
    template <typename T>
    static void min(palo_udf::FunctionContext*, const T& src, T* dst);

    // MaxUpdate/MaxMerge
    template <typename T>
    static void max(palo_udf::FunctionContext*, const T& src, T* dst);


    // String concat
    static void string_concat(
            palo_udf::FunctionContext*,
            const palo_udf::StringVal& src,
            const palo_udf::StringVal& separator,
            palo_udf::StringVal* result);

      /// String concat
    static void string_concat_update(FunctionContext*,
            const StringVal& src, StringVal* result);
    static void string_concat_update(FunctionContext*,
            const StringVal& src, const StringVal& separator, StringVal* result);
    static void string_concat_merge(FunctionContext*,
            const StringVal& src, StringVal* result);
    static StringVal string_concat_finalize(FunctionContext*,
            const StringVal& src);

    // Probabilistic Counting (PC), a distinct estimate algorithms.
    // Probabilistic Counting with Stochastic Averaging (PCSA) is a variant
    // of PC that runs faster and usually gets equally accurate results.
    static void pc_init(palo_udf::FunctionContext*, palo_udf::StringVal* slot);

    template <typename T>
    static void pc_update(palo_udf::FunctionContext*, const T& src, palo_udf::StringVal* dst);
    template <typename T>
    static void pcsa_update(palo_udf::FunctionContext*, const T& src, palo_udf::StringVal* dst);

    static void pc_merge(
            palo_udf::FunctionContext*,
            const palo_udf::StringVal& src,
            palo_udf::StringVal* dst);

    static palo_udf::StringVal pc_finalize(
            palo_udf::FunctionContext*,
            const palo_udf::StringVal& src);

    static palo_udf::StringVal pcsa_finalize(
            palo_udf::FunctionContext*,
            const palo_udf::StringVal& src);

    // Hyperloglog distinct estimate algorithm.
    // See these papers for more details.
    // 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
    // algorithm (2007)
    // 2) HyperLogLog in Practice (paper from google with some improvements)
    static void hll_init(palo_udf::FunctionContext*, palo_udf::StringVal* slot);
    template <typename T>
    static void hll_update(palo_udf::FunctionContext*, const T& src, palo_udf::StringVal* dst);
    static void hll_merge(
            palo_udf::FunctionContext*,
            const palo_udf::StringVal& src,
            palo_udf::StringVal* dst);
    static palo_udf::StringVal hll_finalize(
            palo_udf::FunctionContext*,
            const palo_udf::StringVal& src);

    // count and sum distinct algorithm in multi distinct
    template <typename T>
    static void count_or_sum_distinct_numeric_init(palo_udf::FunctionContext* ctx, palo_udf::StringVal* dst);
    template <typename T>
    static void count_or_sum_distinct_numeric_update(FunctionContext* ctx, T& src, StringVal* dst);
    template <typename T>
    static void count_or_sum_distinct_numeric_merge(FunctionContext* ctx, StringVal& src, StringVal* dst);
    template <typename T>
    static StringVal count_or_sum_distinct_numeric_serialize(FunctionContext* ctx, const StringVal& state_sv);
    template <typename T>
    static BigIntVal count_or_sum_distinct_numeric_finalize(FunctionContext* ctx, const StringVal& state_sv);

    // count distinct in multi distinct for string 
    static void count_distinct_string_init(palo_udf::FunctionContext* ctx, palo_udf::StringVal* dst);
    static void count_distinct_string_update(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static void count_distinct_string_merge(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static StringVal count_distinct_string_serialize(FunctionContext* ctx, const StringVal& state_sv);
    static BigIntVal count_distinct_string_finalize(FunctionContext* ctx, const StringVal& state_sv);
 
    // count distinct in multi distinct for decimal
    static void count_or_sum_distinct_decimal_init(palo_udf::FunctionContext* ctx, palo_udf::StringVal* dst);
    static void count_or_sum_distinct_decimal_update(FunctionContext* ctx, DecimalVal& src, StringVal* dst);
    static void count_or_sum_distinct_decimal_merge(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static StringVal count_or_sum_distinct_decimal_serialize(FunctionContext* ctx, const StringVal& state_sv);
    static BigIntVal count_distinct_decimal_finalize(FunctionContext* ctx, const StringVal& state_sv);
    static DecimalVal sum_distinct_decimal_finalize(FunctionContext* ctx, const StringVal& state_sv);

    // count distinct in multi disticnt for Date
    static void count_distinct_date_init(palo_udf::FunctionContext* ctx, palo_udf::StringVal* dst);
    static void count_distinct_date_update(FunctionContext* ctx, DateTimeVal& src, StringVal* dst);
    static void count_distinct_date_merge(FunctionContext* ctx, StringVal& src, StringVal* dst);
    static StringVal count_distinct_date_serialize(FunctionContext* ctx, const StringVal& state_sv);
    static BigIntVal count_distinct_date_finalize(FunctionContext* ctx, const StringVal& state_sv);
 
    template <typename T>
    static BigIntVal sum_distinct_bigint_finalize(FunctionContext* ctx, const StringVal& state_sv);
    template <typename T>
    static LargeIntVal sum_distinct_largeint_finalize(FunctionContext* ctx, const StringVal& state_sv);
    template <typename T>
    static DoubleVal sum_distinct_double_finalize(FunctionContext* ctx, const StringVal& state_sv); 

    /// Knuth's variance algorithm, more numerically stable than canonical stddev
    /// algorithms; reference implementation:
    /// http://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Online_algorithm
    static void knuth_var_init(FunctionContext* context, StringVal* val);
    template <typename T>
        static void knuth_var_update(FunctionContext* context, const T& input, StringVal* val);
    static void knuth_var_merge(FunctionContext* context, const StringVal& src,
                              StringVal* dst);
    static DoubleVal knuth_var_finalize(FunctionContext* context, const StringVal& val);

    /// Calculates the biased variance, uses KnuthVar Init-Update-Merge functions
    static DoubleVal knuth_var_pop_finalize(FunctionContext* context, const StringVal& val);

    /// Calculates STDDEV, uses KnuthVar Init-Update-Merge functions
    static DoubleVal knuth_stddev_finalize(FunctionContext* context, const StringVal& val);

    /// Calculates the biased STDDEV, uses KnuthVar Init-Update-Merge functions
    static DoubleVal knuth_stddev_pop_finalize(FunctionContext* context, const StringVal& val);

    /// ----------------------------- Analytic Functions ---------------------------------
    /// Analytic functions implement the UDA interface (except Merge(), Serialize()) and are
    /// used internally by the AnalyticEvalNode. Some analytic functions store intermediate
    /// state as a StringVal which is needed for multiple calls to Finalize(), so some fns
    /// also implement a (private) GetValue() method to just return the value. In that
    /// case, Finalize() is only called at the end to clean up.

    // Initializes the state for RANK and DENSE_RANK
    static void rank_init(palo_udf::FunctionContext*, palo_udf::StringVal* slot);

    // Update state for RANK
    static void rank_update(palo_udf::FunctionContext*, palo_udf::StringVal* dst);

    // Update state for DENSE_RANK
    static void dense_rank_update(palo_udf::FunctionContext*, palo_udf::StringVal* dst);

    // Returns the result for RANK and prepares the state for the next Update().
    static palo_udf::BigIntVal rank_get_value(palo_udf::FunctionContext*,
        palo_udf::StringVal& src);

    // Returns the result for DENSE_RANK and prepares the state for the next Update().
    // TODO: Implement DENSE_RANK with a single palo_udf::BigIntVal. Requires src can be modified,
    // AggFnEvaluator would need to handle copying the src palo_udf::AnyVal back into the src slot.
    static palo_udf::BigIntVal dense_rank_get_value(palo_udf::FunctionContext*,
        palo_udf::StringVal& src);

    // Returns the result for RANK and DENSE_RANK and cleans up intermediate state in src.
    static palo_udf::BigIntVal rank_finalize(palo_udf::FunctionContext*,
         palo_udf::StringVal& src);

    // Implements LAST_VALUE.
    template <typename T>
    static void last_val_update(palo_udf::FunctionContext*, const T& src, T* dst);
    template <typename T>
    static void last_val_remove(palo_udf::FunctionContext*, const T& src, T* dst);

    // Implements FIRST_VALUE.
    template <typename T>
    static void first_val_update(palo_udf::FunctionContext*, const T& src, T* dst);
    // Implements FIRST_VALUE for some windows that require rewrites during planning.
    // The palo_udf::BigIntVal is unused by first_val_rewrite_update() (it is used by the
    // AnalyticEvalNode).
    template <typename T>
    static void first_val_rewrite_update(palo_udf::FunctionContext*, const T& src,
            const palo_udf::BigIntVal&, T* dst);

    // OffsetFn*() implement LAG and LEAD. Init() sets the default value (the last
    // constant parameter) as dst.
    template <typename T>
    static void offset_fn_init(palo_udf::FunctionContext*, T* dst);

    // Update() takes all the parameters to LEAD/LAG, including the integer offset and
    // the default value, neither which are needed by Update(). (The offset is already
    // used in the window for the analytic fn evaluation and the default value is set
    // in Init().
    template <typename T>
    static void offset_fn_update(palo_udf::FunctionContext*, const T& src,
        const palo_udf::BigIntVal&, const T&, T* dst);

    //  HLL value type calculate
    //  init sets buffer
    static void hll_union_agg_init(palo_udf::FunctionContext*, palo_udf::StringVal* slot);
    // fill all register accroading to hll set type
    static void hll_union_agg_update(palo_udf::FunctionContext*, const palo_udf::StringVal& src, 
                                     palo_udf::StringVal* dst);
    // merge the register value
    static void hll_union_agg_merge(
                          palo_udf::FunctionContext*,
                          const palo_udf::StringVal& src,
                          palo_udf::StringVal* dst);
    // return result
    static palo_udf::StringVal hll_union_agg_finalize(
                                            palo_udf::FunctionContext*,
                                            const palo_udf::StringVal& src);
    // calculate result
    static int64_t hll_algorithm(const palo_udf::StringVal& src);
    static void hll_union_parse_and_cal(HllSetResolver& resolver, StringVal* dst);
};

}

#endif

