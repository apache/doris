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

#pragma once

#include "udf/udf.h"

namespace doris {

/*
 * How to add a bitmap related function:
 * 1. Implement the function in BitmapFunctions
 *    Note: we have done a improve for bitmap query, So the BitmapValue input
 *    of bitmap functions maybe char array or pointer, you should handle it.
 *    You could refer to bitmap_union or bitmap_count function.
 * 2. Add a UT in BitmapFunctionsTest
 * 3. Add the function signature in gensrc/script/doris_builtins_functions.py
 *    Note: if the result is bitmap serialize data, the function return type should be BITMAP
 *    you could use `nm $DORIS_HOME/output/be/lib/doris_be | grep bitmap` to get the function signature
 * 4. Update the doc  docs/documentation/cn/sql-reference/sql-functions/aggregate-functions/bitmap.md
 *    and docs/documentation/en/sql-reference/sql-functions/aggregate-functions/bitmap_EN.md
 */
class BitmapFunctions {
public:
    static void init();
    static void bitmap_init(FunctionContext* ctx, StringVal* slot);
    static StringVal bitmap_empty(FunctionContext* ctx);

    template <typename T>
    static void bitmap_update_int(FunctionContext* ctx, const T& src, StringVal* dst);
    // the input src's ptr need to point a BitmapValue, this function will release the
    // BitmapValue memory
    static BigIntVal bitmap_finalize(FunctionContext* ctx, const StringVal& src);
    // Get the bitmap cardinality, the difference from bitmap_finalize method is
    // bitmap_get_value method doesn't free memory, this function is used in analytic get_value function
    static BigIntVal bitmap_get_value(FunctionContext* ctx, const StringVal& src);

    static void bitmap_union(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    // the dst value could be null
    static void nullable_bitmap_init(FunctionContext* ctx, StringVal* dst);
    static void bitmap_intersect(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static void group_bitmap_xor(FunctionContext* ctx, const StringVal& src, StringVal* dst);
    static BigIntVal bitmap_count(FunctionContext* ctx, const StringVal& src);
    static BigIntVal bitmap_and_not_count(FunctionContext* ctx, const StringVal& src,
                                          const StringVal& dst);
    static BigIntVal bitmap_xor_count(FunctionContext* ctx, const StringVal& src,
                                      const StringVal& dst);
    static BigIntVal bitmap_min(FunctionContext* ctx, const StringVal& str);

    static BigIntVal bitmap_and_count(FunctionContext* ctx, const StringVal& lhs,
                                      const StringVal& rhs);
    static BigIntVal bitmap_or_count(FunctionContext* ctx, const StringVal& lhs,
                                     const StringVal& rhs);

    static StringVal bitmap_serialize(FunctionContext* ctx, const StringVal& src);
    static StringVal to_bitmap(FunctionContext* ctx, const StringVal& src);
    static StringVal bitmap_hash(FunctionContext* ctx, const StringVal& src);
    static StringVal bitmap_hash64(FunctionContext* ctx, const StringVal& src);
    static StringVal bitmap_or(FunctionContext* ctx, const StringVal& src, const StringVal& dst);
    static StringVal bitmap_xor(FunctionContext* ctx, const StringVal& src, const StringVal& dst);
    static StringVal bitmap_and(FunctionContext* ctx, const StringVal& src, const StringVal& dst);
    static StringVal bitmap_not(FunctionContext* ctx, const StringVal& src, const StringVal& dst);
    static StringVal bitmap_and_not(FunctionContext* ctx, const StringVal& src,
                                    const StringVal& dst);

    //TODO: this functions support variable parameter, but in order to version compatible
    //so have not remove old functions, and now is the version of 0.15, in the future could remove that functions
    static StringVal bitmap_or(FunctionContext* ctx, const StringVal& lhs, int num_args,
                               const StringVal* bitmap_strs);
    static StringVal bitmap_and(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                const StringVal* bitmap_strs);
    static StringVal bitmap_xor(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                const StringVal* bitmap_strs);
    static BigIntVal bitmap_or_count(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                     const StringVal* bitmap_strs);
    static BigIntVal bitmap_and_count(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                      const StringVal* bitmap_strs);
    static BigIntVal bitmap_xor_count(FunctionContext* ctx, const StringVal& lhs, int num_args,
                                      const StringVal* bitmap_strs);

    static StringVal bitmap_to_string(FunctionContext* ctx, const StringVal& input);
    // Convert a comma separated string to a Bitmap
    // Example:
    //      "" will be converted to an empty Bitmap
    //      "1,2,3" will be converted to Bitmap with its Bit 1, 2, 3 set.
    //      "-1, 1" will get nullptr, because -1 is not a valid bit for Bitmap
    static StringVal bitmap_from_string(FunctionContext* ctx, const StringVal& input);
    static BooleanVal bitmap_contains(FunctionContext* ctx, const StringVal& src,
                                      const BigIntVal& input);
    static BooleanVal bitmap_has_any(FunctionContext* ctx, const StringVal& lhs,
                                     const StringVal& rhs);
    static BooleanVal bitmap_has_all(FunctionContext* ctx, const StringVal& lhs,
                                     const StringVal& rhs);

    // intersect count
    template <typename T, typename ValType>
    // this is init function for intersect_count not for bitmap_intersect
    static void bitmap_intersect_init(FunctionContext* ctx, StringVal* dst);
    template <typename T, typename ValType>
    static void bitmap_intersect_update(FunctionContext* ctx, const StringVal& src,
                                        const ValType& key, int num_key, const ValType* keys,
                                        const StringVal* dst);
    template <typename T>
    static void bitmap_intersect_merge(FunctionContext* ctx, const StringVal& src,
                                       const StringVal* dst);
    template <typename T>
    static StringVal bitmap_intersect_serialize(FunctionContext* ctx, const StringVal& src);
    template <typename T>
    static BigIntVal bitmap_intersect_finalize(FunctionContext* ctx, const StringVal& src);
    static BigIntVal bitmap_max(FunctionContext* ctx, const StringVal& str);
    static StringVal bitmap_subset_in_range(FunctionContext* ctx, const StringVal& src,
                                            const BigIntVal& range_start,
                                            const BigIntVal& range_end);
    static StringVal bitmap_subset_limit(FunctionContext* ctx, const StringVal& src,
                                         const BigIntVal& range_start,
                                         const BigIntVal& cardinality_limit);
    static StringVal sub_bitmap(FunctionContext* ctx, const StringVal& src, const BigIntVal& offset,
                                const BigIntVal& cardinality_limit);

    static void orthogonal_bitmap_union_count_init(FunctionContext* ctx, StringVal* slot);
    static StringVal orthogonal_bitmap_count_serialize(FunctionContext* ctx, const StringVal& src);
    static void orthogonal_bitmap_count_merge(FunctionContext* context, const StringVal& src,
                                              StringVal* dst);
    static BigIntVal orthogonal_bitmap_count_finalize(FunctionContext* context,
                                                      const StringVal& src);

    // orthogonal intersect and intersect count
    template <typename T, typename ValType>
    static void orthogonal_bitmap_intersect_count_init(FunctionContext* ctx, StringVal* dst);
    template <typename T, typename ValType>
    static void orthogonal_bitmap_intersect_init(FunctionContext* ctx, StringVal* dst);

    template <typename T>
    static StringVal orthogonal_bitmap_intersect_serialize(FunctionContext* ctx,
                                                           const StringVal& src);
    template <typename T>
    static BigIntVal orthogonal_bitmap_intersect_finalize(FunctionContext* ctx,
                                                          const StringVal& src);

    // orthogonal_bitmap_intersect_count_serialize
    template <typename T>
    static StringVal orthogonal_bitmap_intersect_count_serialize(FunctionContext* ctx,
                                                                 const StringVal& src);
};
} // namespace doris
