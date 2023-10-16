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

package org.apache.doris.catalog;

import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapIntersect;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.BitmapUnionInt;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.CountByEnum;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupBitmapXor;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnion;
import org.apache.doris.nereids.trees.expressions.functions.agg.HllUnionAgg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;

import com.google.common.collect.ImmutableList;

/**
 * Builtin aggregate functions.
 *
 * Note: Please ensure that this class only has some lists and no procedural code.
 *       It helps to be clear and concise.
 */
public class BuiltinAggregateFunctions implements FunctionHelper {
    public final ImmutableList<AggregateFunc> aggregateFunctions = ImmutableList.of(
            agg(Avg.class),
            agg(BitmapIntersect.class, "bitmap_intersect"),
            agg(BitmapUnion.class, "bitmap_union"),
            agg(BitmapUnionCount.class, "bitmap_union_count"),
            agg(BitmapUnionInt.class, "bitmap_union_int"),
            agg(Count.class),
            agg(CountByEnum.class, "count_by_enum"),
            agg(GroupBitmapXor.class, "group_bitmap_xor"),
            agg(HllUnion.class, "hll_union", "hll_raw_agg"),
            agg(HllUnionAgg.class, "hll_union_agg"),
            agg(Max.class),
            agg(Min.class),
            agg(Sum.class)
    );

    public static final BuiltinAggregateFunctions INSTANCE = new BuiltinAggregateFunctions();

    // Note: Do not add any code here!
    private BuiltinAggregateFunctions() {}
}
