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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.util.BitUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * GroupingSetShape is used to compute which group column should be erased to null,
 * and as the computation source of grouping() / grouping_id() function.
 *
 * which grouping function denote by ExtraVirtualSlotShape.
 * grouping_id function denote by GroupingIdSlotShape.
 * grouping function denote by GroupingSlotShape.
 */
public abstract class GroupingSetShape {
    // this field is only used for nereids planner, backend need use the literal which generate by toLongValue.
    protected final List<List<Boolean>> shouldBeErasedToNull;

    public GroupingSetShape(List<List<Boolean>> shouldBeErasedToNull) {
        this.shouldBeErasedToNull = ImmutableList.copyOf(shouldBeErasedToNull);
    }

    public List<List<Boolean>> getShouldBeErasedToNull() {
        return shouldBeErasedToNull;
    }

    /**
     * The function of toLongValues is to display the virtual value corresponding to virtualSlot.
     * For extra generated columns:
     *  Rule: The column corresponding to groupingIdList is a non-null column,
     *        and the value is 0, and the rest are null columns, and the value is 1.
     *
     * For groupingFunc columns:
     *  Rules:
     *      grouping: Only one parameter is allowed,
     *                when the column is an aggregation column, it is set to 1, otherwise it is 0.
     *      grouping_id: Multiple columns are allowed,
     *                   and the decimal result is returned based on the bitmap of the aggregated column.
     *
     * eg:
     * select k1, grouping(k1), grouping(k1, k2) from t1 group by grouping sets ((k1), (k1, k2), (k3) ());
     * nonVirtualGroupingByExpressions: [k1, k2, k3]
     * virtualGroupingByExpressions: GROUPING_ID(), GROUPING_PREFIX_K1(k1), GROUPING_PREFIX_K1_K2(k1, k2)
     *
     * GROUPING_ID():
     * shouldBeErasedToNull: [[false, true, true], [false, false, true], [true, true, false], [true, true, true]].
     * For (k1):
     * +----+----+----+
     * | k1 | k2 | k3 |
     * +----+----+----+
     * | 0  | 1  | 1  |
     * +----+----+----+
     * convert binary to decimal, the result is 3.
     *
     * And so on, The corresponding list of GROUPING_ID() is [3, 1, 6, 7]
     *
     * GROUPING_PREFIX_K1(k1):
     * shouldBeErasedToNull: [[false], [false], [true], [true]]
     * For (k1):
     * +----+
     * | k1 |
     * +----+
     * | 0  |
     * +----+
     * And so on, The corresponding list GROUPING_PREFIX_K1(k1) is [0, 0, 1, 1]
     *
     * GROUPING_PREFIX_K1_K2(k1, k2):
     * shouldBeErasedToNull: [[false, true], [false, false], [true, true], [true, true]]
     * For (k1):
     * +----+----+
     * | k1 | k2 |
     * +----+----+
     * | 0  | 1  |
     * +----+----+
     * convert binary to decimal, the result is 1.
     *
     * And so on, The corresponding list GROUPING_PREFIX_K1_K2(k1, k2) is [1, 0, 3, 3]
     */
    public List<Long> toLongValue() {
        return this.shouldBeErasedToNull.stream()
                .map(BitUtils::bigEndianBitsToLong)
                .collect(Collectors.toList());
    }
}
