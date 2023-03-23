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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * PartitionSlotInput, the input of the partition slot.
 * We will replace the partition slot to PartitionSlotInput#result, so that we can evaluate the expression tree.
 *
 * for example, the partition predicate: part_column1 > 1, if the partition range is [('1'), ('4')),
 * and part_column1 is int type.
 *
 *             GreaterThen                                                 GreaterThen
 *      /                      \                  ->                 /                    \
 * Slot(part_column1)       IntegerLiteral(1)                IntegerLiteral(n)       IntegerLiteral(1)
 *     ^                                                           ^
 *     |                                                           |
 *     +------------------------------------------------------------
 *                             |
 *                        replace by
 *      PartitionSlotInput(result = IntegerLiteral(1))
 *      PartitionSlotInput(result = IntegerLiteral(2))
 *      PartitionSlotInput(result = IntegerLiteral(3))
 *
 *
 * if the partition slot can not enumerable(some RANGE/ all OTHER partition slot type), e.g. we will stay slot:
 * PartitionSlotInput(result = Slot(part_column1))
 */
public class PartitionSlotInput {
    // the partition slot will be replaced to this result
    public final Expression result;

    // all partition slot's range map, the example in the class comment, it will be
    // {Slot(part_column1): [1, 4)}
    public final Map<Slot, ColumnRange> columnRanges;

    public PartitionSlotInput(Expression result, Map<Slot, ColumnRange> columnRanges) {
        this.result = result;
        this.columnRanges = ImmutableMap.copyOf(columnRanges);
    }
}
