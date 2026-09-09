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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class AvgDistinctToSumDivCountTest implements MemoPatternMatchSupported {
    @Test
    public void testBigIntUsesLargeIntAccumulator() {
        SlotReference value = new SlotReference("value", BigIntType.INSTANCE, false);
        SlotReference other = new SlotReference("other", IntegerType.INSTANCE, false);
        LogicalOneRowRelation relation = new LogicalOneRowRelation(
                StatementScopeIdGenerator.newRelationId(), ImmutableList.of(value, other));

        Avg avg = (Avg) TypeCoercionUtils.processBoundFunction(new Avg(true, value));
        Count count = (Count) TypeCoercionUtils.processBoundFunction(new Count(true, other));
        NamedExpression avgOutput = new Alias(avg, "average");
        NamedExpression countOutput = new Alias(count, "count");
        LogicalAggregate<LogicalOneRowRelation> aggregate = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(avgOutput, countOutput), relation);

        PlanChecker.from(MemoTestUtils.createConnectContext(), aggregate)
                .applyTopDown(new AvgDistinctToSumDivCount())
                .matches(logicalAggregate().when(rewritten -> {
                    Set<AggregateFunction> functions = rewritten.getAggregateFunctions();
                    Sum sum = (Sum) functions.stream()
                            .filter(Sum.class::isInstance)
                            .findFirst()
                            .orElseThrow(() -> new AssertionError("rewritten SUM is missing"));
                    boolean countSharesWidenedArgument = functions.stream()
                            .filter(Count.class::isInstance)
                            .anyMatch(function -> function.child(0).equals(sum.child(0)));
                    return sum.child(0).getDataType().isLargeIntType()
                            && countSharesWidenedArgument;
                }));
    }
}
