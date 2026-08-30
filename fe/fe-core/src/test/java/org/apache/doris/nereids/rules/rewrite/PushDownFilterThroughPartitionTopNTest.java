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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.WindowFuncType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Tests for {@link PushDownFilterThroughPartitionTopN}.
 */
class PushDownFilterThroughPartitionTopNTest implements MemoPatternMatchSupported {

    /**
     * A filter conjunct containing a NoneMovableFunction (e.g. assert_true) must NOT be pushed
     * below the partition top-N: the top-N prunes rows, so assert_true would be evaluated on a
     * different domain. even though its input slot is a partition key, the conjunct stays above.
     */
    @Test
    void testDoNotPushNoneMovableFunctionThroughPartitionTopN() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Expression> partitionKeys = ImmutableList.of(scan.getOutput().get(0));
        LogicalPartitionTopN partitionTopN = new LogicalPartitionTopN<>(
                WindowFuncType.ROW_NUMBER, partitionKeys, ImmutableList.of(), false, 100L, scan);
        Expression predicate = new AssertTrue(
                new GreaterThan(scan.getOutput().get(0), new IntegerLiteral(0)), new StringLiteral("msg"));
        LogicalFilter filter = new LogicalFilter<>(ImmutableSet.of(predicate), partitionTopN);

        PlanChecker.from(MemoTestUtils.createConnectContext(), filter)
                .applyTopDown(new PushDownFilterThroughPartitionTopN())
                .matchesFromRoot(
                        logicalFilter(
                                logicalPartitionTopN(logicalOlapScan())
                        ).when(f -> f.getConjuncts().contains(predicate))
                );
    }
}
