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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.MatchAny;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * Test for PushDownMatchProjectionAsVirtualColumn rule.
 */
public class PushDownMatchProjectionAsVirtualColumnTest implements MemoPatternMatchSupported {

    @Test
    void testPushDownMatchProjection() {
        // PlanConstructor creates table with columns: id (INT), name (STRING)
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);    // id: INT
        Slot nameSlot = slots.get(1);  // name: STRING

        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchExpr, "m")), scan);

        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .matches(
                        logicalProject(
                                logicalOlapScan().when(s -> !s.getVirtualColumns().isEmpty())
                        )
                );
    }

    @Test
    void testPushDownMatchProjectionWithFilter() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);
        Slot nameSlot = slots.get(1);

        MatchAny matchExpr = new MatchAny(nameSlot, new StringLiteral("hello"));
        GreaterThan filterPred = new GreaterThan(idSlot, new IntegerLiteral(2));

        LogicalProject<LogicalFilter<LogicalOlapScan>> project = new LogicalProject<>(
                ImmutableList.of(idSlot, new Alias(matchExpr, "m")),
                new LogicalFilter<>(ImmutableSet.of(filterPred), scan));

        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .matches(
                        logicalProject(
                                logicalFilter(
                                        logicalOlapScan().when(s -> !s.getVirtualColumns().isEmpty())
                                )
                        )
                );
    }

    @Test
    void testNoMatchExpressionNoChange() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        List<Slot> slots = scan.getOutput();
        Slot idSlot = slots.get(0);

        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(idSlot), scan);

        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyTopDown(new PushDownMatchProjectionAsVirtualColumn())
                .matches(
                        logicalProject(
                                logicalOlapScan().when(s -> s.getVirtualColumns().isEmpty())
                        )
                );
    }
}
