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

import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

/**
 * Tests for score pushdown restrictions.
 */
public class PushDownScoreTopNIntoOlapScanTest {

    @Test
    public void testNestedSearchWithScoreIsRejected() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        Slot idSlot = scan.getOutput().get(0);
        SearchDslParser.QsNode nestedRoot = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.NESTED, Collections.emptyList());
        SearchDslParser.QsNode expandedRoot = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.OR, ImmutableList.of(nestedRoot));
        SearchExpression nestedSearch = new SearchExpression(
                "NESTED(items, title:hello)",
                new SearchDslParser.QsPlan(expandedRoot, Collections.emptyList()),
                Collections.emptyList());
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(nestedSearch), scan);
        Alias scoreAlias = new Alias(new Score(), "score");
        LogicalProject<LogicalFilter<LogicalOlapScan>> project = new LogicalProject<>(
                ImmutableList.of(idSlot, scoreAlias), filter);
        LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> topN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(scoreAlias.toSlot(), false, false)), 10, 0, project);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyTopDown(new CheckScoreUsage())
                .getPlan());
        Assertions.assertTrue(exception.getMessage().contains("NESTED"));
    }

    @Test
    public void testNestedSearchWithScoreOutsidePushDownShapeIsRejected() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        SearchDslParser.QsNode nestedRoot = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.NESTED, Collections.emptyList());
        SearchExpression nestedSearch = new SearchExpression(
                "NESTED(items, title:hello)",
                new SearchDslParser.QsPlan(nestedRoot, Collections.emptyList()),
                Collections.emptyList());
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(nestedSearch), scan);
        LogicalTopN<LogicalFilter<LogicalOlapScan>> topN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(new Score(), false, false)), 10, 0, filter);

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyTopDown(new CheckScoreUsage())
                .getPlan());
        Assertions.assertTrue(exception.getMessage().contains("NESTED"));
    }

    @Test
    public void testScoreOutsidePushDownWithoutNestedIsUnchanged() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalTopN<LogicalOlapScan> topN = new LogicalTopN<>(
                ImmutableList.of(new OrderKey(new Score(), false, false)), 10, 0, scan);

        Assertions.assertDoesNotThrow(() -> PlanChecker
                .from(MemoTestUtils.createConnectContext(), topN)
                .applyTopDown(new CheckScoreUsage())
                .getPlan());
    }
}
