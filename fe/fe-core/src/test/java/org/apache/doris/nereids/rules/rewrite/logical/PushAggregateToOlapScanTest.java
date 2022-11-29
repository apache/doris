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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Ln;
import org.apache.doris.nereids.trees.plans.PushDownAggOperator;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class PushAggregateToOlapScanTest {

    @Test
    public void testWithoutProject() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);
        LogicalAggregate<LogicalOlapScan> aggregate;
        CascadesContext context;
        LogicalOlapScan pushedOlapScan;

        // min max
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(olapScan.getOutput().get(0)), "min")),
                olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0));
        Assertions.assertTrue(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.MIN_MAX, pushedOlapScan.getPushDownAggOperator());

        // count
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(olapScan.getOutput().get(0)), "count")),
                olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0));
        Assertions.assertTrue(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.COUNT, pushedOlapScan.getPushDownAggOperator());

        // mix
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(olapScan.getOutput().get(0)), "count"),
                        new Alias(new Max(olapScan.getOutput().get(0)), "max")),
                olapScan);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0));
        Assertions.assertTrue(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.MIX, pushedOlapScan.getPushDownAggOperator());
    }

    @Test
    public void testWithProject() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(olapScan.getOutput().get(0)), olapScan);
        LogicalAggregate<LogicalProject<LogicalOlapScan>> aggregate;
        CascadesContext context;
        LogicalOlapScan pushedOlapScan;

        // min max
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(project.getOutput().get(0)), "min")),
                project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0).child(0));
        Assertions.assertTrue(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.MIN_MAX, pushedOlapScan.getPushDownAggOperator());

        // count
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count")),
                project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0).child(0));
        Assertions.assertTrue(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.COUNT, pushedOlapScan.getPushDownAggOperator());

        // mix
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Count(project.getOutput().get(0)), "count"),
                        new Alias(new Max(olapScan.getOutput().get(0)), "max")),
                project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0).child(0));
        Assertions.assertTrue(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.MIX, pushedOlapScan.getPushDownAggOperator());
    }

    @Test
    void testProjectionCheck() {
        LogicalOlapScan olapScan = PlanConstructor.newLogicalOlapScan(1, "tbl", 0);
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.of(new Alias(new Ln(olapScan.getOutput().get(0)), "alias")), olapScan);
        LogicalAggregate<LogicalProject<LogicalOlapScan>> aggregate;
        CascadesContext context;
        LogicalOlapScan pushedOlapScan;

        // min max
        aggregate = new LogicalAggregate<>(
                Collections.emptyList(),
                ImmutableList.of(new Alias(new Min(project.getOutput().get(0)), "min")),
                project);
        context = MemoTestUtils.createCascadesContext(aggregate);

        context.topDownRewrite(new PushAggregateToOlapScan());
        pushedOlapScan = (LogicalOlapScan) (context.getMemo().copyOut().child(0).child(0));
        Assertions.assertFalse(pushedOlapScan.isAggPushed());
        Assertions.assertEquals(PushDownAggOperator.NONE, pushedOlapScan.getPushDownAggOperator());
    }
}
