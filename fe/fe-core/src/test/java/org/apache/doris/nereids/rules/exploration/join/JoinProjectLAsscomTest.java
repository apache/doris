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

package org.apache.doris.nereids.rules.exploration.join;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class JoinProjectLAsscomTest {

    private static List<LogicalOlapScan> scans = Lists.newArrayList();
    private static List<List<SlotReference>> outputs = Lists.newArrayList();

    @BeforeAll
    public static void init() {
        Table t1 = new Table(0L, "t1", Table.TableType.OLAP,
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "0", "")));
        LogicalOlapScan scan1 = new LogicalOlapScan(t1, ImmutableList.of());

        Table t2 = new Table(0L, "t2", Table.TableType.OLAP,
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "0", "")));
        LogicalOlapScan scan2 = new LogicalOlapScan(t2, ImmutableList.of());

        Table t3 = new Table(0L, "t3", Table.TableType.OLAP,
                ImmutableList.of(new Column("id", Type.INT, true, AggregateType.NONE, "0", ""),
                        new Column("name", Type.STRING, true, AggregateType.NONE, "0", "")));
        LogicalOlapScan scan3 = new LogicalOlapScan(t3, ImmutableList.of());
        scans.add(scan1);
        scans.add(scan2);
        scans.add(scan3);

        List<SlotReference> t1Output = scan1.getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> t2Output = scan2.getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        List<SlotReference> t3Output = scan3.getOutput().stream().map(slot -> (SlotReference) slot)
                .collect(Collectors.toList());
        outputs.add(t1Output);
        outputs.add(t2Output);
        outputs.add(t3Output);
    }

    private Pair<LogicalJoin, LogicalJoin> testJoinProjectLAsscom(PlannerContext plannerContext,
            List<NamedExpression> projects) {
        /*
         *        topJoin                   newTopJoin
         *        /     \                   /        \
         *    project    C          newLeftProject newRightProject
         *      /            ──►          /            \
         * bottomJoin                newBottomJoin      B
         *    /   \                     /   \
         *   A     B                   A     C
         */

        Assertions.assertEquals(3, scans.size());

        List<SlotReference> t1 = outputs.get(0);
        List<SlotReference> t2 = outputs.get(1);
        List<SlotReference> t3 = outputs.get(2);
        Expression bottomJoinOnCondition = new EqualTo(t1.get(0), t2.get(0));
        Expression topJoinOnCondition = new EqualTo(t1.get(1), t3.get(1));

        LogicalProject<LogicalJoin<LogicalOlapScan, LogicalOlapScan>> project = new LogicalProject<>(
                projects,
                new LogicalJoin<>(JoinType.INNER_JOIN, Optional.of(bottomJoinOnCondition), scans.get(0), scans.get(1)));

        LogicalJoin<LogicalProject<LogicalJoin<LogicalOlapScan, LogicalOlapScan>>, LogicalOlapScan> topJoin
                = new LogicalJoin<>(JoinType.INNER_JOIN, Optional.of(topJoinOnCondition), project, scans.get(2));

        Rule rule = new JoinProjectLAsscom().build();
        List<Plan> transform = rule.transform(topJoin, plannerContext);
        Assertions.assertEquals(1, transform.size());
        Assertions.assertTrue(transform.get(0) instanceof LogicalJoin);
        LogicalJoin newTopJoin = (LogicalJoin) transform.get(0);
        return new Pair<>(topJoin, newTopJoin);
    }

    @Test
    public void testStarJoinProjectLAsscom(@Mocked PlannerContext plannerContext) {
        List<SlotReference> t1 = outputs.get(0);
        List<SlotReference> t2 = outputs.get(1);
        List<NamedExpression> projects = ImmutableList.of(
                new Alias(t2.get(0), "t2.id"),
                new Alias(t1.get(0), "t1.id"),
                t1.get(1),
                t2.get(1)
        );

        Pair<LogicalJoin, LogicalJoin> pair = testJoinProjectLAsscom(plannerContext, projects);

        LogicalJoin oldJoin = pair.first;
        LogicalJoin newTopJoin = pair.second;

        // Join reorder successfully.
        Assertions.assertNotEquals(oldJoin, newTopJoin);
        Assertions.assertEquals("t1.id", ((Alias) ((LogicalProject) newTopJoin.left()).getProjects().get(0)).getName());
        Assertions.assertEquals("name",
                ((SlotReference) ((LogicalProject) newTopJoin.left()).getProjects().get(1)).getName());
        Assertions.assertEquals("t2.id",
                ((Alias) ((LogicalProject) newTopJoin.right()).getProjects().get(0)).getName());
        Assertions.assertEquals("name",
                ((SlotReference) ((LogicalProject) newTopJoin.left()).getProjects().get(1)).getName());

    }
}
