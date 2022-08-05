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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class JoinLAsscomTest {

    private static List<LogicalOlapScan> scans = Lists.newArrayList();
    private static List<List<SlotReference>> outputs = Lists.newArrayList();

    @BeforeAll
    public static void init() {
        LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScanWithTable("t1");
        LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScanWithTable("t2");
        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScanWithTable("t3");

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

    public Pair<LogicalJoin, LogicalJoin> testJoinLAsscom(PlannerContext plannerContext,
            Expression bottomJoinOnCondition, Expression topJoinOnCondition) {
        /*
         *      topJoin                newTopJoin
         *      /     \                 /     \
         * bottomJoin  C   -->  newBottomJoin  B
         *  /    \                  /    \
         * A      B                A      C
         */
        Assertions.assertEquals(3, scans.size());
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> bottomJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                Optional.of(bottomJoinOnCondition), scans.get(0), scans.get(1));
        LogicalJoin<LogicalJoin<LogicalOlapScan, LogicalOlapScan>, LogicalOlapScan> topJoin = new LogicalJoin<>(
                JoinType.INNER_JOIN, Optional.of(topJoinOnCondition), bottomJoin, scans.get(2));

        Rule rule = new JoinLAsscom().build();
        List<Plan> transform = rule.transform(topJoin, plannerContext);
        Assertions.assertEquals(1, transform.size());
        Assertions.assertTrue(transform.get(0) instanceof LogicalJoin);
        LogicalJoin newTopJoin = (LogicalJoin) transform.get(0);
        return new Pair<>(topJoin, newTopJoin);
    }

    @Test
    public void testStarJoinLAsscom(@Mocked PlannerContext plannerContext) {
        /*
         * Star-Join
         * t1 -- t2
         * |
         * t3
         * <p>
         *     t1.id=t3.id               t1.id=t2.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         * t1.id=t2.id  t3          t1.id=t3.id   t2
         * bottomJoin       -->    newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */

        List<SlotReference> t1 = outputs.get(0);
        List<SlotReference> t2 = outputs.get(1);
        List<SlotReference> t3 = outputs.get(2);
        Expression bottomJoinOnCondition = new EqualTo(t1.get(0), t2.get(0));
        Expression topJoinOnCondition = new EqualTo(t1.get(1), t3.get(1));

        Pair<LogicalJoin, LogicalJoin> pair = testJoinLAsscom(plannerContext, bottomJoinOnCondition,
                topJoinOnCondition);
        LogicalJoin oldJoin = pair.first;
        LogicalJoin newTopJoin = pair.second;

        // Join reorder successfully.
        Assertions.assertNotEquals(oldJoin, newTopJoin);
        Assertions.assertEquals("t1",
                ((LogicalOlapScan) ((LogicalJoin) newTopJoin.left()).left()).getTable().getName());
        Assertions.assertEquals("t3",
                ((LogicalOlapScan) ((LogicalJoin) newTopJoin.left()).right()).getTable().getName());
        Assertions.assertEquals("t2", ((LogicalOlapScan) newTopJoin.right()).getTable().getName());
    }

    @Test
    public void testChainJoinLAsscom(@Mocked PlannerContext plannerContext) {
        /*
         * Chain-Join
         * t1 -- t2 -- t3
         * <p>
         *     t2.id=t3.id               t2.id=t3.id
         *       topJoin                  newTopJoin
         *       /     \                   /     \
         * t1.id=t2.id  t3          t1.id=t3.id   t2
         * bottomJoin       -->    newBottomJoin
         *   /    \                   /    \
         * t1      t2               t1      t3
         */

        List<SlotReference> t1 = outputs.get(0);
        List<SlotReference> t2 = outputs.get(1);
        List<SlotReference> t3 = outputs.get(2);
        Expression bottomJoinOnCondition = new EqualTo(t1.get(0), t2.get(0));
        Expression topJoinOnCondition = new EqualTo(t2.get(0), t3.get(0));

        Pair<LogicalJoin, LogicalJoin> pair = testJoinLAsscom(plannerContext, bottomJoinOnCondition,
                topJoinOnCondition);
        LogicalJoin oldJoin = pair.first;
        LogicalJoin newTopJoin = pair.second;

        // Join reorder failed.
        // Chain-Join LAsscom directly will be failed.
        // After t1 -- t2 -- t3
        // -- join commute -->
        // t1 -- t2
        // |
        // t3
        // then, we can LAsscom for this star-join.
        Assertions.assertEquals(oldJoin, newTopJoin);
    }
}
