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
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderCommon.Type;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Lists;

import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class JoinLAsscomTest {

    private static List<LogicalOlapScan> scans = Lists.newArrayList();
    private static List<List<SlotReference>> outputs = Lists.newArrayList();

    @BeforeAll
    public static void init() {
        LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
        LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);

        scans.add(scan1);
        scans.add(scan2);
        scans.add(scan3);

        List<SlotReference> t1Output = Utils.getOutputSlotReference(scan1);
        List<SlotReference> t2Output = Utils.getOutputSlotReference(scan2);
        List<SlotReference> t3Output = Utils.getOutputSlotReference(scan3);
        outputs.add(t1Output);
        outputs.add(t2Output);
        outputs.add(t3Output);
    }

    public Pair<LogicalJoin, LogicalJoin> testJoinLAsscom(
            Expression bottomJoinOnCondition,
            Expression bottomNonHashExpression,
            Expression topJoinOnCondition,
            Expression topNonHashExpression) {
        /*
         *      topJoin                newTopJoin
         *      /     \                 /     \
         * bottomJoin  C   -->  newBottomJoin  B
         *  /    \                  /    \
         * A      B                A      C
         */
        Assertions.assertEquals(3, scans.size());
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> bottomJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                Lists.newArrayList(bottomJoinOnCondition),
                Optional.of(bottomNonHashExpression), scans.get(0), scans.get(1));
        LogicalJoin<LogicalJoin<LogicalOlapScan, LogicalOlapScan>, LogicalOlapScan> topJoin = new LogicalJoin<>(
                JoinType.INNER_JOIN, Lists.newArrayList(topJoinOnCondition),
                Optional.of(topNonHashExpression), bottomJoin, scans.get(2));

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(topJoin);
        Rule rule = new JoinLAsscom(Type.INNER).build();
        List<Plan> transform = rule.transform(topJoin, cascadesContext);
        Assertions.assertEquals(1, transform.size());
        Assertions.assertTrue(transform.get(0) instanceof LogicalJoin);
        LogicalJoin newTopJoin = (LogicalJoin) transform.get(0);
        return Pair.of(topJoin, newTopJoin);
    }

    @Test
    @Ignore
    public void testStarJoinLAsscom() {
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
        Expression bottomNonHashExpression = new LessThan(t1.get(0), t2.get(0));
        Expression topJoinOnCondition = new EqualTo(t1.get(1), t3.get(1));
        Expression topNonHashCondition = new LessThan(t1.get(1), t3.get(1));

        Pair<LogicalJoin, LogicalJoin> pair = testJoinLAsscom(
                bottomJoinOnCondition,
                bottomNonHashExpression,
                topJoinOnCondition,
                topNonHashCondition);
        LogicalJoin oldJoin = pair.first;
        LogicalJoin newTopJoin = pair.second;

        // Join reorder successfully.
        Assertions.assertNotEquals(oldJoin, newTopJoin);
        Assertions.assertEquals("t1",
                ((LogicalOlapScan) ((LogicalJoin) newTopJoin.left()).left()).getTable().getName());
        Assertions.assertEquals("t3",
                ((LogicalOlapScan) ((LogicalJoin) newTopJoin.left()).right()).getTable().getName());
        Assertions.assertEquals("t2", ((LogicalOlapScan) newTopJoin.right()).getTable().getName());
        Assertions.assertEquals(newTopJoin.getOtherJoinCondition(),
                ((LogicalJoin) oldJoin.child(0)).getOtherJoinCondition());
    }

    @Test
    @Ignore
    public void testChainJoinLAsscom() {
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
        Expression bottomNonHashExpression = new LessThan(t1.get(0), t2.get(0));
        Expression topJoinOnCondition = new EqualTo(t2.get(0), t3.get(0));
        Expression topNonHashExpression = new LessThan(t2.get(0), t3.get(0));

        Pair<LogicalJoin, LogicalJoin> pair = testJoinLAsscom(bottomJoinOnCondition, bottomNonHashExpression,
                topJoinOnCondition, topNonHashExpression);
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
