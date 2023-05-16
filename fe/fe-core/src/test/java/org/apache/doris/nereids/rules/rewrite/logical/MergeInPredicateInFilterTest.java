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

import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Set;

public class MergeInPredicateInFilterTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testMergeInPredicate() {
        SlotReference id = (SlotReference) scan.getOutput().get(0);
        SlotReference name = (SlotReference) scan.getOutput().get(1);
        Expression i1 = new IntegerLiteral(1);
        Expression i2 = new IntegerLiteral(2);
        Expression i3 = new IntegerLiteral(3);
        InPredicate in1 = new InPredicate(id, Lists.newArrayList(i1, i2));
        InPredicate in2 = new InPredicate(id, Lists.newArrayList(i2, i3));
        LogicalPlan filter = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(new Or(in1, in2), new EqualTo(name, new StringLiteral("abc"))))
                .build();
        PlanChecker.from(new ConnectContext(), filter).applyBottomUp(new MergeInPredicateInFilter())
                .matches(
                    logicalFilter().when(plan -> {
                        Set<Expression> conjuncts = plan.getConjuncts();
                        Assertions.assertEquals(2, conjuncts.size());
                        InPredicate inPred = null;
                        EqualTo eq = null;
                        Iterator<Expression> iter = conjuncts.iterator();
                        while (iter.hasNext()) {
                            Expression expr = iter.next();
                            if (expr instanceof EqualTo) {
                                eq = (EqualTo) expr;
                            } else if (expr instanceof InPredicate) {
                                inPred = (InPredicate) expr;
                            } else {
                                Assertions.assertFalse(true);
                            }
                        }
                        Assertions.assertNotNull(eq);
                        Assertions.assertNotNull(inPred);
                        Assertions.assertEquals(id, inPred.getCompareExpr());
                        Assertions.assertEquals(ImmutableList.of(i1, i2, i2, i3), inPred.getOptions());
                        return true;
                    }));
    }

    @Test
    void testNotMergeInPredicate() {
        SlotReference id = (SlotReference) scan.getOutput().get(0);
        Expression i1 = new IntegerLiteral(1);
        Expression i2 = new IntegerLiteral(2);
        Expression i3 = new IntegerLiteral(3);
        InPredicate in1 = new InPredicate(id, Lists.newArrayList(i1, i2));
        InPredicate in2 = new InPredicate(id, Lists.newArrayList(i2, i3));
        LogicalPlan filter = new LogicalPlanBuilder(scan)
                .filter(ImmutableSet.of(in1, in2))
                .build();
        PlanChecker.from(new ConnectContext(), filter).applyBottomUp(new MergeInPredicateInFilter())
                .matches(
                        logicalFilter().when(plan -> {
                            Set<Expression> conjuncts = plan.getConjuncts();
                            Assertions.assertEquals(2, conjuncts.size());
                            return true;
                        }));
    }
}
