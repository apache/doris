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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class EliminateNotNullTest implements MemoPatternMatchSupported {
    private final SlotReference slot = new SlotReference("nullable_col", IntegerType.INSTANCE, true);
    private final LogicalOneRowRelation relation = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(slot));

    @Test
    void testEliminateNotNullForSimplePredicate() {
        Expression simplePredicate = new EqualTo(slot, Literal.of(1));
        Expression explicitNotNull = new Not(new IsNull(slot));
        LogicalPlan plan = new LogicalPlanBuilder(relation)
                .filter(ImmutableSet.of(simplePredicate, explicitNotNull))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalFilter().when(filter -> filter.getConjuncts().size() == 1));
    }

    @Test
    void testKeepNotNullWhenOnlyWidePredicateCanProveIt() {
        Expression widePredicate = new EqualTo(repeatAdd(slot, 257), Literal.of(1));
        Expression explicitNotNull = new Not(new IsNull(slot));
        LogicalPlan plan = new LogicalPlanBuilder(relation)
                .filter(ImmutableSet.of(widePredicate, explicitNotNull))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .applyTopDown(new EliminateNotNull())
                .matches(logicalFilter().when(filter -> filter.getConjuncts().size() == 2));
    }

    private Expression repeatAdd(Expression expression, int width) {
        if (width == 1) {
            return expression;
        }
        int leftWidth = width / 2;
        return new Add(repeatAdd(expression, leftWidth), repeatAdd(expression, width - leftWidth));
    }
}
