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
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class MergeOneRowRelationIntoUnionTest {

    @Test
    public void testMergeUsesRegularChildOutputToBuildConstantExprs() {
        Alias firstProject = new Alias(new ExprId(1), new IntegerLiteral(10), "first_col");
        Alias secondProject = new Alias(new ExprId(2), new IntegerLiteral(20), "second_col");
        SlotReference secondProjectSlot = (SlotReference) secondProject.toSlot();
        LogicalOneRowRelation oneRowRelation = new LogicalOneRowRelation(
                new RelationId(1), ImmutableList.of(firstProject, secondProject));

        SlotReference unionOutput = new SlotReference(new ExprId(10), "second_col",
                IntegerType.INSTANCE, false, ImmutableList.of());
        LogicalUnion union = new LogicalUnion(Qualifier.ALL,
                ImmutableList.of(unionOutput),
                ImmutableList.of(ImmutableList.of(secondProjectSlot)),
                ImmutableList.of(),
                false,
                ImmutableList.of(oneRowRelation));

        Plan rewritten = PlanChecker.from(MemoTestUtils.createConnectContext(), union)
                .applyTopDown(new MergeOneRowRelationIntoUnion())
                .getPlan();

        Assertions.assertInstanceOf(LogicalUnion.class, rewritten);
        LogicalUnion rewrittenUnion = (LogicalUnion) rewritten;
        Assertions.assertEquals(0, rewrittenUnion.children().size());
        Assertions.assertEquals(1, rewrittenUnion.getConstantExprsList().size());

        List<NamedExpression> constantExprs = rewrittenUnion.getConstantExprsList().get(0);
        Assertions.assertEquals(1, constantExprs.size());
        Assertions.assertEquals(secondProject.getExprId(), constantExprs.get(0).getExprId());
        Assertions.assertInstanceOf(IntegerLiteral.class, constantExprs.get(0).child(0));
        Assertions.assertEquals(20, ((IntegerLiteral) constantExprs.get(0).child(0)).getValue());
    }
}
