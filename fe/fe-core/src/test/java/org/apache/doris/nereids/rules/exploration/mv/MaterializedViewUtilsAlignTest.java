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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link MaterializedViewUtils#alignPlanOutputWithOrigin}.
 */
public class MaterializedViewUtilsAlignTest {

    @Test
    public void testSameConstantWithDifferentExprIdIsAccepted() {
        ExprId originExprId = new ExprId(10);
        Plan origin = project(new Alias(originExprId, new IntegerLiteral(1), "k1"));
        Plan rewritten = project(new Alias(new ExprId(11), new IntegerLiteral(1), "k1"));

        Plan aligned = MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin);

        Assertions.assertNotNull(aligned);
        Assertions.assertEquals(originExprId, aligned.getOutput().get(0).getExprId());
        Assertions.assertEquals("k1", aligned.getOutput().get(0).getName());
    }

    @Test
    public void testSameNameDifferentConstantIsRejected() {
        Plan origin = project(new Alias(new ExprId(10), new IntegerLiteral(1), "k1"));
        Plan rewritten = project(new Alias(new ExprId(11), new IntegerLiteral(2), "k1"));

        Assertions.assertNull(MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin));
    }

    @Test
    public void testSameColumnWithSameQualifierIsAccepted() {
        ExprId originExprId = new ExprId(10);
        Plan origin = project(slot(originExprId, ImmutableList.of("a")));
        Plan rewritten = project(slot(new ExprId(11), ImmutableList.of("a")));

        Plan aligned = MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin);

        Assertions.assertNotNull(aligned);
        Assertions.assertEquals(originExprId, aligned.getOutput().get(0).getExprId());
        Assertions.assertEquals("k1", aligned.getOutput().get(0).getName());
    }

    @Test
    public void testSameNameColumnOfDifferentRelationIsRejected() {
        Plan origin = project(slot(new ExprId(10), ImmutableList.of("a")));
        Plan rewritten = project(slot(new ExprId(11), ImmutableList.of("b")));

        Assertions.assertNull(MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin));
    }

    @Test
    public void testSameNameColumnWithoutQualifierIsRejected() {
        // The rewritten output lost the qualifier, so it can not be told apart from the same named column
        // of another relation, e.g. the other relation of a self join.
        Plan origin = project(slot(new ExprId(10), ImmutableList.of("a")));
        Plan rewritten = project(slot(new ExprId(11), ImmutableList.of()));

        Assertions.assertNull(MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin));
    }

    @Test
    public void testSameNameColumnWithoutQualifierOnBothSidesIsRejected() {
        // Both sides lost the qualifier, so the outputs can still be the same named columns of two
        // different relations.
        Plan origin = project(slot(new ExprId(10), ImmutableList.of()));
        Plan rewritten = project(slot(new ExprId(11), ImmutableList.of()));

        Assertions.assertNull(MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin));
    }

    private static SlotReference slot(ExprId exprId, ImmutableList<String> qualifier) {
        return new SlotReference(exprId, "k1", IntegerType.INSTANCE, false, qualifier);
    }

    private static Plan project(NamedExpression... projects) {
        return new LogicalProject<>(ImmutableList.copyOf(projects),
                new LogicalOneRowRelation(new RelationId(1), ImmutableList.of()));
    }
}
