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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.types.IntegerType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test for {@link MaterializedViewUtils#alignPlanOutputWithOrigin} and
 * {@link MaterializedViewUtils#normalizeSinkExpressions}.
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

    @Test
    public void testSameNameColumnOfDifferentSubPathIsRejected() {
        // variant['a'] and variant['b'] have the same name, type and qualifier but are different columns.
        Plan origin = project(slot(new ExprId(10), ImmutableList.of("a")).withSubPath(ImmutableList.of("a")));
        Plan rewritten = project(slot(new ExprId(11), ImmutableList.of("a")).withSubPath(ImmutableList.of("b")));

        Assertions.assertNull(MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin));
    }

    @Test
    public void testSameNameColumnOfSameSubPathIsAccepted() {
        ExprId originExprId = new ExprId(10);
        Plan origin = project(slot(originExprId, ImmutableList.of("a")).withSubPath(ImmutableList.of("b")));
        Plan rewritten = project(slot(new ExprId(11), ImmutableList.of("a")).withSubPath(ImmutableList.of("b")));

        Plan aligned = MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin);

        Assertions.assertNotNull(aligned);
        Assertions.assertEquals(originExprId, aligned.getOutput().get(0).getExprId());
    }

    @Test
    public void testFileSinkOutputWithFreshExprIdIsMaterialized() {
        ExprId originExprId = new ExprId(10);
        ExprId rewrittenExprId = new ExprId(11);
        SlotReference rewrittenSlot = slot(rewrittenExprId, ImmutableList.of("a"));
        LogicalFileSink<Plan> rewritten = fileSink(ImmutableList.of(rewrittenSlot), project(rewrittenSlot));
        LogicalFileSink<Plan> origin = fileSink(ImmutableList.of(slot(originExprId, ImmutableList.of("a"))),
                oneRow());

        Plan normalized = MaterializedViewUtils.normalizeSinkExpressions(rewritten, origin);

        Assertions.assertNotNull(normalized);
        Assertions.assertEquals(originExprId, normalized.getOutput().get(0).getExprId());
        Assertions.assertTrue(normalized.child(0) instanceof LogicalProject);
        assertSinkOutputsResolvable(normalized);
        assertSinkOutputExpressionsResolvable(normalized);
    }

    @Test
    public void testSinkWithOnlyNullablePropertyChangedDoesNotAddProject() {
        ExprId exprId = new ExprId(10);
        SlotReference childOutput = new SlotReference(exprId, "k1", IntegerType.INSTANCE, true,
                ImmutableList.of("a"));
        LogicalOneRowRelation child = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(childOutput));
        LogicalFileSink<Plan> rewritten = fileSink(ImmutableList.of(childOutput), child);
        LogicalFileSink<Plan> origin = fileSink(ImmutableList.of(
                new SlotReference(exprId, "k1", IntegerType.INSTANCE, false, ImmutableList.of("a"))), oneRow());

        Plan normalized = MaterializedViewUtils.normalizeSinkExpressions(rewritten, origin);

        Assertions.assertNotNull(normalized);
        Assertions.assertSame(child, normalized.child(0));
        Assertions.assertFalse(normalized.getOutput().get(0).nullable());
        assertSinkOutputExpressionsResolvable(normalized);
    }

    @Test
    public void testMaterializedProjectKeepsChildNullableForPassThroughOutput() {
        ExprId originFirst = new ExprId(10);
        ExprId rewrittenFirst = new ExprId(11);
        ExprId shared = new ExprId(12);
        SlotReference childFirst = slot(rewrittenFirst, ImmutableList.of("a"));
        SlotReference childShared = new SlotReference(shared, "k2", IntegerType.INSTANCE, true,
                ImmutableList.of("a"));
        LogicalOneRowRelation child = new LogicalOneRowRelation(new RelationId(1),
                ImmutableList.of(childFirst, childShared));
        LogicalFileSink<Plan> rewritten = fileSink(ImmutableList.of(childFirst, childShared), child);
        LogicalFileSink<Plan> origin = fileSink(ImmutableList.of(
                slot(originFirst, ImmutableList.of("a")),
                new SlotReference(shared, "k2", IntegerType.INSTANCE, false, ImmutableList.of("a"))), oneRow());

        Plan normalized = MaterializedViewUtils.normalizeSinkExpressions(rewritten, origin);

        Assertions.assertNotNull(normalized);
        Assertions.assertEquals(originFirst, normalized.getOutput().get(0).getExprId());
        Assertions.assertFalse(normalized.getOutput().get(1).nullable());
        Plan project = normalized.child(0);
        Assertions.assertTrue(project instanceof LogicalProject);
        // the materialized project passes the output which the child already produces through with the
        // nullable property of the child, so merging the projects does not change the logical properties
        Assertions.assertTrue(project.getOutput().get(1).nullable());
        assertSinkOutputsResolvable(normalized);
        assertSinkOutputExpressionsResolvable(normalized);
    }

    @Test
    public void testAlignSinkMaterializesOutputOfFreshExprId() {
        ExprId originExprId = new ExprId(10);
        ExprId rewrittenExprId = new ExprId(11);
        SlotReference rewrittenSlot = slot(rewrittenExprId, ImmutableList.of("a"));
        LogicalFileSink<Plan> rewritten = fileSink(ImmutableList.of(rewrittenSlot),
                new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(rewrittenSlot)));
        LogicalFileSink<Plan> origin = fileSink(ImmutableList.of(slot(originExprId, ImmutableList.of("a"))),
                oneRow());

        Plan aligned = MaterializedViewUtils.alignPlanOutputWithOrigin(rewritten, origin);

        Assertions.assertNotNull(aligned);
        Assertions.assertEquals(originExprId, aligned.getOutput().get(0).getExprId());
        assertSinkOutputsResolvable(aligned);
        assertSinkOutputExpressionsResolvable(aligned);
    }

    @Test
    public void testStaleSinkOutputUsesChildOutput() {
        ExprId originExprId = new ExprId(11);
        ExprId staleExprId = new ExprId(2);
        SlotReference childOutput = slot(originExprId, ImmutableList.of("a"));
        LogicalOneRowRelation child = new LogicalOneRowRelation(new RelationId(1), ImmutableList.of(childOutput));
        // the sink still outputs the exprId from before a rule changed the child output, the child does
        // not produce this exprId any more
        LogicalFileSink<Plan> rewritten = fileSink(ImmutableList.of(slot(staleExprId, ImmutableList.of("a"))), child);
        LogicalFileSink<Plan> origin = fileSink(ImmutableList.of(slot(originExprId, ImmutableList.of("a"))), oneRow());

        Plan normalized = MaterializedViewUtils.normalizeSinkExpressions(rewritten, origin);

        Assertions.assertNotNull(normalized);
        Assertions.assertEquals(originExprId, normalized.getOutput().get(0).getExprId());
        Assertions.assertSame(child, normalized.child(0));
        assertSinkOutputsResolvable(normalized);
        assertSinkOutputExpressionsResolvable(normalized);
    }

    private static void assertSinkOutputsResolvable(Plan sinkPlan) {
        // PhysicalFileSink resolves every output exprId in the child, see
        // PhysicalPlanTranslator.visitPhysicalFileSink and translateOutputExprs
        Set<ExprId> childOutputExprIds = sinkPlan.child(0).getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        for (Slot sinkOutput : sinkPlan.getOutput()) {
            Assertions.assertTrue(childOutputExprIds.contains(sinkOutput.getExprId()),
                    "the child must produce the sink output exprId " + sinkOutput.getExprId());
        }
    }

    private static void assertSinkOutputExpressionsResolvable(Plan sinkPlan) {
        Set<ExprId> childOutputExprIds = sinkPlan.child(0).getOutput().stream()
                .map(Slot::getExprId).collect(Collectors.toSet());
        for (NamedExpression output : ((LogicalSink<?>) sinkPlan).getOutputExprs()) {
            for (Slot inputSlot : output.getInputSlots()) {
                Assertions.assertTrue(childOutputExprIds.contains(inputSlot.getExprId()),
                        "the sink output expression references a slot not produced by the child: " + inputSlot);
            }
        }
    }

    private static SlotReference slot(ExprId exprId, ImmutableList<String> qualifier) {
        return new SlotReference(exprId, "k1", IntegerType.INSTANCE, false, qualifier);
    }

    private static LogicalFileSink<Plan> fileSink(List<NamedExpression> outputs, Plan child) {
        return new LogicalFileSink<>("file:///tmp/materialized_view_align_test", "csv",
                ImmutableMap.of(), outputs, child);
    }

    private static LogicalOneRowRelation oneRow() {
        return new LogicalOneRowRelation(new RelationId(1), ImmutableList.of());
    }

    private static Plan project(NamedExpression... projects) {
        return new LogicalProject<>(ImmutableList.copyOf(projects), oneRow());
    }
}
