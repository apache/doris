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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.IvmNormalizeMTMV;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class IvmPlanSignatureGeneratorTest extends IvmDeltaTestBase {

    @Test
    void testSamePlanSignatureStableAcrossNormalizeRuns() {
        IvmPlanSignature signature1 = signatureForPlan(buildScanRoot(buildMowScan(1, "t")));
        IvmPlanSignature signature2 = signatureForPlan(buildScanRoot(buildMowScan(1, "t")));

        Assertions.assertEquals(signature1.getSha256(), signature2.getSha256());
        Assertions.assertFalse(signature1.getCanonicalString().contains("#"),
                "canonical string should not contain ExprId text");
        Assertions.assertFalse(signature1.getCanonicalString().contains("LogicalProject["),
                "canonical string should not contain debug plan id text");
        Assertions.assertFalse(signature1.getCanonicalString().contains("distinct="),
                "canonical string should not contain project DISTINCT flag");
        Assertions.assertFalse(signature1.getCanonicalString().contains("ivm="),
                "canonical string should not contain derived IVM column marker");
        Assertions.assertFalse(signature1.getCanonicalString().contains("explicit="),
                "canonical string should not contain cast explicit flag");
        Assertions.assertFalse(signature1.getCanonicalString().contains("skew="),
                "canonical string should not contain aggregate skew flag");
        Assertions.assertFalse(signature1.getCanonicalString().contains("unique="),
                "canonical string should not contain scalar function implementation marker");
        Assertions.assertFalse(signature1.getCanonicalString().contains("nullable="),
                "canonical string should not contain nullable flag");
        Assertions.assertFalse(signature1.getCanonicalString().contains("type="),
                "canonical string should not contain type fields");
        Assertions.assertFalse(signature1.getCanonicalString().contains("keysType="),
                "canonical string should not contain scan key type");
    }

    @Test
    void testFilterDoesNotChangeSignature() {
        LogicalOlapScan scan1 = buildMowScan(1, "t");
        IvmPlanSignature withoutFilter = signatureForPlan(buildScanRoot(scan1));

        LogicalOlapScan scan2 = buildMowScan(1, "t");
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(new IntegerLiteral(0), scan2.getOutput().get(0))), scan2);
        IvmPlanSignature withFilter = signatureForPlan(buildScanRoot(filter));

        Assertions.assertEquals(withoutFilter.getSha256(), withFilter.getSha256());
    }

    @Test
    void testOlapTableSinkDoesNotChangeSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> project = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));
        IvmPlanSignature withoutSink = signatureForNormalizedPlan(project);

        LogicalOlapTableSink<Plan> sink = new LogicalOlapTableSink<>(
                new Database(),
                scan.getTable(),
                scan.getTable().getBaseSchema(),
                ImmutableList.of(),
                ImmutableList.copyOf(project.getOutput()),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.NONE,
                project);
        IvmPlanSignature withSink = signatureForNormalizedPlan(sink);

        Assertions.assertEquals(withoutSink.getSha256(), withSink.getSha256());
    }

    @Test
    void testPassthroughProjectsDoNotChangeSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> normalizedProject = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));
        IvmPlanSignature baseSignature = signatureForNormalizedPlan(normalizedProject);

        Slot rowIdSlot = normalizedProject.getOutput().get(0);
        Slot visibleSlot = normalizedProject.getOutput().get(1);
        LogicalProject<?> slotPassthroughProject = new LogicalProject<>(ImmutableList.<NamedExpression>builder()
                .add((NamedExpression) rowIdSlot)
                .add(new Alias(visibleSlot, visibleSlot.getName()))
                .addAll(normalizedProject.getOutput().subList(2, normalizedProject.getOutput().size()))
                .build(), normalizedProject);
        LogicalProject<?> aliasPassthroughProject = new LogicalProject<>(ImmutableList.<NamedExpression>builder()
                .add(new Alias(rowIdSlot, Column.IVM_ROW_ID_COL))
                .addAll(slotPassthroughProject.getOutput().subList(1, slotPassthroughProject.getOutput().size()))
                .build(), slotPassthroughProject);

        IvmPlanSignature passthroughSignature = signatureForNormalizedPlan(aliasPassthroughProject);
        Assertions.assertEquals(baseSignature.getSha256(), passthroughSignature.getSha256());
    }

    @Test
    void testSinkAdapterProjectDoesNotChangeSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> normalizedProject = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));
        IvmPlanSignature baseSignature = signatureForNormalizedPlan(normalizedProject);

        LogicalProject<?> sinkAdapterProject = new LogicalProject<>(ImmutableList.<NamedExpression>builder()
                .add(new Alias(new IntegerLiteral(0), Column.DELETE_SIGN))
                .add(new Alias(normalizedProject.getOutput().get(0), Column.IVM_ROW_ID_COL))
                .add(new Alias(new IntegerLiteral(0), Column.VERSION_COL))
                .add(new Alias(normalizedProject.getOutput().get(1), normalizedProject.getOutput().get(1).getName()))
                .add(new Alias(normalizedProject.getOutput().get(2), normalizedProject.getOutput().get(2).getName()))
                .build(), normalizedProject);

        IvmPlanSignature sinkAdapterSignature = signatureForNormalizedPlan(sinkAdapterProject);
        Assertions.assertEquals(baseSignature.getSha256(), sinkAdapterSignature.getSha256());
    }

    @Test
    void testSinkWithAdapterProjectDoesNotChangeSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> normalizedProject = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));
        IvmPlanSignature baseSignature = signatureForNormalizedPlan(normalizedProject);

        LogicalProject<?> sinkAdapterProject = new LogicalProject<>(ImmutableList.<NamedExpression>builder()
                .add(new Alias(new IntegerLiteral(0), Column.DELETE_SIGN))
                .add(new Alias(normalizedProject.getOutput().get(0), Column.IVM_ROW_ID_COL))
                .add(new Alias(new IntegerLiteral(0), Column.VERSION_COL))
                .add(new Alias(normalizedProject.getOutput().get(1), normalizedProject.getOutput().get(1).getName()))
                .add(new Alias(normalizedProject.getOutput().get(2), normalizedProject.getOutput().get(2).getName()))
                .build(), normalizedProject);
        LogicalOlapTableSink<Plan> sink = new LogicalOlapTableSink<>(
                new Database(),
                scan.getTable(),
                scan.getTable().getBaseSchema(),
                ImmutableList.of(),
                ImmutableList.copyOf(sinkAdapterProject.getOutput()),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.NONE,
                sinkAdapterProject);

        IvmPlanSignature sinkSignature = signatureForNormalizedPlan(sink);
        Assertions.assertEquals(baseSignature.getSha256(), sinkSignature.getSha256());
    }

    @Test
    void testRenamedAliasProjectChangesSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> normalizedProject = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));
        IvmPlanSignature baseSignature = signatureForNormalizedPlan(normalizedProject);

        Slot visibleSlot = normalizedProject.getOutput().get(1);
        LogicalProject<?> renamedAliasProject = new LogicalProject<>(ImmutableList.<NamedExpression>builder()
                .add((NamedExpression) normalizedProject.getOutput().get(0))
                .add(new Alias(visibleSlot, visibleSlot.getName() + "_renamed"))
                .addAll(normalizedProject.getOutput().subList(2, normalizedProject.getOutput().size()))
                .build(), normalizedProject);

        IvmPlanSignature renamedAliasSignature = signatureForNormalizedPlan(renamedAliasProject);
        Assertions.assertNotEquals(baseSignature.getSha256(), renamedAliasSignature.getSha256());
    }

    @Test
    void testSubQueryAliasNameDoesNotChangeSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> normalizedProject = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));

        IvmPlanSignature baseSignature = signatureForNormalizedPlan(normalizedProject);
        IvmPlanSignature aliasASignature = signatureForNormalizedPlan(
                new LogicalSubQueryAlias<>("alias_a", normalizedProject));
        IvmPlanSignature aliasBSignature = signatureForNormalizedPlan(
                new LogicalSubQueryAlias<>("alias_b", normalizedProject));

        Assertions.assertEquals(baseSignature.getSha256(), aliasASignature.getSha256());
        Assertions.assertEquals(aliasASignature.getSha256(), aliasBSignature.getSha256());
    }

    @Test
    void testNestedSubQueryAliasNameDoesNotChangeSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        LogicalProject<?> normalizedProject = buildRowIdProject(scan,
                ImmutableList.of(scan.getOutput().get(0), scan.getOutput().get(1)));

        IvmPlanSignature singleAliasSignature = signatureForNormalizedPlan(
                new LogicalSubQueryAlias<>("alias_a", normalizedProject));
        IvmPlanSignature nestedAliasSignature = signatureForNormalizedPlan(
                new LogicalSubQueryAlias<>("alias_outer",
                        new LogicalSubQueryAlias<>("alias_inner", normalizedProject)));

        Assertions.assertEquals(singleAliasSignature.getSha256(), nestedAliasSignature.getSha256());
    }

    @Test
    void testAliasNameDoesNotChangeExpressionSignature() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        Slot slot = scan.getOutput().get(0);
        IvmPlanSignatureGenerator generator = new IvmPlanSignatureGenerator();

        String aliasA = generator.canonicalExpression(new Alias(slot, "alias_a"));
        String aliasB = generator.canonicalExpression(new Alias(slot, "alias_b"));

        Assertions.assertEquals(aliasA, aliasB);
        Assertions.assertFalse(aliasA.contains("alias_a"));
        Assertions.assertFalse(aliasB.contains("alias_b"));
        Assertions.assertFalse(aliasA.contains(" AS "));
    }

    @Test
    void testSlotTypeAndNullabilityDoNotChangeExpressionSignature() {
        IvmPlanSignatureGenerator generator = new IvmPlanSignatureGenerator();
        SlotReference nullable = new SlotReference(StatementScopeIdGenerator.newExprId(),
                "k1", IntegerType.INSTANCE, true, ImmutableList.of("db", "t"));
        SlotReference nonNullableWithDifferentType = new SlotReference(StatementScopeIdGenerator.newExprId(),
                "k1", BigIntType.INSTANCE, false, ImmutableList.of("db", "t"));

        Assertions.assertEquals(
                generator.canonicalExpression(nullable),
                generator.canonicalExpression(nonNullableWithDifferentType));
    }

    @Test
    void testGenericExpressionSignatureDoesNotRecordNonLayoutFlags() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        Slot slot = scan.getOutput().get(0);
        IvmPlanSignatureGenerator generator = new IvmPlanSignatureGenerator();

        String cast = generator.canonicalExpression(new Cast(slot, IntegerType.INSTANCE, true));
        String lessThan = generator.canonicalExpression(new LessThan(slot, new IntegerLiteral(1)));

        Assertions.assertTrue(cast.contains("class=Cast"));
        Assertions.assertTrue(cast.contains("sql=cast("));
        Assertions.assertTrue(cast.contains("as INT"));
        Assertions.assertTrue(lessThan.contains("class=LessThan"));
        Assertions.assertTrue(lessThan.contains("sql=(id < 1)"));
        Assertions.assertFalse(cast.contains("explicit="));
        Assertions.assertFalse(cast.contains("type="));
        Assertions.assertFalse(cast.contains("nullable="));
        Assertions.assertFalse(lessThan.contains("nullable="));
    }

    @Test
    void testProjectVisibleOutputOrderDoesNotChangeSignature() {
        LogicalOlapScan scan1 = buildMowScan(1, "t");
        IvmPlanSignature idName = signatureForPlan(buildScanRoot(scan1,
                ImmutableList.copyOf(scan1.getOutput())));

        LogicalOlapScan scan2 = buildMowScan(1, "t");
        IvmPlanSignature nameId = signatureForPlan(buildScanRoot(scan2,
                ImmutableList.of(scan2.getOutput().get(1), scan2.getOutput().get(0))));

        Assertions.assertEquals(idName.getSha256(), nameId.getSha256());
    }

    @Test
    void testProjectRowIdExpressionOrderChangesSignature() {
        LogicalOlapScan scan1 = buildMowScan(1, "t");
        IvmPlanSignature idName = signatureForNormalizedPlan(buildRowIdProject(scan1,
                ImmutableList.of(scan1.getOutput().get(0), scan1.getOutput().get(1))));

        LogicalOlapScan scan2 = buildMowScan(1, "t");
        IvmPlanSignature nameId = signatureForNormalizedPlan(buildRowIdProject(scan2,
                ImmutableList.of(scan2.getOutput().get(1), scan2.getOutput().get(0))));

        Assertions.assertNotEquals(idName.getSha256(), nameId.getSha256());
    }

    @Test
    void testInnerJoinChildOrderChangesSignature() {
        LogicalOlapScan scanA = buildMowScan(1, "a");
        LogicalOlapScan scanB = buildMowScan(2, "b");
        LogicalJoin<?, ?> joinAB = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanA, scanB, JoinReorderContext.EMPTY);

        LogicalOlapScan scanA2 = buildMowScan(1, "a");
        LogicalOlapScan scanB2 = buildMowScan(2, "b");
        LogicalJoin<?, ?> joinBA = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(), scanB2, scanA2, JoinReorderContext.EMPTY);

        Assertions.assertNotEquals(
                signatureForPlan(buildScanRoot(joinAB)).getSha256(),
                signatureForPlan(buildScanRoot(joinBA)).getSha256());
    }

    @Test
    void testJoinTypeChangesSignature() {
        LogicalOlapScan left1 = buildMowScan(1, "l");
        LogicalOlapScan right1 = buildMowScan(2, "r");
        LogicalJoin<?, ?> innerJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(left1.getOutput().get(0), right1.getOutput().get(0))),
                left1, right1, JoinReorderContext.EMPTY);

        LogicalOlapScan left2 = buildMowScan(1, "l");
        LogicalOlapScan right2 = buildMowScan(2, "r");
        LogicalJoin<?, ?> leftOuterJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(left2.getOutput().get(0), right2.getOutput().get(0))),
                left2, right2, JoinReorderContext.EMPTY);

        Assertions.assertNotEquals(
                signatureForPlan(buildScanRoot(innerJoin)).getSha256(),
                signatureForPlan(buildScanRoot(leftOuterJoin)).getSha256());
    }

    @Test
    void testInnerJoinPredicateDoesNotChangeSignature() {
        LogicalOlapScan scanA1 = buildMowScan(1, "a");
        LogicalOlapScan scanB1 = buildMowScan(2, "b");
        LogicalJoin<?, ?> idJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(scanA1.getOutput().get(0), scanB1.getOutput().get(0))),
                scanA1, scanB1, JoinReorderContext.EMPTY);

        LogicalOlapScan scanA2 = buildMowScan(1, "a");
        LogicalOlapScan scanB2 = buildMowScan(2, "b");
        LogicalJoin<?, ?> nameJoin = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(scanA2.getOutput().get(1), scanB2.getOutput().get(1))),
                scanA2, scanB2, JoinReorderContext.EMPTY);

        IvmPlanSignature idSignature = signatureForPlan(buildScanRoot(idJoin));
        IvmPlanSignature nameSignature = signatureForPlan(buildScanRoot(nameJoin));

        Assertions.assertFalse(idSignature.getCanonicalString().contains("hashConjuncts"));
        Assertions.assertEquals(idSignature.getSha256(), nameSignature.getSha256());
    }

    @Test
    void testUnionArmOrderChangesSignature() {
        LogicalUnion unionAB = buildUnionAll(buildMowScan(1, "a"), buildMowScan(2, "b"));
        LogicalUnion unionBA = buildUnionAll(buildMowScan(2, "b"), buildMowScan(1, "a"));

        Assertions.assertNotEquals(
                signatureForPlan(buildScanRoot(unionAB)).getSha256(),
                signatureForPlan(buildScanRoot(unionBA)).getSha256());
    }

    @Test
    void testAggregateSignatureUsesNormalizedHiddenOutputsWithoutAggMeta() {
        PlanBundle bundle = normalizeAggPlan(buildGroupedAgg(buildMowScan(1, "t")));
        IvmPlanSignature signature = bundle.rewriteResult.getPlanSignature();

        Assertions.assertFalse(signature.getCanonicalString().contains("AGG_META"));
        Assertions.assertFalse(signature.getCanonicalString().contains("aggType="));
        Assertions.assertTrue(signature.getCanonicalString().contains(Column.IVM_AGG_COUNT_COL));
        Assertions.assertTrue(signature.getCanonicalString().contains("__DORIS_IVM_AGG_2_SUM_COL__"));
    }

    @Test
    void testRepeatSignatureDoesNotRecordHiddenOutputs() {
        LogicalOlapScan scan = buildMowScan(1, "t");
        Slot idSlot = scan.getOutput().get(0);
        Slot nameSlot = scan.getOutput().get(1);
        LogicalRepeat<LogicalOlapScan> repeat = new LogicalRepeat<>(
                ImmutableList.of(
                        ImmutableList.of(idSlot, nameSlot),
                        ImmutableList.of(idSlot),
                        ImmutableList.of()),
                ImmutableList.of(idSlot, nameSlot),
                RepeatType.GROUPING_SETS,
                scan);

        IvmPlanSignature signature = signatureForNormalizedPlan(repeat);

        Assertions.assertTrue(signature.getCanonicalString().contains("plan=REPEAT[child=SCAN"));
        Assertions.assertFalse(signature.getCanonicalString().contains("REPEAT[hiddenOutputs"));
    }

    @Test
    void testOuterJoinRepairModeDoesNotChangeSignature() {
        LogicalOlapScan left1 = buildMowScan(1, "l");
        LogicalOlapScan right1 = buildMowScan(2, "r");
        LogicalJoin<?, ?> equiJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(left1.getOutput().get(0), right1.getOutput().get(0))),
                left1, right1, JoinReorderContext.EMPTY);

        LogicalOlapScan left2 = buildMowScan(1, "l");
        LogicalOlapScan right2 = buildMowScan(2, "r");
        LogicalJoin<?, ?> nonEquiJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), ImmutableList.of(new LessThan(right2.getOutput().get(0),
                        left2.getOutput().get(0))), left2, right2, JoinReorderContext.EMPTY);

        IvmPlanSignature rightEvents = signatureForPlan(buildScanRoot(equiJoin));
        IvmPlanSignature repairBranches = signatureForPlan(buildScanRoot(nonEquiJoin));

        Assertions.assertFalse(rightEvents.getCanonicalString().contains("repairMode"));
        Assertions.assertFalse(repairBranches.getCanonicalString().contains("repairMode"));
        Assertions.assertEquals(rightEvents.getSha256(), repairBranches.getSha256());
    }

    @Test
    void testOuterJoinRepairBranchPredicateDoesNotChangeSignature() {
        LogicalOlapScan left1 = buildMowScan(1, "l");
        LogicalOlapScan right1 = buildMowScan(2, "r");
        LogicalJoin<?, ?> lessThanIdJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), ImmutableList.of(new LessThan(right1.getOutput().get(0),
                        left1.getOutput().get(0))), left1, right1, JoinReorderContext.EMPTY);

        LogicalOlapScan left2 = buildMowScan(1, "l");
        LogicalOlapScan right2 = buildMowScan(2, "r");
        LogicalJoin<?, ?> lessThanNameJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(), ImmutableList.of(new LessThan(right2.getOutput().get(1),
                        left2.getOutput().get(1))), left2, right2, JoinReorderContext.EMPTY);

        IvmPlanSignature idSignature = signatureForPlan(buildScanRoot(lessThanIdJoin));
        IvmPlanSignature nameSignature = signatureForPlan(buildScanRoot(lessThanNameJoin));

        Assertions.assertFalse(idSignature.getCanonicalString().contains("repairMode"));
        Assertions.assertFalse(idSignature.getCanonicalString().contains("otherConjuncts"));
        Assertions.assertEquals(idSignature.getSha256(), nameSignature.getSha256());
    }

    private IvmPlanSignature signatureForPlan(Plan root) {
        ConnectContext ctx = newConnectContext();
        JobContext jobContext = newJobContextForRoot(root, ctx);
        new IvmNormalizeMTMV().rewriteRoot(root, jobContext);
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult().get();
        return rewriteResult.getPlanSignature();
    }

    private IvmPlanSignature signatureForNormalizedPlan(Plan normalizedPlan) {
        return new IvmPlanSignatureGenerator().generate(normalizedPlan);
    }

    private LogicalResultSink<?> buildScanRoot(Plan plan) {
        ImmutableList<NamedExpression> exprs = ImmutableList.copyOf(plan.getOutput());
        return buildScanRoot(plan, exprs);
    }

    private LogicalResultSink<?> buildScanRoot(Plan plan, List<? extends NamedExpression> exprs) {
        ImmutableList<NamedExpression> outputExprs = ImmutableList.copyOf(exprs);
        LogicalProject<?> project = new LogicalProject<>(outputExprs, plan);
        return new LogicalResultSink<>(outputExprs, project);
    }

    private LogicalProject<?> buildRowIdProject(LogicalOlapScan scan, List<? extends Expression> rowIdArgs) {
        Alias rowIdAlias = new Alias(IvmUtil.buildRowIdHash(rowIdArgs), Column.IVM_ROW_ID_COL);
        ImmutableList<NamedExpression> exprs = ImmutableList.<NamedExpression>builder()
                .add(rowIdAlias)
                .addAll(scan.getOutput())
                .build();
        return new LogicalProject<>(exprs, scan);
    }

    private LogicalOlapScan buildMowScan(long tableId, String name) {
        OlapTable table = PlanConstructor.newOlapTable(tableId, name, 0, KeysType.UNIQUE_KEYS);
        table.setEnableUniqueKeyMergeOnWrite(true);
        enableRowBinlog(table);
        table.setQualifiedDbName("test_db");
        return new LogicalOlapScan(PlanConstructor.getNextRelationId(), table,
                ImmutableList.of("test_db"));
    }

    private LogicalUnion buildUnionAll(Plan... children) {
        List<Slot> firstOutput = children[0].getOutput();
        ImmutableList.Builder<NamedExpression> outputs = ImmutableList.builder();
        for (Slot slot : firstOutput) {
            outputs.add(new SlotReference(StatementScopeIdGenerator.newExprId(),
                    slot.getName(), slot.getDataType(), slot.nullable(), ImmutableList.of()));
        }
        ImmutableList.Builder<List<SlotReference>> childrenOutputs = ImmutableList.builder();
        for (Plan child : children) {
            ImmutableList.Builder<SlotReference> childMapping = ImmutableList.builder();
            for (Slot slot : child.getOutput()) {
                childMapping.add((SlotReference) slot);
            }
            childrenOutputs.add(childMapping.build());
        }
        return new LogicalUnion(Qualifier.ALL, outputs.build(), childrenOutputs.build(),
                ImmutableList.of(), false, ImmutableList.copyOf(children));
    }
}
