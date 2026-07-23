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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.ivm.IvmDeltaRewriter;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Optional;

class IvmIncrRefreshMTMVTest {
    private static final long MTMV_ID = 100L;
    private static final String SIGNATURE = "abc";

    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final MTMV mtmv = mockMtmv(MTMV_ID, SIGNATURE);

    @Test
    void testNoRewriteContextKeepsPlanUnchanged() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        RecordingRule rule = new RecordingRule(scan);

        Plan result = rule.rewriteRoot(sink, newJobContext(sink, null, newRewriteResult(SIGNATURE)));

        Assertions.assertSame(sink, result);
        Assertions.assertEquals(0, rule.rewriter.callCount);
    }

    @Test
    void testCreateRewriteContextKeepsPlanUnchanged() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        RecordingRule rule = new RecordingRule(scan);
        IvmRewriteContext context = new IvmRewriteContext(IvmRewriteContext.Mode.NORMALIZE, null, false);

        Plan result = rule.rewriteRoot(sink, newJobContext(sink, context, newRewriteResult(SIGNATURE)));

        Assertions.assertSame(sink, result);
        Assertions.assertEquals(0, rule.rewriter.callCount);
    }

    @Test
    void testMissingRewriteResultThrows() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        RecordingRule rule = new RecordingRule(scan);

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> rule.rewriteRoot(sink, newJobContext(sink,
                        IvmRewriteContext.incremental(mtmv, false), null)));

        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, exception.getFailureReason());
        Assertions.assertEquals(0, rule.rewriter.callCount);
    }

    @Test
    void testSignatureMismatchThrowsBeforeDeltaRewrite() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        RecordingRule rule = new RecordingRule(scan);

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> rule.rewriteRoot(sink, newJobContext(sink,
                        IvmRewriteContext.incremental(mtmv, false), newRewriteResult("new"))));

        Assertions.assertEquals(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("storedSignature=" + SIGNATURE));
        Assertions.assertTrue(exception.getMessage().contains("currentSignature=new"));
        Assertions.assertEquals(0, rule.rewriter.callCount);
    }

    @Test
    void testNonSinkRootThrows() {
        RecordingRule rule = new RecordingRule(scan);

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> rule.rewriteRoot(scan, newJobContext(scan,
                        IvmRewriteContext.incremental(mtmv, false), newRewriteResult(SIGNATURE))));

        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("LogicalOlapTableSink"));
        Assertions.assertEquals(0, rule.rewriter.callCount);
    }

    @Test
    void testTargetTableMismatchThrows() {
        LogicalOlapTableSink<Plan> sink = newSink(mockMtmv(200L, SIGNATURE), scan);
        RecordingRule rule = new RecordingRule(scan);

        IvmException exception = Assertions.assertThrows(IvmException.class,
                () -> rule.rewriteRoot(sink, newJobContext(sink,
                        IvmRewriteContext.incremental(mtmv, false), newRewriteResult(SIGNATURE))));

        Assertions.assertEquals(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, exception.getFailureReason());
        Assertions.assertTrue(exception.getMessage().contains("target table mismatch"));
        Assertions.assertEquals(0, rule.rewriter.callCount);
    }

    @Test
    void testReplacesSinkChildWithDeltaPlanAndMarksRunOnce() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        LogicalProject<Plan> deltaPlan = new LogicalProject<>(
                ImmutableList.of(new org.apache.doris.nereids.trees.expressions.Alias(
                        scan.getOutput().get(0), scan.getOutput().get(0).getName())), scan);
        RecordingRule rule = new RecordingRule(deltaPlan);
        IvmRewriteResult rewriteResult = newRewriteResult(SIGNATURE);
        JobContext jobContext = newJobContext(sink,
                IvmRewriteContext.incremental(mtmv, true), rewriteResult);

        Plan result = rule.rewriteRoot(sink, jobContext);

        Assertions.assertInstanceOf(LogicalOlapTableSink.class, result);
        LogicalOlapTableSink<?> rewrittenSink = (LogicalOlapTableSink<?>) result;
        Assertions.assertSame(deltaPlan, rewrittenSink.child());
        Assertions.assertEquals(deltaPlan.getOutput().get(0).getExprId(),
                rewrittenSink.getOutputExprs().get(0).getExprId());
        Assertions.assertTrue(rewriteResult.isIncrRefreshRewritten());
        Assertions.assertEquals(1, rule.rewriter.callCount);
        Assertions.assertSame(sink.child(), rule.rewriter.normalizedPlan);
        Assertions.assertSame(rewriteResult, rule.rewriter.rewriteResult);
        Assertions.assertTrue(rule.rewriter.rewriteContext.isIncludeExhaustedStreams());
    }

    @Test
    void testPreservesSinkAdapterProjectWhenRewritingDeltaPlan() {
        LogicalProject<Plan> adapterProject = new LogicalProject<>(ImmutableList.of(
                new org.apache.doris.nereids.trees.expressions.Alias(new IntegerLiteral(0), Column.DELETE_SIGN),
                new org.apache.doris.nereids.trees.expressions.Alias(scan.getOutput().get(0), "id"),
                new org.apache.doris.nereids.trees.expressions.Alias(new IntegerLiteral(0), Column.VERSION_COL)
        ), scan);
        LogicalOlapTableSink<Plan> sink = new LogicalOlapTableSink<>(
                new Database(),
                mtmv,
                ImmutableList.of(new Column("id", scan.getOutput().get(0).getDataType().toCatalogDataType())),
                new ArrayList<>(),
                ImmutableList.of((NamedExpression) scan.getOutput().get(0)),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.NONE,
                adapterProject);
        LogicalProject<Plan> deltaPlan = new LogicalProject<>(
                ImmutableList.of(
                        new org.apache.doris.nereids.trees.expressions.Alias(scan.getOutput().get(0), "id")
                ), scan);
        RecordingRule rule = new RecordingRule(deltaPlan);

        Plan result = rule.rewriteRoot(sink, newJobContext(sink,
                IvmRewriteContext.incremental(mtmv, false), newRewriteResult(SIGNATURE)));

        LogicalOlapTableSink<?> rewrittenSink = (LogicalOlapTableSink<?>) result;
        Assertions.assertSame(deltaPlan, rewrittenSink.child());
        Assertions.assertEquals(1, rewrittenSink.getOutputExprs().size());
        Assertions.assertEquals("id", rewrittenSink.getOutputExprs().get(0).getName());
        Assertions.assertSame(adapterProject, rule.rewriter.normalizedPlan);
    }

    @Test
    void testRunOnceGuardSkipsSecondRewrite() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        LogicalProject<Plan> deltaPlan = new LogicalProject<>(
                ImmutableList.of((NamedExpression) scan.getOutput().get(0)), scan);
        RecordingRule rule = new RecordingRule(deltaPlan);
        IvmRewriteResult rewriteResult = newRewriteResult(SIGNATURE);
        JobContext jobContext = newJobContext(sink,
                IvmRewriteContext.incremental(mtmv, false), rewriteResult);

        Plan firstResult = rule.rewriteRoot(sink, jobContext);
        Plan secondResult = rule.rewriteRoot(firstResult, jobContext);

        Assertions.assertSame(firstResult, secondResult);
        Assertions.assertEquals(1, rule.rewriter.callCount);
    }

    @Test
    void testNullDeltaPlanFallsBackToEmptyRelation() {
        LogicalOlapTableSink<Plan> sink = newSink(mtmv, scan);
        RecordingRule rule = new RecordingRule(null);

        Plan result = rule.rewriteRoot(sink, newJobContext(sink,
                IvmRewriteContext.incremental(mtmv, false), newRewriteResult(SIGNATURE)));

        Assertions.assertInstanceOf(LogicalOlapTableSink.class, result);
        LogicalOlapTableSink<?> rewrittenSink = (LogicalOlapTableSink<?>) result;
        Assertions.assertInstanceOf(LogicalEmptyRelation.class, rewrittenSink.child());
        Assertions.assertEquals(sink.child().getOutput().size(), rewrittenSink.child().getOutput().size());
        Assertions.assertEquals(1, rule.rewriter.callCount);
    }

    @Test
    void testIncrementalContextRejectsNullMtmv() {
        Assertions.assertThrows(NullPointerException.class,
                () -> IvmRewriteContext.incremental(null, false));
    }

    private JobContext newJobContext(Plan root, IvmRewriteContext rewriteContext,
            IvmRewriteResult rewriteResult) {
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        StatementContext statementContext = new StatementContext(connectContext, null);
        connectContext.setStatementContext(statementContext);
        if (rewriteContext != null) {
            statementContext.setIvmRewriteContext(Optional.of(rewriteContext));
        }
        CascadesContext cascadesContext = CascadesContext.initContext(statementContext, root, PhysicalProperties.ANY);
        if (rewriteResult != null) {
            cascadesContext.setIvmRewriteResult(rewriteResult);
        }
        return new JobContext(cascadesContext, PhysicalProperties.ANY);
    }

    private LogicalOlapTableSink<Plan> newSink(MTMV target, Plan child) {
        return new LogicalOlapTableSink<>(
                new Database(),
                target,
                ImmutableList.of(new Column("id", scan.getOutput().get(0).getDataType().toCatalogDataType())),
                new ArrayList<>(),
                ImmutableList.of((NamedExpression) scan.getOutput().get(0)),
                false,
                TPartialUpdateNewRowPolicy.APPEND,
                DMLCommandType.NONE,
                child);
    }

    private IvmRewriteResult newRewriteResult(String signature) {
        IvmRewriteResult rewriteResult = new IvmRewriteResult();
        rewriteResult.setPlanSignature(new IvmPlanSignature("canonical", signature));
        return rewriteResult;
    }

    private static MTMV mockMtmv(long id, String signature) {
        MTMV mtmv = Mockito.mock(MTMV.class);
        IvmInfo ivmInfo = new IvmInfo();
        ivmInfo.setPlanSignature(signature);
        Mockito.when(mtmv.getId()).thenReturn(id);
        Mockito.when(mtmv.getName()).thenReturn("mv_" + id);
        Mockito.when(mtmv.getIvmInfo()).thenReturn(ivmInfo);
        return mtmv;
    }

    private static class RecordingRule extends IvmIncrRefreshMTMV {
        private final RecordingDeltaRewriter rewriter;

        private RecordingRule(Plan deltaPlan) {
            this.rewriter = new RecordingDeltaRewriter(deltaPlan);
        }

        @Override
        protected IvmDeltaRewriter newDeltaRewriter() {
            return rewriter;
        }
    }

    private static class RecordingDeltaRewriter extends IvmDeltaRewriter {
        private final Plan deltaPlan;
        private int callCount;
        private Plan normalizedPlan;
        private IvmRewriteResult rewriteResult;
        private IvmRewriteContext rewriteContext;

        private RecordingDeltaRewriter(Plan deltaPlan) {
            this.deltaPlan = deltaPlan;
        }

        @Override
        public Plan generateIncrRefreshPlan(Plan normalizedPlan, IvmRewriteResult rewriteResult,
                IvmRewriteContext rewriteContext, ConnectContext connectContext) {
            callCount++;
            this.normalizedPlan = normalizedPlan;
            this.rewriteResult = rewriteResult;
            this.rewriteContext = rewriteContext;
            if (deltaPlan == null) {
                return new LogicalEmptyRelation(
                        connectContext.getStatementContext().getNextRelationId(), normalizedPlan.getOutput());
            }
            return deltaPlan;
        }
    }
}
