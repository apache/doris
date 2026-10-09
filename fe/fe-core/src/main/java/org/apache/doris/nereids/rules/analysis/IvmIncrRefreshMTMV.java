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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.ivm.IvmDeltaRewriteHelper;
import org.apache.doris.mtmv.ivm.IvmDeltaRewriter;
import org.apache.doris.mtmv.ivm.IvmDryRunLimit;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Rewrites an internal IVM refresh INSERT query into the incremental delta query.
 */
public class IvmIncrRefreshMTMV implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        StatementContext statementContext = jobContext.getCascadesContext().getStatementContext();
        Optional<IvmRewriteContext> rewriteContext = statementContext.getIvmRewriteContext();
        if (!rewriteContext.isPresent()
                || rewriteContext.get().getMode() != IvmRewriteContext.Mode.INCREMENTAL) {
            return plan;
        }
        IvmRewriteResult rewriteResult = jobContext.getCascadesContext().getIvmRewriteResult()
                .orElseThrow(() -> new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                        "IVM incremental refresh requires normalize result"));
        if (rewriteResult.isIncrRefreshRewritten()) {
            return plan;
        }
        IvmRewriteContext context = rewriteContext.get();
        Plan rewritten = rewriteIncrementalPlan(plan, rewriteResult, context,
                jobContext.getCascadesContext().getConnectContext());
        CloudTableStreamReadStateHook.installReadStates(rewritten);
        rewriteResult.setIncrRefreshRewritten(true);
        return rewritten;
    }

    private Plan rewriteIncrementalPlan(Plan plan, IvmRewriteResult rewriteResult,
            IvmRewriteContext rewriteContext, ConnectContext connectContext) {
        if (!(plan instanceof LogicalOlapTableSink)) {
            if (!rewriteContext.isDryRun()) {
                throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                        "IVM incremental refresh requires LogicalOlapTableSink root, but found "
                                + plan.getClass().getSimpleName());
            }
            // Dry run: the root is the raw MV query (no sink wrapper). Rewrite it in place,
            // then cap rows with an optional LogicalLimit and wrap with a result sink so the
            // plan is executable as a plain query.
            Plan rewritten = newDeltaRewriter().generateIncrRefreshPlan(
                    plan, rewriteResult, rewriteContext, connectContext);
            Optional<IvmDryRunLimit> dryRunLimit = rewriteContext.getDryRunLimit();
            if (dryRunLimit.isPresent()) {
                IvmDryRunLimit limit = dryRunLimit.get();
                rewritten = new LogicalLimit<>(limit.getCount(), limit.getOffset(),
                        LimitPhase.ORIGIN, (LogicalPlan) rewritten);
            }
            return new LogicalResultSink<Plan>(
                    ImmutableList.copyOf(rewritten.getOutput()), (LogicalPlan) rewritten);
        }
        LogicalOlapTableSink<?> sink = (LogicalOlapTableSink<?>) plan;
        MTMV mtmv = rewriteContext.getMtmv();
        if (sink.getTargetTable().getId() != mtmv.getId()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM incremental refresh target table mismatch, sink=" + sink.getTargetTable().getName()
                            + ", mtmv=" + mtmv.getName());
        }
        Plan rewrittenSinkChild = newDeltaRewriter().generateIncrRefreshPlan(
                sink.child(), rewriteResult, rewriteContext, connectContext);
        List<NamedExpression> reboundOutputExprs = IvmDeltaRewriteHelper.INSTANCE.rebindSinkOutputs(
                sink.getOutputExprs(), rewrittenSinkChild.getOutput(), "sink");
        return sink.withOutputExprs(reboundOutputExprs)
                .withChildren(Collections.singletonList(rewrittenSinkChild));
    }

    protected IvmDeltaRewriter newDeltaRewriter() {
        return new IvmDeltaRewriter();
    }
}
