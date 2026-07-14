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
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.mtmv.ivm.IvmPlanSignature;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmRewriteResult;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.qe.ConnectContext;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
        validatePlanSignature(context.getMtmv(), rewriteResult);
        Plan rewritten = rewriteIncrementalPlan(plan, rewriteResult, context,
                jobContext.getCascadesContext().getConnectContext());
        rewriteResult.setIncrRefreshRewritten(true);
        return rewritten;
    }

    private Plan rewriteIncrementalPlan(Plan plan, IvmRewriteResult rewriteResult,
            IvmRewriteContext rewriteContext, ConnectContext connectContext) {
        if (!(plan instanceof LogicalOlapTableSink)) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM incremental refresh requires LogicalOlapTableSink root, but found "
                            + plan.getClass().getSimpleName());
        }
        LogicalOlapTableSink<?> sink = (LogicalOlapTableSink<?>) plan;
        MTMV mtmv = rewriteContext.getMtmv();
        if (sink.getTargetTable().getId() != mtmv.getId()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM incremental refresh target table mismatch, sink=" + sink.getTargetTable().getName()
                            + ", mtmv=" + mtmv.getName());
        }
        Plan rewrittenSinkChild = newDeltaRewriter().generateIncrementalRefreshPlan(
                sink.child(), rewriteResult, rewriteContext, connectContext);
        List<NamedExpression> reboundOutputExprs = IvmDeltaRewriteHelper.INSTANCE.rebindSinkOutputs(
                sink.getOutputExprs(), rewrittenSinkChild.getOutput(), "sink");
        return sink.withOutputExprs(reboundOutputExprs).withChildren(Collections.singletonList(rewrittenSinkChild));
    }

    protected IvmDeltaRewriter newDeltaRewriter() {
        return new IvmDeltaRewriter();
    }

    private void validatePlanSignature(MTMV mtmv, IvmRewriteResult rewriteResult) {
        IvmPlanSignature currentSignature = rewriteResult.getPlanSignature();
        IvmInfo ivmInfo = mtmv.getIvmInfo();
        String storedSignature = ivmInfo.getPlanSignature();
        boolean signatureMatched = currentSignature != null
                && Objects.equals(storedSignature, currentSignature.getSha256());
        if (signatureMatched) {
            return;
        }
        String detail = "IVM layout signature mismatch for mv=" + mtmv.getName()
                + ", storedSignature=" + storedSignature
                + ", currentSignature=" + (currentSignature == null ? "null" : currentSignature.getSha256())
                + ". Run a full refresh to rebuild IVM layout baseline.";
        throw new IvmException(IvmFailureReason.PLAN_SIGNATURE_MISMATCH, detail);
    }
}
