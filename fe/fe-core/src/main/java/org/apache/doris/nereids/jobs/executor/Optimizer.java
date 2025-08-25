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

package org.apache.doris.nereids.jobs.executor;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.jobs.cascades.OptimizeGroupJob;
import org.apache.doris.nereids.jobs.joinorder.JoinOrderJob;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.util.MoreFieldsThread;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import java.util.Objects;

/**
 * Cascades style optimize:
 * Perform equivalent logical plan exploration and physical implementation enumeration,
 * try to find best plan under the guidance of statistic information and cost model.
 */
public class Optimizer {

    private final CascadesContext cascadesContext;

    public Optimizer(CascadesContext cascadesContext) {
        this.cascadesContext = Objects.requireNonNull(cascadesContext, "cascadesContext cannot be null");
    }

    /**
     * execute optimize, use dphyp or cascades according to join number and session variables.
     */
    public void execute() {
        MoreFieldsThread.keepFunctionSignature(() -> {
            // init memo
            cascadesContext.toMemo();
            // stats derive
            cascadesContext.getMemo().getRoot().getLogicalExpressions().forEach(groupExpression ->
                    cascadesContext.pushJob(
                            new DeriveStatsJob(groupExpression, cascadesContext.getCurrentJobContext())));
            cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            if (cascadesContext.getStatementContext().isDpHyp() || isDpHyp(cascadesContext)) {
                // RightNow, dp hyper can only order 64 join operators
                dpHypOptimize();
            }
            // Cascades optimize
            cascadesContext.pushJob(
                    new OptimizeGroupJob(cascadesContext.getMemo().getRoot(), cascadesContext.getCurrentJobContext()));
            cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
            return null;
        });
    }

    /**
     * This method calc the result that if use dp hyper or not
     */
    public static boolean isDpHyp(CascadesContext cascadesContext) {
        boolean optimizeWithUnknownColStats = false;
        if (ConnectContext.get() != null && ConnectContext.get().getStatementContext() != null) {
            if (ConnectContext.get().getStatementContext().isHasUnknownColStats()) {
                optimizeWithUnknownColStats = true;
            }
        }
        // DPHyp optimize
        SessionVariable sessionVariable = cascadesContext.getConnectContext().getSessionVariable();
        int maxTableCount = sessionVariable.getMaxTableCountUseCascadesJoinReorder();
        if (optimizeWithUnknownColStats) {
            // if column stats are unknown, 10~20 table-join is optimized by cascading framework
            maxTableCount = 2 * maxTableCount;
        }
        int continuousJoinNum = Memo.countMaxContinuousJoin(cascadesContext.getRewritePlan());
        cascadesContext.getStatementContext().setMaxContinuousJoin(continuousJoinNum);
        boolean isDpHyp = sessionVariable.enableDPHypOptimizer || continuousJoinNum > maxTableCount;
        boolean finalEnableDpHyp = !sessionVariable.isDisableJoinReorder()
                && !cascadesContext.isLeadingDisableJoinReorder()
                && continuousJoinNum <= sessionVariable.getMaxJoinNumberOfReorder()
                && isDpHyp;
        cascadesContext.getStatementContext().setDpHyp(finalEnableDpHyp);
        return finalEnableDpHyp;
    }

    private void dpHypOptimize() {
        Group root = cascadesContext.getMemo().getRoot();
        // Due to EnsureProjectOnTopJoin, root group can't be Join Group, so DPHyp doesn't change the root group
        cascadesContext.pushJob(new JoinOrderJob(root, cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }

    private SessionVariable getSessionVariable() {
        return cascadesContext.getConnectContext().getSessionVariable();
    }
}
