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

package org.apache.doris.nereids.jobs.cascades;

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

/**
 * Job to optimize {@link org.apache.doris.nereids.trees.plans.Plan} in {@link org.apache.doris.nereids.memo.Memo}.
 */
public class OptimizeGroupExpressionJob extends Job {
    private final GroupExpression groupExpression;

    public OptimizeGroupExpressionJob(GroupExpression groupExpression, JobContext context) {
        super(JobType.OPTIMIZE_PLAN, context);
        this.groupExpression = groupExpression;
    }

    @Override
    public void execute() {
        if (groupExpression.isUnused()) {
            return;
        }

        countJobExecutionTimesOfGroupExpressions(groupExpression);
        List<Rule> implementationRules = getRuleSet().getImplementationRules();
        List<Rule> explorationRules = getExplorationRules();

        for (Rule rule : explorationRules) {
            if (rule.isInvalid(disableRules, groupExpression)) {
                continue;
            }
            pushJob(new ApplyRuleJob(groupExpression, rule, context));
        }

        for (Rule rule : implementationRules) {
            if (rule.isInvalid(disableRules, groupExpression)) {
                continue;
            }
            pushJob(new ApplyRuleJob(groupExpression, rule, context));
        }
    }

    private List<Rule> getExplorationRules() {
        return ImmutableList.<Rule>builder()
                .addAll(getJoinRules())
                .addAll(getMvRules())
                .build();
    }

    private List<Rule> getJoinRules() {
        boolean isDisableJoinReorder = context.getCascadesContext().getConnectContext().getSessionVariable()
                .isDisableJoinReorder()
                || context.getCascadesContext().isLeadingDisableJoinReorder()
                || context.getCascadesContext().getMemo().getGroupExpressionsSize() > context.getCascadesContext()
                .getConnectContext().getSessionVariable().memoMaxGroupExpressionSize;
        boolean isDpHyp = context.getCascadesContext().getStatementContext().isDpHyp();
        boolean isEnableBushyTree = context.getCascadesContext().getConnectContext().getSessionVariable()
                .isEnableBushyTree();
        boolean isLeftZigZagTree = context.getCascadesContext().getConnectContext()
                .getSessionVariable().isEnableLeftZigZag()
                || (groupExpression.getOwnerGroup() != null && !groupExpression.getOwnerGroup().isStatsReliable());
        int joinNumBushyTree = context.getCascadesContext().getConnectContext()
                .getSessionVariable().getMaxJoinNumBushyTree();
        if (isDisableJoinReorder) {
            return Collections.emptyList();
        } else if (isDpHyp) {
            return getRuleSet().getDPHypReorderRules();
        } else if (isLeftZigZagTree) {
            return getRuleSet().getLeftZigZagTreeJoinReorder();
        } else if (isEnableBushyTree) {
            return getRuleSet().getBushyTreeJoinReorder();
        } else if (context.getCascadesContext().getStatementContext().getMaxNAryInnerJoin() <= joinNumBushyTree) {
            return getRuleSet().getBushyTreeJoinReorder();
        } else {
            return getRuleSet().getZigZagTreeJoinReorder();
        }
    }

    private List<Rule> getMvRules() {
        ConnectContext connectContext = context.getCascadesContext().getConnectContext();
        if (connectContext.getSessionVariable().isEnableMaterializedViewRewrite()) {
            return getRuleSet().getMaterializedViewRules();
        }
        return ImmutableList.of();
    }
}
