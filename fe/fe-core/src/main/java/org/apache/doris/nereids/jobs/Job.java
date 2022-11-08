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

package org.apache.doris.nereids.jobs;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Abstract class for all job using for analyze and optimize query plan in Nereids.
 */
public abstract class Job {
    public final Logger logger = LogManager.getLogger(getClass());

    protected JobType type;
    protected JobContext context;
    protected boolean once;
    protected final boolean enableTrace;

    public Job(JobType type, JobContext context) {
        this(type, context, true);
    }

    /** job full parameter constructor */
    public Job(JobType type, JobContext context, boolean once) {
        this.type = type;
        this.context = context;
        this.once = once;
        ConnectContext connectContext = ConnectContext.get();
        this.enableTrace = connectContext == null
                ? false
                : connectContext.getSessionVariable().isEnableNereidsTrace();
    }

    public void pushJob(Job job) {
        context.getCascadesContext().pushJob(job);
    }

    public RuleSet getRuleSet() {
        return context.getCascadesContext().getRuleSet();
    }

    public boolean isOnce() {
        return once;
    }

    /**
     * Get the rule set of this job. Filter out already applied rules and rules that are not matched on root node.
     *
     * @param groupExpression group expression to be applied on
     * @param candidateRules rules to be applied
     * @return all rules that can be applied on this group expression
     */
    public List<Rule> getValidRules(GroupExpression groupExpression,
            List<Rule> candidateRules) {
        return candidateRules.stream()
                .filter(rule -> Objects.nonNull(rule) && rule.getPattern().matchRoot(groupExpression.getPlan())
                        && groupExpression.notApplied(rule)).collect(Collectors.toList());
    }

    public abstract void execute() throws AnalysisException;

    protected Optional<CopyInResult> invokeRewriteRuleWithTrace(Rule rule, Plan before, Group targetGroup) {
        context.onInvokeRule(rule.getRuleType());

        String traceBefore = enableTrace ? getTraceLog(rule) : null;

        List<Plan> afters = rule.transform(before, context.getCascadesContext());
        Preconditions.checkArgument(afters.size() == 1);
        Plan after = afters.get(0);

        if (after != before) {
            CopyInResult result = context.getCascadesContext()
                    .getMemo()
                    .copyIn(after, targetGroup, true);

            if (result.generateNewExpression && enableTrace) {
                String traceAfter = getTraceLog(rule);
                printTraceLog(rule, traceBefore, traceAfter);
            }

            return Optional.of(result);
        }

        return Optional.empty();
    }

    protected String getTraceLog(Rule rule) {
        if (rule.isRewrite()) {
            return context.getCascadesContext()
                    .getMemo()
                    .copyOut(false)
                    .treeString();
        } else {
            return context.getCascadesContext()
                    .getMemo()
                    .getRoot()
                    .treeString();
        }
    }

    protected void printTraceLog(Rule rule, String traceBefore, String traceAfter) {
        logger.info("========== {} {} ==========\nbefore:\n{}\n\nafter:\n{}\n",
                getClass().getSimpleName(), rule.getRuleType(), traceBefore, traceAfter);
    }
}
