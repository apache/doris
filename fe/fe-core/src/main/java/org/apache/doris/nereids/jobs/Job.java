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

import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.model.Statistics;

import com.google.common.base.Preconditions;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract class for all job using for analyze and optimize query plan in Nereids.
 */
public abstract class Job {
    protected JobType type;
    protected JobContext context;
    protected boolean once;
    protected final BitSet disableRules;

    protected Map<CTEId, Statistics> cteIdToStats;

    public Job(JobType type, JobContext context) {
        this(type, context, true);
    }

    /** job full parameter constructor */
    public Job(JobType type, JobContext context, boolean once) {
        this.type = type;
        this.context = context;
        this.once = once;
        this.disableRules = getDisableRules(context);
    }

    public void pushJob(Job job) {
        context.getScheduleContext().pushJob(job);
    }

    public RuleSet getRuleSet() {
        return context.getCascadesContext().getRuleSet();
    }

    public boolean isOnce() {
        return once;
    }

    public ConnectContext getConnectContext() {
        return context.getCascadesContext().getConnectContext();
    }

    public abstract void execute();

    protected Optional<CopyInResult> invokeRewriteRule(Rule rule, Plan before, Group targetGroup) {
        context.onInvokeRule(rule.getRuleType());
        List<Plan> afters = rule.transform(before, context.getCascadesContext());
        Preconditions.checkArgument(afters.size() == 1);
        Plan after = afters.get(0);
        if (after == before) {
            return Optional.empty();
        }

        CopyInResult result = context.getCascadesContext()
                .getMemo()
                .copyIn(after, targetGroup, rule.isRewrite(),
                        context.getCascadesContext().getStatementContext().isDpHyp());

        return Optional.of(result);
    }

    public static BitSet getDisableRules(JobContext context) {
        return context.getCascadesContext().getAndCacheDisableRules();
    }
}
