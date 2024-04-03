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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.metrics.CounterType;
import org.apache.doris.nereids.metrics.EventChannel;
import org.apache.doris.nereids.metrics.EventProducer;
import org.apache.doris.nereids.metrics.TracerSupplier;
import org.apache.doris.nereids.metrics.consumer.LogConsumer;
import org.apache.doris.nereids.metrics.enhancer.AddCounterEventEnhancer;
import org.apache.doris.nereids.metrics.event.CounterEvent;
import org.apache.doris.nereids.metrics.event.TransformEvent;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.trees.expressions.CTEId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Abstract class for all job using for analyze and optimize query plan in Nereids.
 */
public abstract class Job implements TracerSupplier {
    // counter tracer to count expression transform times.
    protected static final EventProducer COUNTER_TRACER = new EventProducer(CounterEvent.class,
            EventChannel.getDefaultChannel()
                    .addEnhancers(new AddCounterEventEnhancer())
                    .addConsumers(new LogConsumer(CounterEvent.class, EventChannel.LOG)));
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

    public EventProducer getEventTracer() {
        throw new UnsupportedOperationException("get_event_tracer is unsupported");
    }

    protected Optional<CopyInResult> invokeRewriteRuleWithTrace(Rule rule, Plan before, Group targetGroup) {
        context.onInvokeRule(rule.getRuleType());
        COUNTER_TRACER.log(CounterEvent.of(Memo.getStateId(),
                CounterType.EXPRESSION_TRANSFORM, targetGroup, targetGroup.getLogicalExpression(), before));

        List<Plan> afters = rule.transform(before, context.getCascadesContext());
        Preconditions.checkArgument(afters.size() == 1);
        Plan after = afters.get(0);
        if (after == before) {
            return Optional.empty();
        }

        CopyInResult result = context.getCascadesContext()
                .getMemo()
                .copyIn(after, targetGroup, rule.isRewrite());

        if (result.generateNewExpression || result.correspondingExpression.getOwnerGroup() != targetGroup) {
            getEventTracer().log(TransformEvent.of(targetGroup.getLogicalExpression(), before, afters,
                            rule.getRuleType()), rule::isRewrite);
        }

        return Optional.of(result);
    }

    /**
     * count the job execution times of groupExpressions, all groupExpressions will be inclusive.
     * TODO: count a specific groupExpression.
     * @param groupExpression the groupExpression at current job.
     */
    protected void countJobExecutionTimesOfGroupExpressions(GroupExpression groupExpression) {
        COUNTER_TRACER.log(CounterEvent.of(Memo.getStateId(), CounterType.JOB_EXECUTION,
                groupExpression.getOwnerGroup(), groupExpression, groupExpression.getPlan()));
    }

    public static BitSet getDisableRules(JobContext context) {
        return context.getCascadesContext().getAndCacheDisableRules();
    }
}
