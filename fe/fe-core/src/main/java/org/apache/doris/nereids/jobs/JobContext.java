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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.rewrite.RewriteJob;
import org.apache.doris.nereids.jobs.scheduler.ScheduleContext;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;

import com.google.common.collect.Maps;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Context for one job in Nereids' cascades framework.
 */
public class JobContext {

    // use for optimizer
    protected final ScheduleContext scheduleContext;
    protected final PhysicalProperties requiredProperties;
    protected double costUpperBound;

    // use for rewriter
    protected boolean rewritten = false;
    protected List<RewriteJob> remainJobs = Collections.emptyList();

    // user for trace
    protected Map<RuleType, Integer> ruleInvokeTimes = Maps.newLinkedHashMap();

    public JobContext(ScheduleContext scheduleContext, PhysicalProperties requiredProperties, double costUpperBound) {
        this.scheduleContext = scheduleContext;
        this.requiredProperties = requiredProperties;
        this.costUpperBound = costUpperBound;
    }

    public ScheduleContext getScheduleContext() {
        return scheduleContext;
    }

    public CascadesContext getCascadesContext() {
        return (CascadesContext) scheduleContext;
    }

    public PhysicalProperties getRequiredProperties() {
        return requiredProperties;
    }

    public double getCostUpperBound() {
        return costUpperBound;
    }

    public void setCostUpperBound(double costUpperBound) {
        this.costUpperBound = costUpperBound;
    }

    public boolean isRewritten() {
        return rewritten;
    }

    public void setRewritten(boolean rewritten) {
        this.rewritten = rewritten;
    }

    public List<RewriteJob> getRemainJobs() {
        return remainJobs;
    }

    public void setRemainJobs(List<RewriteJob> remainJobs) {
        this.remainJobs = remainJobs;
    }

    public void onInvokeRule(RuleType ruleType) {
        addRuleInvokeTimes(ruleType);
    }

    private void addRuleInvokeTimes(RuleType ruleType) {
        Integer times = ruleInvokeTimes.get(ruleType);
        if (times == null) {
            times = 0;
        }
        ruleInvokeTimes.put(ruleType, times + 1);
    }
}
