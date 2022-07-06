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

package org.apache.doris.nereids;

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.scheduler.JobPool;
import org.apache.doris.nereids.jobs.scheduler.JobScheduler;
import org.apache.doris.nereids.jobs.scheduler.JobStack;
import org.apache.doris.nereids.jobs.scheduler.SimpleJobScheduler;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.qe.ConnectContext;

/**
 * Context used in memo.
 */
public class PlannerContext {
    private final Memo memo;
    private final ConnectContext connectContext;
    private RuleSet ruleSet;
    private JobPool jobPool;
    private final JobScheduler jobScheduler;
    private JobContext currentJobContext;

    /**
     * Constructor of OptimizerContext.
     *
     * @param memo {@link Memo} reference
     */
    public PlannerContext(Memo memo, ConnectContext connectContext) {
        this.memo = memo;
        this.connectContext = connectContext;
        this.ruleSet = new RuleSet();
        this.jobPool = new JobStack();
        this.jobScheduler = new SimpleJobScheduler();
    }

    public void pushJob(Job job) {
        jobPool.push(job);
    }

    public Memo getMemo() {
        return memo;
    }

    public ConnectContext getConnectContext() {
        return connectContext;
    }

    public RuleSet getRuleSet() {
        return ruleSet;
    }

    public void setRuleSet(RuleSet ruleSet) {
        this.ruleSet = ruleSet;
    }

    public JobPool getJobPool() {
        return jobPool;
    }

    public void setJobPool(JobPool jobPool) {
        this.jobPool = jobPool;
    }

    public JobScheduler getJobScheduler() {
        return jobScheduler;
    }

    public JobContext getCurrentJobContext() {
        return currentJobContext;
    }

    public void setCurrentJobContext(JobContext currentJobContext) {
        this.currentJobContext = currentJobContext;
    }
}
