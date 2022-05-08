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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.memo.PlanReference;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;

import java.util.List;

/**
 * Abstract class for all job using for analyze and optimize query plan in Nereids.
 */
public abstract class Job {
    protected JobType type;
    protected PlannerContext context;

    public Job(JobType type, PlannerContext context) {
        this.type = type;
        this.context = context;
    }

    public void pushTask(Job job) {
        context.getOptimizerContext().pushTask(job);
    }

    public RuleSet getRuleSet() {
        return context.getOptimizerContext().getRuleSet();
    }

    public void prunedInvalidRules(PlanReference planReference, List<Rule> candidateRules) {

    }

    public abstract void execute() throws AnalysisException;
}
