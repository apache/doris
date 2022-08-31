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
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Abstract class for all job using for analyze and optimize query plan in Nereids.
 */
public abstract class Job {
    protected JobType type;
    protected JobContext context;

    public Job(JobType type, JobContext context) {
        this.type = type;
        this.context = context;
    }

    public void pushTask(Job job) {
        context.getCascadesContext().pushJob(job);
    }

    public RuleSet getRuleSet() {
        return context.getCascadesContext().getRuleSet();
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
}
