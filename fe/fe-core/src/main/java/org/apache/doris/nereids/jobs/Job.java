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

import org.apache.doris.nereids.PlannerContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.trees.TreeNode;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Abstract class for all job using for analyze and optimize query plan in Nereids.
 */
public abstract class Job<NODE_TYPE extends TreeNode<NODE_TYPE>> {
    protected JobType type;
    protected PlannerContext context;

    public Job(JobType type, PlannerContext context) {
        this.type = type;
        this.context = context;
    }

    public void pushTask(Job job) {
        context.getOptimizerContext().pushJob(job);
    }

    public RuleSet getRuleSet() {
        return context.getOptimizerContext().getRuleSet();
    }

    /**
     * Get the rule set of this job. Filter out already applied rules and rules that are not matched on root node.
     *
     * @param groupExpression group expression to be applied on
     * @param candidateRules rules to be applied
     * @return all rules that can be applied on this group expression
     */
    public List<Rule<NODE_TYPE>> getValidRules(GroupExpression groupExpression,
            List<Rule<NODE_TYPE>> candidateRules) {
        return candidateRules.stream()
                .filter(rule -> Objects.nonNull(rule) && rule.getPattern().matchOperator(groupExpression.getOperator())
                        && groupExpression.notApplied(rule)).collect(Collectors.toList());
    }

    public abstract void execute() throws AnalysisException;
}
