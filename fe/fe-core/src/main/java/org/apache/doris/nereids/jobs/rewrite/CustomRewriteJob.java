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

package org.apache.doris.nereids.jobs.rewrite;

import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Custom rewrite the plan.
 * Just pass the plan node to the 'CustomRewriter', and the 'CustomRewriter' rule will handle it.
 * The 'CustomRewriter' rule use the 'Visitor' design pattern to implement the rule.
 * You can check the 'CustomRewriter' interface to see which rules use this way to do rewrite.
 */
public class CustomRewriteJob implements RewriteJob {

    private final RuleType ruleType;
    private final Supplier<CustomRewriter> customRewriter;

    /**
     * Constructor.
     */
    public CustomRewriteJob(Supplier<CustomRewriter> rewriter, RuleType ruleType) {
        this.ruleType = Objects.requireNonNull(ruleType, "ruleType cannot be null");
        this.customRewriter = Objects.requireNonNull(rewriter, "customRewriter cannot be null");
    }

    @Override
    public void execute(JobContext context) {
        Set<Integer> disableRules = Job.getDisableRules(context);
        if (disableRules.contains(ruleType.type())) {
            return;
        }
        Plan root = context.getCascadesContext().getRewritePlan();
        // COUNTER_TRACER.log(CounterEvent.of(Memo.get=-StateId(), CounterType.JOB_EXECUTION, group, logicalExpression,
        //         root));
        Plan rewrittenRoot = customRewriter.get().rewriteRoot(root, context);
        if (rewrittenRoot == null) {
            return;
        }

        // don't remove this comment, it can help us to trace some bug when developing.

        // if (!root.deepEquals(rewrittenRoot)) {
        //     String traceBefore = root.treeString();
        //     String traceAfter = root.treeString();
        //     printTraceLog(ruleType, traceBefore, traceAfter);
        // }
        context.getCascadesContext().setRewritePlan(rewrittenRoot);
    }

    @Override
    public boolean isOnce() {
        return false;
    }

    private void printTraceLog(RuleType ruleType, String traceBefore, String traceAfter) {
        System.out.println("========== " + getClass().getSimpleName() + " " + ruleType
                + " ==========\nbefore:\n" + traceBefore + "\n\nafter:\n" + traceAfter + "\n");
    }
}
