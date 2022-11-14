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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.Job;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.jobs.JobType;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.Objects;

/**
 * Use visitor to rewrite the plan.
 */
public class VisitorRewriteJob extends Job {
    private final Group group;

    private final DefaultPlanRewriter<JobContext> planRewriter;

    /**
     * Constructor.
     */
    public VisitorRewriteJob(CascadesContext cascadesContext, DefaultPlanRewriter<JobContext> rewriter, boolean once) {
        super(JobType.VISITOR_REWRITE, cascadesContext.getCurrentJobContext(), once);
        this.group = Objects.requireNonNull(cascadesContext.getMemo().getRoot(), "group cannot be null");
        this.planRewriter = Objects.requireNonNull(rewriter, "planRewriter cannot be null");
    }

    @Override
    public void execute() {
        GroupExpression logicalExpression = group.getLogicalExpression();
        Plan root = context.getCascadesContext().getMemo().copyOut(logicalExpression, true);
        Plan rewrittenRoot = root.accept(planRewriter, context);
        context.getCascadesContext().getMemo().copyIn(rewrittenRoot, group, true);
    }

}
