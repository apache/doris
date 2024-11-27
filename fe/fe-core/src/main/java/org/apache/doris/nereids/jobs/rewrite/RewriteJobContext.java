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

import org.apache.doris.nereids.trees.plans.Plan;

import javax.annotation.Nullable;

/** RewriteJobContext */
public class RewriteJobContext {

    final boolean childrenVisited;
    final int batchId;
    final RewriteJobContext parentContext;
    final int childIndexInParentContext;
    final Plan plan;
    final RewriteJobContext[] childrenContext;
    Plan result;

    /** RewriteJobContext */
    public RewriteJobContext(Plan plan, @Nullable RewriteJobContext parentContext, int childIndexInParentContext,
            boolean childrenVisited, int batchId) {
        this.plan = plan;
        this.parentContext = parentContext;
        this.childIndexInParentContext = childIndexInParentContext;
        this.childrenVisited = childrenVisited;
        this.childrenContext = new RewriteJobContext[plan.arity()];
        if (parentContext != null) {
            parentContext.childrenContext[childIndexInParentContext] = this;
        }
        this.batchId = batchId;
    }

    public void setResult(Plan result) {
        this.result = result;
    }

    public RewriteJobContext withChildrenVisited(boolean childrenVisited) {
        return new RewriteJobContext(plan, parentContext, childIndexInParentContext, childrenVisited, batchId);
    }

    public RewriteJobContext withPlan(Plan plan) {
        return new RewriteJobContext(plan, parentContext, childIndexInParentContext, childrenVisited, batchId);
    }

    public RewriteJobContext withPlanAndChildrenVisited(Plan plan, boolean childrenVisited) {
        return new RewriteJobContext(plan, parentContext, childIndexInParentContext, childrenVisited, batchId);
    }

    public boolean isRewriteRoot() {
        return false;
    }
}
