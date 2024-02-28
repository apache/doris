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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Eliminate sort that is not directly below result sink
 * Note we have put limit in sort node so that we don't need to consider limit
 */

public class EliminateSort extends DefaultPlanRewriter<Boolean> implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        Boolean eliminateSort = false;
        return plan.accept(this, eliminateSort);
    }

    @Override
    public Plan visit(Plan plan, Boolean pruneSort) {
        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Plan newChild = child.accept(this, true);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? plan.withChildren(newChildren) : plan;
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Boolean eliminateSort) {
        if (eliminateSort) {
            return visit(sort.child(), true);
        }
        return visit(sort, true);
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, Boolean eliminateSort) {
        return skipEliminateSort(project, eliminateSort);
    }

    @Override
    public Plan visitLogicalSink(LogicalSink<? extends Plan> sink, Boolean eliminateSort) {
        // 1. table sink: eliminate -> true
        // 2. sink -> tablesink -> olaptablesink
        return skipEliminateSort(sink, eliminateSort);
    }

    private Plan skipEliminateSort(Plan plan, Boolean eliminateSort) {
        List<Plan> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Plan child : plan.children()) {
            Plan newChild = child.accept(this, eliminateSort);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? plan.withChildren(newChildren) : plan;
    }
}
