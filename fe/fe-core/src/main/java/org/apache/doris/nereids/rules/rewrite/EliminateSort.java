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
import org.apache.doris.nereids.trees.plans.logical.LogicalDeferMaterializeTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Eliminate sort that is not directly below result sink, can also eliminate sort under table sink
 * Note we have put limit in sort node so that we don't need to consider limit
 */

public class EliminateSort extends DefaultPlanRewriter<Boolean> implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        // default eliminate = true
        return plan.accept(this, true);
    }

    @Override
    public Plan visit(Plan plan, Boolean pruneSort) {
        if (plan instanceof LogicalTableSink) {
            return doEliminateSort(plan, true);
        }
        return doEliminateSort(plan, pruneSort);
    }

    @Override
    public Plan visitLogicalSort(LogicalSort<? extends Plan> sort, Boolean pruneSort) {
        if (pruneSort) {
            // remove current sort
            return doEliminateSort(sort.child(), true);
        }
        // if pruneSort = false, doesn't remove current sort but need to remove sort under it
        return doEliminateSort(sort, true);
    }

    @Override
    public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink, Boolean context) {
        return doEliminateSort(logicalResultSink, false);
    }

    @Override
    public Plan visitLogicalFileSink(LogicalFileSink<? extends Plan> logicalFileSink, Boolean context) {
        return doEliminateSort(logicalFileSink, false);
    }

    @Override
    public Plan visitLogicalDeferMaterializeTopN(LogicalDeferMaterializeTopN<? extends Plan> topN, Boolean context) {
        return doEliminateSort(topN, false);
    }

    private Plan doEliminateSort(Plan plan, Boolean eliminateSort) {
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
