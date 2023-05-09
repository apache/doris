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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

/**
 * add default order by for limit
 * select * from t limit 10 offset 5
 * to: select * from t order by {keys} limit 10 offset 5
 */
public class AddDefaultOrderByForLimit extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private boolean needOrderBy = false;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return null;
    }

    @Override
    public LogicalPlan visitLogicalLimit(LogicalLimit limit, JobContext context) {
        needOrderBy = true;
        if (limit.child() instanceof LogicalSort) {
            return limit;
        }
        needOrderBy = true;
        return ((LogicalPlan) super.visit(limit, context));
    }
}
