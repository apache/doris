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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

/**
 * Before:
 *          project
 *             │
 *             ▼
 *           limit
 *             │
 *             ▼
 *          plan node
 *
 * After:
 *
 *           limit
 *             │
 *             ▼
 *          project
 *             │
 *             ▼
 *          plan node
 */
public class PushdownProjectThroughLimit extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(logicalLimit(group())).thenApply(ctx -> {
            LogicalProject<LogicalLimit<GroupPlan>> logicalProject = ctx.root;
            LogicalLimit<GroupPlan> logicalLimit = logicalProject.child();
            return new LogicalLimit<LogicalProject<GroupPlan>>(logicalLimit.getLimit(),
                    logicalLimit.getOffset(), new LogicalProject<>(logicalProject.getProjects(),
                    logicalLimit.child()));
        }).toRule(RuleType.PUSHDOWN_PROJECT_THROUGHT_LIMIT);
    }
}
