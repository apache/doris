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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.util.PlanUtils;

import java.util.List;

/**
 * Project(OneRowRelation) -> OneRowRelation
 */
public class PushProjectIntoOneRowRelation extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalProject(logicalOneRowRelation()).then(project -> {
            LogicalOneRowRelation oneRowRelation = project.child();
            List<NamedExpression> namedExpressions = PlanUtils.mergeProjections(oneRowRelation.getProjects(),
                    project.getProjects());
            return oneRowRelation.withProjects(namedExpressions);

        }).toRule(RuleType.PUSH_PROJECT_INTO_ONE_ROW_RELATION);
    }
}
