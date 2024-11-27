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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

/**
 * EliminateDistinctConstant.
 * <p>
 * example sql:
 * <pre>
 * select distinct 1,2,3 from tbl
 *          =>
 * select 1,2,3 from (select 1, 2, 3 from tbl limit 1) as tmp
 *  </pre>
 */
public class EliminateDistinctConstant extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.ELIMINATE_DISTINCT_CONSTANT.build(
                logicalProject()
                        .when(LogicalProject::isDistinct)
                        .when(project -> project.getProjects().stream().allMatch(Expression::isConstant))
                        .then(project -> new LogicalProject(project.getProjects(), new LogicalLimit<>(1, 0,
                                LimitPhase.ORIGIN, project.child())))
        );
    }
}
