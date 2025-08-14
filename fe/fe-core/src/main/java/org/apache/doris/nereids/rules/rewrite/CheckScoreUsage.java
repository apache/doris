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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

/**
 * Check score function usage in project without proper optimization context.
 *
 * This rule ensures that score() function is used only in contexts where it can be optimized,
 * requiring WHERE clause with MATCH function, ORDER BY and LIMIT.
 */
public class CheckScoreUsage extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(any())
                .when(project -> hasScoreFunction(project) && !isScoreAlreadyOptimized(project))
                .then(project -> {
                    throw new AnalysisException(
                            "score() function requires WHERE clause with MATCH function, "
                            + "ORDER BY and LIMIT for optimization");
                }).toRule(RuleType.CHECK_SCORE_USAGE);
    }

    private boolean hasScoreFunction(LogicalProject<?> project) {
        return project.getProjects().stream()
                .anyMatch(projection -> {
                    if (projection instanceof Alias) {
                        return ((Alias) projection).child() instanceof Score;
                    }
                    return false;
                });
    }

    private boolean isScoreAlreadyOptimized(LogicalProject<?> project) {
        Plan child = project.child();
        if (child instanceof LogicalOlapScan) {
            LogicalOlapScan scan = (LogicalOlapScan) child;
            return scan.getVirtualColumns().stream()
                    .anyMatch(virtualCol -> {
                        if (virtualCol instanceof Alias) {
                            Expression childExpr = ((Alias) virtualCol).child();
                            return childExpr instanceof Score;
                        }
                        return false;
                    });
        }
        return false;
    }
}
