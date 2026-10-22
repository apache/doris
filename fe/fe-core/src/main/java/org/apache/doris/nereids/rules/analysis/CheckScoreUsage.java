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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;

/**
 * Check the uses of score() that BindScore cannot see in the parsed plan.
 *
 * <ul>
 *   <li>A score() call that analysis only expands later, e.g. in the body of an alias function, is not
 *   bound to a score column, and would read 0. Only the score column of a scan may compute score().</li>
 *   <li>A correlated subquery cannot read a score: a SELECT in a subquery calls score() under ORDER BY
 *   score() with a LIMIT, and decorrelation can remove that LIMIT.</li>
 * </ul>
 */
public class CheckScoreUsage implements CustomRewriter {
    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        plan.foreach(node -> {
            if (!(node instanceof LogicalOlapScan) && ((Plan) node).getExpressions().stream()
                    .anyMatch(expression -> expression.containsType(Score.class))) {
                throw new AnalysisException(BindScore.UNSUPPORTED_SELECT_ERROR);
            }
            if (node instanceof LogicalApply && ((LogicalApply<?, ?>) node).isCorrelated()
                    && ((LogicalApply<?, ?>) node).right().anyMatch(child -> child instanceof LogicalOlapScan
                            && BindScore.findScoreSlot((LogicalOlapScan) child).isPresent())) {
                throw new AnalysisException(BindScore.CORRELATED_SUBQUERY_ERROR);
            }
        });
        return plan;
    }
}
