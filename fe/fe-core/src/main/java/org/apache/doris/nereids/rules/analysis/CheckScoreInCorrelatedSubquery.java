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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;

/**
 * Reject score() in a correlated subquery.
 *
 * <p>A SELECT in a subquery calls score() under ORDER BY score() with a LIMIT, see BindScore, and
 * decorrelation can remove that LIMIT.
 */
public class CheckScoreInCorrelatedSubquery extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return logicalApply().when(LogicalApply::isCorrelated).thenApply(ctx -> {
            if (ctx.root.right().anyMatch(plan -> plan instanceof LogicalOlapScan
                    && BindScore.findScoreSlot((LogicalOlapScan) plan).isPresent())) {
                throw new AnalysisException(BindScore.CORRELATED_SUBQUERY_ERROR);
            }
            return ctx.root;
        }).toRule(RuleType.CHECK_SCORE_IN_CORRELATED_SUBQUERY);
    }
}
