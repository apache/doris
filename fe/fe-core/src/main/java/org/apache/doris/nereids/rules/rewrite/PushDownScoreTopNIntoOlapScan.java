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
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Push down score function into olap scan node.
 * It will push down score function as a virtual column, and also push down the
 * topN info.
 *
 * Pattern:
 * logicalTopN(logicalProject(logicalFilter(logicalOlapScan)))
 *
 * Requirements:
 * 1. The TopN node has exactly one ordering expression.
 * 2. The ordering expression in TopN must be a slot reference that refers to a
 * Score function.
 * 3. The Filter node must contain at least one Match function.
 *
 * Example:
 * Before:
 * SELECT score() as score FROM table WHERE text_col MATCH 'query' ORDER BY
 * score DESC LIMIT 10
 *
 * After:
 * The Score function is pushed down into the OlapScan node as a virtual column,
 * and the TopN information (order by, limit) is also pushed down to be used by
 * the storage engine.
 */
public class PushDownScoreTopNIntoOlapScan implements RewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(PushDownScoreTopNIntoOlapScan.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalFilter(logicalOlapScan())))
                        .then(topN -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = topN.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(topN, project, filter, scan);
                        }).toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN));
    }

    private Plan pushDown(
            LogicalTopN<LogicalProject<LogicalFilter<LogicalOlapScan>>> topN,
            LogicalProject<LogicalFilter<LogicalOlapScan>> project,
            LogicalFilter<LogicalOlapScan> filter,
            LogicalOlapScan scan) {
        // 1. Requirement: Project must contain a score() function.
        boolean hasScoreFunction = project.getProjects().stream()
                .anyMatch(projection -> {
                    if (projection instanceof Alias) {
                        return ((Alias) projection).child() instanceof Score;
                    }
                    return false;
                });
        if (!hasScoreFunction) {
            return null;
        }

        // 2. Requirement: WHERE clause must contain a MATCH function.
        boolean hasMatchPredicate = filter.getConjuncts().stream()
                .anyMatch(conjunct -> !conjunct.collect(e -> e instanceof Match).isEmpty());
        if (!hasMatchPredicate) {
            throw new AnalysisException(
                    "WHERE clause must contain at least one MATCH function"
                            + " for score() push down optimization");
        }

        // 3. Requirement: WHERE clause must NOT contain any score() function.
        boolean hasScorePredicate = filter.getConjuncts().stream()
                .anyMatch(conjunct -> !conjunct.collect(e -> e instanceof Score).isEmpty());
        if (hasScorePredicate) {
            throw new AnalysisException(
                    "score() function can only be used in SELECT clause, not in WHERE clause");
        }

        // 4. Requirement: TopN must have exactly one ordering expression.
        if (topN.getOrderKeys().size() != 1) {
            throw new AnalysisException(
                    "TopN must have exactly one ordering expression for score() push down optimization");
        }

        // 5. Requirement: ORDER BY expression must refer to a SELECT score() alias.
        Expression orderKey = topN.getOrderKeys().get(0).getExpr();
        if (!(orderKey instanceof SlotReference)) {
            throw new AnalysisException(
                    "ORDER BY expression must be a slot reference (not a function call or complex expression)"
                            + " for score() push down optimization");
        }

        SlotReference orderSlot = (SlotReference) orderKey;
        Expression scoreExpr = null;
        Alias scoreAlias = null;

        // Find the 'score()' expression in the project list that matches the order by
        // slot.
        for (NamedExpression projection : project.getProjects()) {
            if (projection.toSlot().equals(orderSlot) && projection instanceof Alias) {
                Expression childExpr = ((Alias) projection).child();
                if (childExpr instanceof Score) {
                    scoreExpr = childExpr;
                    scoreAlias = (Alias) projection;
                    break;
                }
            }
        }

        if (scoreAlias == null) {
            throw new AnalysisException(
                    "ORDER BY expression must reference a score() function from SELECT clause"
                            + " for push down optimization");
        }

        // All conditions met, perform the push down.
        // This is the core action: push score() as a virtual column and also push the
        // topN info.
        Plan newScan = scan.withVirtualColumnsAndTopN(ImmutableList.of(scoreAlias),
                ImmutableList.of(), Optional.empty(),
                topN.getOrderKeys(), Optional.of(topN.getLimit() + topN.getOffset()));

        // Rebuild the plan tree above the new scan.
        // We need to replace the original score() function with a reference to the new
        // virtual column slot.
        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        replaceMap.put(scoreExpr, scoreAlias.toSlot());
        replaceMap.put(scoreAlias, scoreAlias.toSlot());

        // The filter node remains, as the MATCH predicate is still needed.
        Plan newFilter = filter.withConjunctsAndChild(filter.getConjuncts(), newScan);

        // Rebuild project list with the replaced expressions.
        List<NamedExpression> newProjections = ExpressionUtils
                .replaceNamedExpressions(project.getProjects(), replaceMap);
        Plan newProject = project.withProjectsAndChild(newProjections, newFilter);

        // Rebuild the TopN node on top of the new project.
        return topN.withChildren(newProject);
    }
}
