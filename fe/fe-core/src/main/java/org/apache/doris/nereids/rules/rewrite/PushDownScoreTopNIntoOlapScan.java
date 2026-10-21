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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Score;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Limit the scoring of a scan to the rows that a TopN on its score keeps.
 *
 * <p>BindScoreToOlapScan makes the scan score every row it returns. When a TopN orders the rows of
 * the scan by the score alone, the scan can keep the best rows of each segment instead, and SEARCH
 * can skip documents that cannot reach them. A filter on the score left above the scan has to see
 * every score, so it disables this: PushDownScoreRangeIntoOlapScan, which runs first, moves the
 * minimum score filters into the scan.
 */
public class PushDownScoreTopNIntoOlapScan implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalFilter(logicalOlapScan())))
                        .then(topN -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = topN.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            return pushDown(topN, Optional.of(project), filter).map(newFilter ->
                                    topN.withChildren(project.withChildren(newFilter))).orElse(null);
                        }).toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN),
                logicalTopN(logicalFilter(logicalOlapScan()))
                        .then(topN -> pushDown(topN, Optional.empty(), topN.child())
                                .map(topN::withChildren).orElse(null))
                        .toRule(RuleType.PUSH_DOWN_SCORE_TOPN_INTO_OLAP_SCAN));
    }

    private Optional<Plan> pushDown(LogicalTopN<?> topN, Optional<LogicalProject<?>> project,
            LogicalFilter<LogicalOlapScan> filter) {
        LogicalOlapScan scan = filter.child();
        Optional<Alias> scoreColumn = Score.findScoreColumn(scan.getVirtualColumns());
        if (!scoreColumn.isPresent() || !scan.getScoreLimit().equals(Optional.of(0L))
                || topN.getOrderKeys().size() != 1) {
            return Optional.empty();
        }
        Slot scoreSlot = scoreColumn.get().toSlot();
        OrderKey orderKey = topN.getOrderKeys().get(0);
        if (!ordersByScore(orderKey.getExpr(), project, scoreSlot)
                || filter.getConjuncts().stream().anyMatch(conjunct -> conjunct.getInputSlots().contains(scoreSlot))) {
            return Optional.empty();
        }
        // When limit + offset overflows the long range, the pushed scan limit would wrap to a
        // negative value. Fail with the same error as ordinary TopN.
        if (Utils.addOverflows(topN.getLimit(), topN.getOffset())) {
            throw new AnalysisException("limit + offset overflows the long range");
        }
        if (searchNeedsAllScores(filter.getConjuncts())) {
            return Optional.empty();
        }
        LogicalOlapScan newScan = scan.appendVirtualColumnsAndTopN(ImmutableList.of(),
                scan.getAnnOrderKeys(), scan.getAnnLimit(),
                ImmutableList.of(new OrderKey(scoreSlot, orderKey.isAsc(), orderKey.isNullFirst())),
                Optional.of(topN.getLimit() + topN.getOffset()), scan.getScoreRangeInfo());
        return Optional.of(filter.withChildren(newScan));
    }

    private static boolean ordersByScore(Expression orderExpr, Optional<LogicalProject<?>> project, Slot scoreSlot) {
        if (orderExpr.equals(scoreSlot)) {
            return true;
        }
        if (!project.isPresent() || !(orderExpr instanceof Slot)) {
            return false;
        }
        for (NamedExpression projection : project.get().getProjects()) {
            if (projection.toSlot().equals(orderExpr)) {
                return projection instanceof Alias && ((Alias) projection).child().equals(scoreSlot);
            }
        }
        return false;
    }

    // SEARCH skips documents that cannot reach the top only when it is the only predicate of the
    // scan: another predicate could drop the documents it keeps.
    private static boolean searchNeedsAllScores(Set<Expression> conjuncts) {
        if (conjuncts.stream().noneMatch(conjunct -> conjunct.containsType(SearchExpression.class))) {
            return false;
        }
        return conjuncts.size() > 1 || !(conjuncts.iterator().next() instanceof SearchExpression);
    }
}
