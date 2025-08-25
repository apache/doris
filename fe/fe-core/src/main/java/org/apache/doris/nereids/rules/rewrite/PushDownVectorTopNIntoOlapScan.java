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

import org.apache.doris.analysis.IndexDef.IndexType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.InnerProductApproximate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.L2DistanceApproximate;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * extract virtual column from filter and push down them into olap scan.
 */
public class PushDownVectorTopNIntoOlapScan implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalTopN(logicalProject(logicalOlapScan())).when(t -> t.getOrderKeys().size() == 1).then(topN -> {
                    LogicalProject<LogicalOlapScan> project = topN.child();
                    LogicalOlapScan scan = project.child();
                    return pushDown(topN, project, scan, Optional.empty());
                }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN),
                logicalTopN(logicalProject(logicalFilter(logicalOlapScan())))
                        .when(t -> t.getOrderKeys().size() == 1).then(topN -> {
                            LogicalProject<LogicalFilter<LogicalOlapScan>> project = topN.child();
                            LogicalFilter<LogicalOlapScan> filter = project.child();
                            LogicalOlapScan scan = filter.child();
                            return pushDown(topN, project, scan, Optional.of(filter));
                        }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN)
        );
    }

    private Plan pushDown(
            LogicalTopN<?> topN,
            LogicalProject<?> project,
            LogicalOlapScan scan,
            Optional<LogicalFilter<?>> optionalFilter) {
        // Retrives the expression used for ordering in the TopN.
        Expression orderKey = topN.getOrderKeys().get(0).getExpr();
        // The order key must be a SlotReference corresponding to an expr.
        if (!(orderKey instanceof SlotReference)) {
            return null;
        }
        SlotReference keySlot = (SlotReference) orderKey;
        Expression orderKeyExpr = null;
        Alias orderKeyAlias = null;
        // Find the corresponding expression in the project that matches the keySlot.
        for (NamedExpression projection : project.getProjects()) {
            if (projection.toSlot().equals(keySlot) && projection instanceof Alias) {
                orderKeyExpr = ((Alias) projection).child();
                orderKeyAlias = (Alias) projection;
                break;
            }
        }
        if (orderKeyExpr == null) {
            return null;
        }

        boolean l2Dist;
        boolean innerProduct;
        l2Dist = orderKeyExpr instanceof L2DistanceApproximate;
        innerProduct = orderKeyExpr instanceof InnerProductApproximate;
        if (!(l2Dist) && !(innerProduct)) {
            return null;
        }

        Expression left = null;
        if (l2Dist) {
            L2DistanceApproximate l2DistanceApproximate = (L2DistanceApproximate) orderKeyExpr;
            left = l2DistanceApproximate.left();
        } else {
            InnerProductApproximate innerProductApproximate = (InnerProductApproximate) orderKeyExpr;
            left = innerProductApproximate.left();
        }

        while (left instanceof Cast) {
            left = ((Cast) left).child();
        }

        if (l2Dist) {
            if (!(left instanceof SlotReference && ((L2DistanceApproximate) orderKeyExpr).right().isConstant())) {
                return null;
            }
        } else {
            if (!(left instanceof SlotReference && ((InnerProductApproximate) orderKeyExpr).right().isConstant())) {
                return null;
            }
        }

        SlotReference leftInput = (SlotReference) left;
        if (!leftInput.getOriginalColumn().isPresent() || !leftInput.getOriginalTable().isPresent()) {
            return null;
        }
        TableIf table = leftInput.getOriginalTable().get();
        Column column = leftInput.getOriginalColumn().get();
        boolean hasAnnIndexOnColumn = false;
        for (Index index : table.getTableIndexes().getIndexes()) {
            if (index.getIndexType() == IndexType.ANN) {
                if (index.getColumns().size() != 1) {
                    continue;
                }
                if (index.getColumns().get(0).equalsIgnoreCase(column.getName())) {
                    hasAnnIndexOnColumn = true;
                    break;
                }
            }
        }
        if (!hasAnnIndexOnColumn) {
            return null;
        }

        Plan plan = scan.withVirtualColumnsAndTopN(
                ImmutableList.of(orderKeyAlias),
                topN.getOrderKeys(), Optional.of(topN.getLimit() + topN.getOffset()),
                ImmutableList.of(), Optional.empty());

        Map<Expression, Expression> replaceMap = Maps.newHashMap();
        replaceMap.put(orderKeyAlias, orderKeyAlias.toSlot());
        replaceMap.put(orderKeyExpr, orderKeyAlias.toSlot());
        if (optionalFilter.isPresent()) {
            LogicalFilter<?> filter = optionalFilter.get();
            Set<Expression> newConjuncts = ExpressionUtils.replace(filter.getConjuncts(), replaceMap);
            plan = filter.withConjunctsAndChild(newConjuncts, plan);
        }
        List<NamedExpression> newProjections = ExpressionUtils
                .replaceNamedExpressions(project.getProjects(), replaceMap);
        LogicalProject<?> newProject = project.withProjectsAndChild(newProjections, plan);
        return topN.withChildren(newProject);
    }
}
