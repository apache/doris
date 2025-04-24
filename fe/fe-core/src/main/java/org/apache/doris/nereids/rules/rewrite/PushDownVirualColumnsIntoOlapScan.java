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

import org.apache.doris.nereids.processor.post.CommonSubExpressionCollector;
import org.apache.doris.nereids.processor.post.CommonSubExpressionOpt.ExpressionReplacer;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * extract virtual column from filter and push down them into olap scan.
 */
public class PushDownVirualColumnsIntoOlapScan extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalProject(logicalFilter(logicalOlapScan()
                .when(s -> s.getVirtualColumns().isEmpty())))
                .then(project -> {
                    // 1. extract filter common expr
                    // 2. generate virtual column from common expr and add them to scan
                    // 3. replace filter
                    // 4. replace project
                    LogicalFilter<LogicalOlapScan> filter = project.child();
                    LogicalOlapScan logicalOlapScan = filter.child();
                    CommonSubExpressionCollector collector = new CommonSubExpressionCollector();
                    for (Expression expr : filter.getConjuncts()) {
                        collector.collect(expr);
                    }
                    Map<Expression, Alias> aliasMap = new LinkedHashMap<>();
                    if (!collector.commonExprByDepth.isEmpty()) {
                        for (int i = 1; i <= collector.commonExprByDepth.size(); i++) {
                            Set<Expression> exprsInDepth = CommonSubExpressionCollector
                                    .getExpressionsFromDepthMap(i, collector.commonExprByDepth);
                            exprsInDepth.forEach(expr -> {
                                if (!(expr instanceof WhenClause)) {
                                    // case whenClause1 whenClause2 END
                                    // whenClause should not be regarded as common-sub-expression, because
                                    // cse will be replaced by a slot, after rewrite the case clause becomes:
                                    // 'case slot whenClause2 END'
                                    // This is illegal.
                                    Expression rewritten = expr.accept(ExpressionReplacer.INSTANCE, aliasMap);
                                    // if rewritten is already alias, use it directly,
                                    // because in materialized view rewriting
                                    // Should keep out slot immutably after rewritten successfully
                                    aliasMap.put(expr, rewritten instanceof Alias
                                            ? (Alias) rewritten : new Alias(rewritten));
                                }
                            });
                        }
                    }
                    List<NamedExpression> virtualColumns = Lists.newArrayList();
                    Map<Expression, Slot> replaceMap = Maps.newHashMap();
                    for (Map.Entry<Expression, Alias> entry : aliasMap.entrySet()) {
                        Alias alias = entry.getValue();
                        replaceMap.put(entry.getKey(), alias.toSlot());
                        virtualColumns.add(alias);
                    }
                    logicalOlapScan = logicalOlapScan.withVirtualColumns(virtualColumns);
                    Set<Expression> conjuncts = ExpressionUtils.replace(filter.getConjuncts(), replaceMap);
                    List<NamedExpression> projections = ExpressionUtils.replace(
                            (List) project.getProjects(), replaceMap);
                    LogicalFilter<?> newFilter = filter.withConjunctsAndChild(conjuncts, logicalOlapScan);
                    LogicalProject<?> newProject = project.withProjectsAndChild(projections, newFilter);
                    return newProject;
                }).toRule(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN);
    }
}
