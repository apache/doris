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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collect scan filter for hbo
 */
public class CollectPredicateOnScan implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.COLLECT_SCAN_FILTER_FOR_HBO.build(
                logicalFilter(logicalOlapScan()).then(filter -> {
                    if (ConnectContext.get() == null || ConnectContext.get().getSessionVariable() == null
                            || !ConnectContext.get().getSessionVariable().isEnableHboInfoCollection()) {
                        return filter;
                    }
                    LogicalOlapScan scan = (LogicalOlapScan) filter.child();
                    String queryId = DebugUtil.printId(ConnectContext.get().queryId());
                    Map<RelationId, Set<Expression>> tableToFilterExprMap = HboPlanStatisticsManager.getInstance()
                            .getHboPlanInfoProvider().getTableToExprMap(queryId);
                    if (tableToFilterExprMap.isEmpty()) {
                        HboPlanStatisticsManager.getInstance()
                                .getHboPlanInfoProvider().putTableToExprMap(queryId, tableToFilterExprMap);
                    }
                    tableToFilterExprMap.put(scan.getRelationId(), filter.getConjuncts());
                    return filter;
                })),

                RuleType.COLLECT_SCAN_PROJECT_FILTER_FOR_HBO.build(
                logicalFilter(logicalProject(logicalOlapScan())).then(filter -> {
                    if (ConnectContext.get() == null || ConnectContext.get().getSessionVariable() == null
                            || !ConnectContext.get().getSessionVariable().isEnableHboInfoCollection()) {
                        return filter;
                    }
                    LogicalOlapScan scan = (LogicalOlapScan) filter.child().child();
                    String queryId = DebugUtil.printId(ConnectContext.get().queryId());
                    Map<RelationId, Set<Expression>> tableToFilterExprMap = HboPlanStatisticsManager.getInstance()
                            .getHboPlanInfoProvider().getTableToExprMap(queryId);
                    if (tableToFilterExprMap.isEmpty()) {
                        HboPlanStatisticsManager.getInstance()
                                .getHboPlanInfoProvider().putTableToExprMap(queryId, tableToFilterExprMap);
                    }
                    tableToFilterExprMap.put(scan.getRelationId(), filter.getConjuncts());
                    return filter;
                }))
        );
    }
}
