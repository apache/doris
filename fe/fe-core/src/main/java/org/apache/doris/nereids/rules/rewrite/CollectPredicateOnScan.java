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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Collect filter pushed down on top of the scan.
 * Mainly used in hbo and controlled by 'enable_hbo_info_collection' session variable.
 * It provides scanToFilterMap info to HboPlanInfoProvider.
 */
public class CollectPredicateOnScan implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.COLLECT_SCAN_FILTER.build(
                logicalFilter(logicalOlapScan()).then(filter -> {
                    if (!StatisticsUtil.isEnableHboInfoCollection()) {
                        return filter;
                    }
                    LogicalOlapScan scan = filter.child();
                    String queryId = DebugUtil.printId(ConnectContext.get().queryId());
                    Map<RelationId, Set<Expression>> scanToFilterMap = Env.getCurrentEnv().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().getScanToFilterMap(queryId);
                    if (scanToFilterMap.isEmpty()) {
                        Env.getCurrentEnv().getHboPlanStatisticsManager()
                                .getHboPlanInfoProvider().putScanToFilterMap(queryId, scanToFilterMap);
                    }
                    scanToFilterMap.put(scan.getRelationId(), filter.getConjuncts());
                    return filter;
                })),

                RuleType.COLLECT_SCAN_PROJECT_FILTER.build(
                logicalFilter(logicalProject(logicalOlapScan())).then(filter -> {
                    if (!StatisticsUtil.isEnableHboInfoCollection()) {
                        return filter;
                    }
                    LogicalOlapScan scan = filter.child().child();
                    String queryId = DebugUtil.printId(ConnectContext.get().queryId());
                    Map<RelationId, Set<Expression>> scanToFilterMap = Env.getCurrentEnv().getHboPlanStatisticsManager()
                            .getHboPlanInfoProvider().getScanToFilterMap(queryId);
                    if (scanToFilterMap.isEmpty()) {
                        Env.getCurrentEnv().getHboPlanStatisticsManager()
                                .getHboPlanInfoProvider().putScanToFilterMap(queryId, scanToFilterMap);
                    }
                    scanToFilterMap.put(scan.getRelationId(), filter.getConjuncts());
                    return filter;
                }))
        );
    }
}
