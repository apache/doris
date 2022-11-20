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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionColumnFilterConverter;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.HashDistributionPruner;
import org.apache.doris.planner.PartitionColumnFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * prune bucket
 */
public class PruneOlapScanTablet extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalOlapScan())
                .thenApply(ctx -> {
                    LogicalFilter<LogicalOlapScan> filter = ctx.root;
                    LogicalOlapScan olapScan = filter.child();
                    OlapTable table = olapScan.getTable();
                    List<Long> indexList = Lists.newArrayList();
                    for (Long id : olapScan.getSelectedPartitionIds()) {
                        Partition partition = table.getPartition(id);
                        MaterializedIndex index = partition.getIndex(olapScan.getSelectedIndexId());
                        indexList.addAll(getPrunedTablet(olapScan, filter.getConjuncts(),
                                index, table.getDefaultDistributionInfo()));
                    }
                    return filter.withChildren(olapScan.withSelectedTabletId(ImmutableList.copyOf(indexList)));
                }).toRule(RuleType.OLAP_SCAN_TABLET_PRUNE);
    }

    private Collection<Long> getPrunedTablet(LogicalOlapScan olapScan, List<Expression> exprs,
            MaterializedIndex index, DistributionInfo info) {
        if (info.getType() == DistributionInfoType.HASH) {
            HashDistributionInfo hashInfo = (HashDistributionInfo) info;
            Map<String, PartitionColumnFilter> filterMap = Maps.newHashMap();
            exprs.stream().map(ExpressionUtils::checkAndMaybeCommute).filter(Objects::nonNull)
                            .forEach(expr -> ExpressionColumnFilterConverter.convert(expr, filterMap));
            return new HashDistributionPruner(index.getTabletIdsInOrder(),
                    hashInfo.getDistributionColumns(),
                    filterMap,
                    hashInfo.getBucketNum()
                    ).prune();
        }
        return index.getTabletIdsInOrder();
    }
}

