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
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner;
import org.apache.doris.nereids.rules.expression.rules.PartitionPruner.PartitionTableType;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to prune partition of file scan. For different external tables, there is no unified partition prune method.
 * For example, Hive is using hive meta store api to get partitions. Iceberg is using Iceberg api to get FileScanTask,
 * which doesn't return a partition list.
 * So here we only support Hive table partition prune.
 * For other external table, simply pass the conjuncts to LogicalFileScan, so that different
 * external file ScanNode could do the partition filter by themselves.
 */
public class PruneFileScanPartition extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalFilter(logicalFileScan()).whenNot(p -> p.child().getSelectedPartitions().isPruned)
                .thenApply(ctx -> {
                    LogicalFilter<LogicalFileScan> filter = ctx.root;
                    LogicalFileScan scan = filter.child();
                    ExternalTable tbl = scan.getTable();

                    SelectedPartitions selectedPartitions;
                    // TODO(cmy): support other external table
                    if (tbl instanceof HMSExternalTable && ((HMSExternalTable) tbl).getDlaType() == DLAType.HIVE) {
                        HMSExternalTable hiveTbl = (HMSExternalTable) tbl;
                        selectedPartitions = pruneHivePartitions(hiveTbl, filter, scan, ctx.cascadesContext);
                    } else {
                        // set isPruned so that it won't go pass the partition prune again
                        selectedPartitions = new SelectedPartitions(0, ImmutableMap.of(), true);
                    }

                    LogicalFileScan rewrittenScan = scan.withConjuncts(filter.getConjuncts())
                            .withSelectedPartitions(selectedPartitions);
                    return new LogicalFilter<>(filter.getConjuncts(), rewrittenScan);
                }).toRule(RuleType.FILE_SCAN_PARTITION_PRUNE);
    }

    private SelectedPartitions pruneHivePartitions(HMSExternalTable hiveTbl,
            LogicalFilter<LogicalFileScan> filter, LogicalFileScan scan, CascadesContext ctx) {
        Map<Long, PartitionItem> selectedPartitionItems = Maps.newHashMap();
        if (CollectionUtils.isEmpty(hiveTbl.getPartitionColumns())) {
            // non partitioned table, return NOT_PRUNED.
            // non partition table will be handled in HiveScanNode.
            return SelectedPartitions.NOT_PRUNED;
        }
        Map<String, Slot> scanOutput = scan.getOutput()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getName().toLowerCase(), Function.identity()));

        List<Slot> partitionSlots = hiveTbl.getPartitionColumns()
                .stream()
                .map(column -> scanOutput.get(column.getName().toLowerCase()))
                .collect(Collectors.toList());

        HiveMetaStoreCache cache = Env.getCurrentEnv().getExtMetaCacheMgr()
                .getMetaStoreCache((HMSExternalCatalog) hiveTbl.getCatalog());
        HiveMetaStoreCache.HivePartitionValues hivePartitionValues = cache.getPartitionValues(
                hiveTbl.getDbName(), hiveTbl.getName(), hiveTbl.getPartitionColumnTypes());
        Map<Long, PartitionItem> idToPartitionItem = hivePartitionValues.getIdToPartitionItem();
        List<Long> prunedPartitions = new ArrayList<>(PartitionPruner.prune(
                partitionSlots, filter.getPredicate(), idToPartitionItem, ctx, PartitionTableType.HIVE));

        for (Long id : prunedPartitions) {
            selectedPartitionItems.put(id, idToPartitionItem.get(id));
        }
        return new SelectedPartitions(idToPartitionItem.size(), selectedPartitionItems, true);
    }
}
