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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.Partition;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.Config;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.transaction.TransactionEntry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to prune empty partition.
 */
public class PruneEmptyPartition extends OneRewriteRuleFactory {
    public static final Logger LOG = LogManager.getLogger(PruneEmptyPartition.class);

    @Override
    public Rule build() {
        return logicalOlapScan().thenApply(ctx -> {
            // We still want to keep LogicalOlapScan even if partitions are empty,
            // so that the planner can build a scan node and the PreparedStatement can cache ShortCircuitQueryContext.
            if (ctx.connectContext != null && ctx.connectContext.getStatementContext() != null
                    && ctx.connectContext.getStatementContext().isShortCircuitQuery()) {
                return null;
            }
            LogicalOlapScan scan = ctx.root;
            OlapTable table = scan.getTable();
            List<Long> partitionIdsToPrune = scan.getSelectedPartitionIds();
            List<Long> ids = selectNonEmptyPartitionIds(scan, table, partitionIdsToPrune);
            if (ctx.connectContext != null && ctx.connectContext.isTxnModel()) {
                // In transaction load, need to add empty partitions which have invisible data of sub transactions
                selectNonEmptyPartitionIdsForTxnLoad(ctx.connectContext.getTxnEntry(), table, scan.getSelectedIndexId(),
                        partitionIdsToPrune, ids);
            }
            if (ids.isEmpty()) {
                return new LogicalEmptyRelation(ConnectContext.get().getStatementContext().getNextRelationId(),
                        scan.getOutput());
            }
            if (partitionIdsToPrune.equals(ids)) {
                // Not Prune actually, return directly
                return null;
            }
            return scan.withSelectedPartitionIds(ids);
        }).toRule(RuleType.PRUNE_EMPTY_PARTITION);
    }

    private List<Long> selectNonEmptyPartitionIds(LogicalOlapScan scan, OlapTable table,
            List<Long> partitionIds) {
        if (shouldRefreshPartitionVersionsFromMs(scan, table)) {
            List<CloudPartition> partitions = partitionIds.stream()
                    .map(table::getPartition)
                    .filter(Objects::nonNull)
                    .map(partition -> (CloudPartition) partition)
                    .collect(Collectors.toList());
            List<CloudPartition> partitionsToRefresh = new ArrayList<>();
            Set<Long> nonEmptyPartitionIds = new HashSet<>();
            for (CloudPartition partition : partitions) {
                if (partition.hasDataCached()) {
                    nonEmptyPartitionIds.add(partition.getId());
                } else {
                    // The incremental read may have just waited for a transaction to become visible. Refresh a
                    // cached-empty or unknown partition so newly visible data is not pruned before scan planning.
                    partitionsToRefresh.add(partition);
                }
            }
            if (partitionsToRefresh.isEmpty()) {
                return partitions.stream().map(CloudPartition::getId).collect(Collectors.toList());
            }
            try {
                List<Long> versions = CloudPartition.getSnapshotVisibleVersionFromMs(partitionsToRefresh, false);
                assert versions.size() == partitionsToRefresh.size()
                        : "the got num versions is not equals to acquired num versions";
                for (int i = 0; i < versions.size(); i++) {
                    if (versions.get(i) > Partition.PARTITION_INIT_VERSION) {
                        nonEmptyPartitionIds.add(partitionsToRefresh.get(i).getId());
                    }
                }
            } catch (RpcException e) {
                throw new RuntimeException("get version from meta service failed", e);
            }
            return partitions.stream()
                    .filter(partition -> nonEmptyPartitionIds.contains(partition.getId()))
                    .map(CloudPartition::getId)
                    .collect(Collectors.toList());
        }
        return table.selectNonEmptyPartitionIds(partitionIds, scan.getStreamReadMode());
    }

    private boolean shouldRefreshPartitionVersionsFromMs(LogicalOlapScan scan, OlapTable table) {
        boolean hasFixedVisibleVersions = table instanceof OlapTableWrapper
                && ((OlapTableWrapper) table).hasFixedVisibleVersions();
        return Config.isCloudMode() && !hasFixedVisibleVersions && scan.getScanParams().isPresent()
                && scan.getScanParams().get().incrementalRead();
    }

    private void selectNonEmptyPartitionIdsForTxnLoad(TransactionEntry txnEntry, OlapTable table, long indexId,
            List<Long> selectedPartitions, List<Long> nonEmptyPartitionIds) {
        for (Long selectedPartitionId : selectedPartitions) {
            if (nonEmptyPartitionIds.contains(selectedPartitionId)) {
                continue;
            }
            Partition partition = table.getPartition(selectedPartitionId);
            if (partition == null) {
                continue;
            }
            if (!txnEntry.getPartitionSubTxnIds(table.getId(), partition, indexId).isEmpty()) {
                nonEmptyPartitionIds.add(selectedPartitionId);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("add partition for txn load, table: {}, selected partitions: {}, non empty partitions: {}",
                    table.getId(), selectedPartitions, nonEmptyPartitionIds);
        }
    }
}
