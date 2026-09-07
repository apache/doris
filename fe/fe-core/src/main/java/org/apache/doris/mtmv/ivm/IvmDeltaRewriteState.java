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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.types.DataType;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Mutable state owned by one recursive delta rewrite.
 *
 * <p>It deliberately is not the visitor context: visitor callbacks still receive the immutable
 * {@link IvmIncrRefreshContext}. The state only carries scan-local rewrite data and the deterministic
 * left-to-right delta scan index.
 */
class IvmDeltaRewriteState {
    private final Map<OlapTable, OlapTableStream> streams;
    private final boolean includeExhaustedStreams;
    private final IvmSequenceCalculator sequenceCalculator;
    /**
     * Window partition ids (last N partitions by partition value) per base table,
     * configured via the {@code ivm_partition_window_limit} MTMV property. A {@code null}
     * value means the table is unlimited (full table).
     */
    private final Map<OlapTable, List<Long>> windowPartitionIdsByTable;
    private int nextDeltaScanIndex;

    IvmDeltaRewriteState(Map<OlapTable, OlapTableStream> streams,
            boolean includeExhaustedStreams, long refreshVersion, DataType sequenceType,
            Map<OlapTable, List<Long>> windowPartitionIdsByTable) {
        this.streams = new HashMap<>(streams);
        this.includeExhaustedStreams = includeExhaustedStreams;
        this.sequenceCalculator = IvmSequenceCalculator.create(refreshVersion, sequenceType);
        this.windowPartitionIdsByTable = windowPartitionIdsByTable;
    }

    /**
     * Restricts a scan's selected partitions to the configured window. Returns the same scan
     * when the table is unlimited or the selection already fits the window.
     */
    LogicalOlapScan restrictWindow(LogicalOlapScan scan) {
        List<Long> windowPartitionIds = windowPartitionIdsByTable.get(scan.getTable());
        if (windowPartitionIds == null) {
            return scan;
        }
        List<Long> restricted = windowPartitionIds(scan, windowPartitionIds);
        if (restricted.equals(scan.getSelectedPartitionIds())) {
            return scan;
        }
        return scan.withSelectedPartitionIds(restricted);
    }

    private List<Long> windowPartitionIds(LogicalOlapScan scan, List<Long> windowPartitionIds) {
        List<Long> selectedPartitionIds = scan.getSelectedPartitionIds();
        if (selectedPartitionIds.isEmpty()) {
            // Empty selection means all partitions; the window is the restriction.
            return windowPartitionIds;
        }
        Set<Long> windowPartitionIdSet = new HashSet<>(windowPartitionIds);
        return selectedPartitionIds.stream()
                .filter(windowPartitionIdSet::contains)
                .collect(Collectors.toList());
    }

    Optional<LogicalOlapTableStreamScan> createDeltaScan(LogicalOlapScan scan) {
        if (isExcluded(scan)) {
            return Optional.empty();
        }
        OlapTable originTable = (OlapTable) scan.getTable();
        OlapTableStream stream = streams.get(originTable);
        if (stream == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: missing delta scan context for " + scan.getTable().getName());
        }
        List<Long> partitionIds = restrictWindow(scan).getSelectedPartitionIds();
        if (!includeExhaustedStreams && !hasPendingData(stream, partitionIds)) {
            return Optional.empty();
        }
        List<Long> tabletIds = scan.getSelectedTabletIds();
        if (!tabletIds.isEmpty() && !partitionIds.equals(scan.getSelectedPartitionIds())) {
            // The partition selection was narrowed by the window; narrow the tablet
            // selection to the window partitions' tablets as well.
            Set<Long> windowTabletIds = new HashSet<>();
            for (Long partitionId : partitionIds) {
                Partition partition = originTable.getPartition(partitionId);
                if (partition != null && partition.getIndex(originTable.getBaseIndexId()) != null) {
                    windowTabletIds.addAll(partition.getIndex(originTable.getBaseIndexId()).getTabletIdsInOrder());
                }
            }
            tabletIds = tabletIds.stream()
                    .filter(windowTabletIds::contains)
                    .collect(Collectors.toList());
        }
        OlapTableStreamWrapper streamWrapper = new OlapTableStreamWrapper(
                stream, originTable, partitionIds);
        return Optional.of(new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(),
                streamWrapper,
                scan.getQualifier(),
                partitionIds,
                tabletIds,
                scan.getHints(),
                scan.getTableSample(),
                scan.getOperativeSlots()));
    }

    boolean isExcluded(LogicalOlapScan scan) {
        return !streams.containsKey(scan.getTable());
    }

    OlapTableStream getStream(LogicalOlapScan scan) {
        OlapTableStream stream = streams.get(scan.getTable());
        if (stream == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: missing delta scan context for " + scan.getTable().getName());
        }
        return stream;
    }

    int nextDeltaIndex() {
        int index = nextDeltaScanIndex++;
        if (index >= 1024) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: too many delta scans for sequence encoding: " + index);
        }
        return index;
    }

    Literal toSequence(int deltaIndex) {
        return sequenceCalculator.encode(deltaIndex, BigInteger.ZERO, true);
    }

    Expression toSequenceByDmlFactor(Expression dmlFactor, int deltaIndex) {
        return sequenceCalculator.encodeByDmlFactor(dmlFactor, deltaIndex);
    }

    private boolean hasPendingData(OlapTableStream stream, List<Long> partitionIds) {
        OlapTable baseTable = stream.getBaseTableNullable();
        if (baseTable == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: stream base table is null for stream " + stream.getName());
        }
        for (Long partitionId : partitionIds) {
            if (baseTable.getPartition(partitionId) != null
                    && stream.hasData(baseTable.getPartition(partitionId))) {
                return true;
            }
        }
        return false;
    }
}
