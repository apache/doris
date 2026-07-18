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
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Mutable state owned by one recursive delta rewrite.
 *
 * <p>It deliberately is not the visitor context: visitor callbacks still receive the immutable
 * {@link IvmRefreshContext}. The state only carries scan-local rewrite data and the deterministic
 * left-to-right delta scan index.
 */
class IvmDeltaRewriteState {
    private final Map<OlapTable, OlapTableStream> streams;
    private final boolean includeUpToDateStreams;
    private final long refreshVersion;
    private int nextDeltaScanIndex;

    IvmDeltaRewriteState(Map<OlapTable, OlapTableStream> streams,
            boolean includeUpToDateStreams, long refreshVersion) {
        this.streams = new HashMap<>(streams);
        this.includeUpToDateStreams = includeUpToDateStreams;
        if (refreshVersion < 0 || refreshVersion > (Long.MAX_VALUE >>> 11)) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM refresh version exceeds the sequence encoding range: " + refreshVersion);
        }
        this.refreshVersion = refreshVersion;
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
        if (!includeUpToDateStreams && !hasPendingData(stream, scan)) {
            return Optional.empty();
        }
        OlapTableStreamWrapper streamWrapper = new OlapTableStreamWrapper(
                stream, originTable, scan.getSelectedPartitionIds());
        return Optional.of(new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(),
                streamWrapper,
                scan.getQualifier(),
                scan.getSelectedPartitionIds(),
                scan.getSelectedTabletIds(),
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

    long nextSubSeqPrefix() {
        int index = nextDeltaScanIndex++;
        if (index >= 1024) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: too many delta scans for sequence encoding: " + index);
        }
        return (long) index << 1;
    }

    long toSequence(long subSeq) {
        return (refreshVersion << 11) | subSeq;
    }

    long maxSeqSuffix(long subSeqPrefix) {
        return subSeqPrefix | 1;
    }

    private boolean hasPendingData(OlapTableStream stream, LogicalOlapScan scan) {
        OlapTable baseTable = stream.getBaseTableNullable();
        if (baseTable == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: stream base table is null for stream " + stream.getName());
        }
        for (Long partitionId : scan.getSelectedPartitionIds()) {
            if (baseTable.getPartition(partitionId) != null
                    && stream.hasData(baseTable.getPartition(partitionId))) {
                return true;
            }
        }
        return false;
    }
}
