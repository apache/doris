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

package org.apache.doris.statistics.sampling;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.cloud.catalog.CloudPartition;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.analysis.AnalyzeSkipException;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.system.Backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** A job-local sampling frame. Imputed counts have no path back to the catalog row count. */
public record IndexSamplingSnapshot(TabletSamplingScope scope, Set<Long> replicaBackends) {
    public static IndexSamplingSnapshot read(OlapTable table, long indexId) {
        List<Partition> parts = snapshotPartitions(table);
        List<Long> versions = versions(parts);
        List<PartitionSampleMeta> raw = new ArrayList<>();
        Set<Long> backends = new HashSet<>();
        table.readLock();
        try {
            for (int i = 0; i < parts.size(); i++) {
                long version = versions.get(i);
                if (version == Partition.PARTITION_INIT_VERSION) {
                    continue;
                }
                Partition part = parts.get(i);
                MaterializedIndex index = part.getIndex(indexId);
                if (index == null) {
                    throw new AnalyzeSkipException("Index " + indexId + " is missing in partition " + part.getId());
                }
                List<TabletSampleMeta> tablets = new ArrayList<>();
                for (Tablet tablet : index.getTablets()) {
                    long rows = tablet.getMinReplicaRowCount(version);
                    tablets.add(new TabletSampleMeta(tablet.getId(), rows, rows <= 0));
                    if (!Config.isCloudMode()) {
                        tablet.getReplicas().forEach(replica -> backends.add(replica.getBackendIdWithoutException()));
                    }
                }
                raw.add(new PartitionSampleMeta(part.getId(), tablets));
            }
        } finally {
            table.readUnlock();
        }
        return new IndexSamplingSnapshot(impute(raw), Set.copyOf(backends));
    }

    public static boolean hasWrittenPartition(OlapTable table) {
        return versions(snapshotPartitions(table)).stream()
                .anyMatch(v -> v > Partition.PARTITION_INIT_VERSION);
    }

    public enum ReportState { EMPTY, PENDING, READY }

    /** Stop after the first positive report. The collector only needs readiness, not a filled frame. */
    public static ReportState reportState(OlapTable table, long indexId) {
        List<Partition> parts = snapshotPartitions(table);
        List<Long> versions = versions(parts);
        table.readLock();
        try {
            boolean written = false;
            for (int i = 0; i < parts.size(); i++) {
                long version = versions.get(i);
                if (version == Partition.PARTITION_INIT_VERSION) {
                    continue;
                }
                written = true;
                MaterializedIndex index = parts.get(i).getIndex(indexId);
                if (index != null && index.getTablets().stream().anyMatch(t -> t.getMinReplicaRowCount(version) > 0)) {
                    return ReportState.READY;
                }
            }
            return written ? ReportState.PENDING : ReportState.EMPTY;
        } finally {
            table.readUnlock();
        }
    }

    private static List<Partition> snapshotPartitions(OlapTable table) {
        table.readLock();
        try {
            return List.copyOf(table.getPartitions());
        } finally {
            table.readUnlock();
        }
    }

    public static long reportedIndexRows(OlapTable table, long indexId, boolean strict) {
        table.readLock();
        try {
            return table.getRowCountForIndex(indexId, strict);
        } finally {
            table.readUnlock();
        }
    }

    // The Cloud MS lookup may take table version locks. Never call it under the table read lock.
    private static List<Long> versions(List<Partition> partitions) {
        if (Config.isCloudMode()) {
            try {
                return CloudPartition.getSnapshotVisibleVersionFromMs(
                        partitions.stream().map(CloudPartition.class::cast).toList());
            } catch (RpcException e) {
                throw new AnalyzeSkipException("Unable to verify partition versions for statistics", e);
            }
        }
        return partitions.stream().map(Partition::getVisibleVersion).toList();
    }

    static TabletSamplingScope impute(List<PartitionSampleMeta> raw) {
        Mean global = new Mean();
        List<Mean> localMeans = new ArrayList<>();
        for (PartitionSampleMeta p : raw) {
            Mean local = new Mean();
            for (TabletSampleMeta t : p.tablets()) {
                if (t.estimatedRows() > 0) {
                    local.add(t.estimatedRows());
                    global.add(t.estimatedRows());
                }
            }
            localMeans.add(local);
        }
        if (!raw.isEmpty() && global.count == 0) {
            throw new AnalyzeSkipException("Written partitions have no positive tablet row report for this index");
        }
        List<PartitionSampleMeta> filled = new ArrayList<>();
        for (int i = 0; i < raw.size(); i++) {
            Mean local = localMeans.get(i);
            long replacement = (local.count == 0 ? global : local).ceiling();
            PartitionSampleMeta p = raw.get(i);
            if (!p.tablets().isEmpty()) {
                filled.add(new PartitionSampleMeta(p.id(), p.tablets().stream().map(t -> t.estimatedRows() > 0
                        ? t : new TabletSampleMeta(t.id(), replacement, true)).toList()));
            }
        }
        return TabletSamplingScope.table(filled);
    }

    public boolean allCandidatesSupportFixedBlock() {
        return allCandidatesSupport(false);
    }

    public boolean allCandidatesSupport(boolean keyRange) {
        Collection<Backend> candidates;
        if (Config.isCloudMode()) {
            try (AutoCloseConnectContext context = StatisticsUtil.buildConnectContext(false)) {
                candidates = Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values();
            } catch (AnalysisException e) {
                return false;
            }
        } else {
            candidates = replicaBackends.stream().map(id -> Env.getCurrentSystemInfo().getBackend(id))
                    .filter(Objects::nonNull).toList();
        }
        boolean available = false;
        for (Backend backend : candidates) {
            if (backend.isQueryAvailable() && backend.isMixNode()) {
                available = true;
                boolean supported = keyRange ? backend.isSupportsKeyRangeBlockSamplingV1()
                        : backend.isSupportsFixedBlockSamplingV1();
                if (!supported) {
                    return false;
                }
            }
        }
        return available;
    }

    private static final class Mean {
        long sum;
        long count;

        void add(long rows) {
            sum = Math.addExact(sum, rows);
            count++;
        }

        long ceiling() {
            return 1 + (sum - 1) / count;
        }
    }
}
