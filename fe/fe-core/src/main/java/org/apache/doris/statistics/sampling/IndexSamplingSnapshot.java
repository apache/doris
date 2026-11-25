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
import org.apache.doris.catalog.TableIf;
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
        return read(table, indexId, null);
    }

    public static IndexSamplingSnapshot read(OlapTable table, long indexId, Set<Long> partitionIds) {
        List<Partition> parts = snapshotPartitions(table, partitionIds);
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
        return versions(snapshotPartitions(table, null)).stream()
                .anyMatch(v -> v > Partition.PARTITION_INIT_VERSION);
    }

    public static boolean hasWrittenPartition(OlapTable table, Set<Long> partitionIds) {
        return versions(snapshotPartitions(table, partitionIds)).stream()
                .anyMatch(v -> v > Partition.PARTITION_INIT_VERSION);
    }

    public enum ReportState { EMPTY, PENDING, READY }

    /** Stop after the first positive report. The collector only needs readiness, not a filled frame. */
    public static ReportState reportState(OlapTable table, long indexId) {
        return reportState(table, indexId, null);
    }

    public static ReportState reportState(OlapTable table, long indexId, Set<Long> partitionIds) {
        List<Partition> parts = snapshotPartitions(table, partitionIds);
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

    private static List<Partition> snapshotPartitions(OlapTable table, Set<Long> partitionIds) {
        table.readLock();
        try {
            if (partitionIds == null || partitionIds.isEmpty()) {
                return List.copyOf(table.getPartitions());
            }
            List<Partition> selected = new ArrayList<>(partitionIds.size());
            for (long partId : partitionIds) {
                Partition part = table.getPartition(partId);
                if (part != null) {
                    selected.add(part);
                } else {
                    for (Partition candidate : table.getPartitions()) {
                        if (candidate.getId() == partId) {
                            selected.add(candidate);
                            break;
                        }
                    }
                }
            }
            return List.copyOf(selected);
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

    public static long reportedPartitionIndexRows(OlapTable table, long indexId, long partitionId) {
        List<Partition> parts = snapshotPartitions(table, Set.of(partitionId));
        if (parts.isEmpty()) {
            return 0L;
        }
        long version = versions(parts).get(0);
        if (version == Partition.PARTITION_INIT_VERSION) {
            return 0L;
        }
        table.readLock();
        try {
            Partition part = parts.get(0);
            MaterializedIndex index = part.getIndex(indexId);
            if (index == null) {
                return TableIf.UNKNOWN_ROW_COUNT;
            }
            long reported = index.getRowCount();
            if (reported > 0) {
                return reported;
            }
            long sum = 0L;
            int known = 0;
            List<Tablet> tablets = index.getTablets();
            for (Tablet tablet : tablets) {
                long rows = tablet.getMinReplicaRowCount(version);
                if (rows > 0) {
                    sum = Math.addExact(sum, rows);
                    known++;
                }
            }
            if (known == 0) {
                return TableIf.UNKNOWN_ROW_COUNT;
            }
            if (known == tablets.size()) {
                return sum;
            }
            long mean = 1L + (sum - 1L) / known;
            return Math.addExact(sum, Math.multiplyExact((long) (tablets.size() - known), mean));
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
        return allCandidatesMatch(backend -> keyRange ? backend.isSupportsKeyRangeBlockSamplingV1()
                : backend.isSupportsFixedBlockSamplingV1());
    }

    public boolean allCandidatesSupportSharedKeyRange() {
        return allCandidatesMatch(Backend::isSupportsSharedKeyRangeBlockSamplingV1);
    }

    private boolean allCandidatesMatch(java.util.function.Predicate<Backend> predicate) {
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
                if (!predicate.test(backend)) {
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
