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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.source.PaimonScanNode;

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.RowDataToObjectArrayConverter;
import org.apache.paimon.utils.ThreadPoolUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class MTMVPaimonParamsAssembler extends MTMVExternalTableParamsAssembler {
    @Override
    public void markReadBySnapshot(Map<String, String> params, long snapshotId) {
        super.markReadBySnapshot(params, snapshotId);
        params.put(TableScanParams.READ_MODE, TableScanParams.INCREMENTAL_READ);
        params.put(PaimonScanNode.DORIS_START_SNAPSHOT_ID, String.valueOf(snapshotId));
    }

    @Override
    public void markReadBySnapshotIncremental(Map<String, String> params, long startSnapshotId, long endSnapshotId) {
        super.markReadBySnapshotIncremental(params, startSnapshotId, endSnapshotId);
        params.put(TableScanParams.READ_MODE, TableScanParams.INCREMENTAL_READ);
        params.put(PaimonScanNode.DORIS_START_SNAPSHOT_ID, String.valueOf(startSnapshotId));
        params.put(PaimonScanNode.DORIS_END_SNAPSHOT_ID, String.valueOf(endSnapshotId));
        params.put(PaimonScanNode.DORIS_INCREMENTAL_BETWEEN_SCAN_MODE, "delta");
    }

    @Override
    public List<String> calculateNeedRefreshPartitions(
            MTMVExternalTableParamsAssembler.RefreshSnapshotInfo refreshSnapshotInfo,
            Map<String, Set<String>> partitionMappings, MvccTable table) {
        List<String> needRefreshPartitions = new ArrayList<>();
        if (refreshSnapshotInfo.getRefreshType() == RefreshType.OVERWRITE) {
            FileStoreTable paimonTable = (FileStoreTable) ((PaimonExternalTable) table)
                    .getPaimonTable(Optional.empty());
            Set<String> updatePartitions = getUpdatePartitionsBySnapshotId(
                    paimonTable, refreshSnapshotInfo.getSnapshotId());
            for (Map.Entry<String, Set<String>> partition : partitionMappings.entrySet()) {
                Set<String> partitionValue = partition.getValue();
                if (partitionValue.isEmpty()) {
                    needRefreshPartitions.add(partition.getKey());
                } else {
                    for (String p : partitionValue) {
                        if (updatePartitions.contains(p)) {
                            needRefreshPartitions.add(partition.getKey());
                        }
                    }
                }
            }
        }
        return needRefreshPartitions;
    }

    @Override
    Optional<RefreshSnapshotInfo> calculateNextSnapshot(
            MvccTable table, long startSnapshotId, long endSnapshotId) {
        PriorityQueue<PaimonSnapshot> paimonSnapshotQueue =
                ((PaimonExternalTable) table).getSnapshotInfos(startSnapshotId, endSnapshotId);
        if (paimonSnapshotQueue.isEmpty()) {
            return Optional.empty();
        } else {
            long snapshotId = 0;
            RefreshType refreshType = null;
            PaimonSnapshot firstPaimonSnapshot = paimonSnapshotQueue.peek();
            if (isDataAppend(firstPaimonSnapshot)) {
                while (!paimonSnapshotQueue.isEmpty() && isDataAppend(paimonSnapshotQueue.peek())) {
                    PaimonSnapshot paimonSnapshot = paimonSnapshotQueue.poll();
                    snapshotId = paimonSnapshot.getSnapshotId();
                    refreshType = RefreshType.INCREMENTAL;
                }
            } else {
                snapshotId = firstPaimonSnapshot.getSnapshotId();
                refreshType = RefreshType.OVERWRITE;
            }
            return Optional.of(new RefreshSnapshotInfo(snapshotId, refreshType));
        }
    }

    private boolean isDataAppend(PaimonSnapshot snapshot) {
        return snapshot.getCommitKind() == Snapshot.CommitKind.COMPACT
                || snapshot.getCommitKind() == Snapshot.CommitKind.APPEND && snapshot.getDeltaRecordCount() >= 0;
    }

    public Set<String> getUpdatePartitionsBySnapshotId(FileStoreTable table, long snapshotId) {
        RowType partitionType = table.schema().logicalPartitionType();

        SnapshotReader snapshotReader = table.newSnapshotReader();
        Snapshot snapshot = snapshotReader.snapshotManager().snapshot(snapshotId);
        ManifestsReader.Result read =
                snapshotReader.manifestsReader().read(snapshot, ScanMode.DELTA);
        Map<BinaryRow, PartitionEntry> partitions = new ConcurrentHashMap<>();
        Consumer<ManifestFileMeta> processor =
                m -> PartitionEntry.merge(PartitionEntry.merge(snapshotReader.readManifest(m)), partitions);
        ThreadPoolUtils.randomlyOnlyExecute(
                ManifestReadThreadPool.getExecutorService(3), processor, read.filteredManifests);

        RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
        Set<String> updatePartitions = partitions.values().stream()
                .map(entry -> {
                    Object[] partition = converter.convert(entry.partition());
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < partition.length; i++) {
                        if (i > 0) {
                            sb.append("/");
                        }
                        sb.append(partitionType.getFields().get(i).name()).append("=").append(partition[i]);
                    }
                    return sb.toString();
                })
                .collect(Collectors.toSet());

        return updatePartitions;
    }
}
