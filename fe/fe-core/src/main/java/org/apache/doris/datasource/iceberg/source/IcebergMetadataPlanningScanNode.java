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

package org.apache.doris.datasource.iceberg.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.TableFormatType;
import org.apache.doris.datasource.iceberg.IcebergMetadataPlanningExternalTable;
import org.apache.doris.datasource.iceberg.IcebergUtils;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.spi.Split;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TIcebergFileDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.util.SerializationUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Scan node for the Iceberg metadata planning system table ({@code tbl$metadata_planning}).
 *
 * <p>This node is used by the distributed Iceberg metadata planning path. It groups the
 * matching data manifests from the current snapshot into per-BE shards using a weighted
 * min-heap assignment, then emits one {@link IcebergSplit} per shard. Each split carries
 * the serialized table, snapshot ID, predicate, and manifest list so that the BE-side
 * {@code IcebergMetadataPlanningJniScanner} can scan only the assigned manifests and return
 * file-level metadata rows back to FE for split construction.
 */
public class IcebergMetadataPlanningScanNode extends IcebergSysScanNode {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public IcebergMetadataPlanningScanNode(PlanNodeId id, TupleDescriptor desc, boolean needCheckColumnPriv,
            SessionVariable sv, ScanContext scanContext) {
        super(id, desc, needCheckColumnPriv, sv, scanContext);
    }

    @Override
    protected void setScanParams(TFileRangeDesc rangeDesc, Split split) {
        Preconditions.checkState(split instanceof IcebergSplit,
                "Iceberg metadata planning split must be IcebergSplit, but got %s",
                split == null ? "null" : split.getClass().getName());
        IcebergSplit icebergSplit = (IcebergSplit) split;
        TTableFormatFileDesc tableFormatFileDesc = new TTableFormatFileDesc();
        tableFormatFileDesc.setTableFormatType(TableFormatType.ICEBERG.value());
        tableFormatFileDesc.setTableLevelRowCount(-1);
        TIcebergFileDesc fileDesc = new TIcebergFileDesc();
        fileDesc.setSerializedSplit(icebergSplit.getSerializedSplit());
        fileDesc.setIsMetadataPlanning(true);
        tableFormatFileDesc.setIcebergParams(fileDesc);
        rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
        rangeDesc.setTableFormatParams(tableFormatFileDesc);
    }

    @Override
    public List<Split> getSplits(int numBackends) throws UserException {
        return executeWithPreExecutionAuthenticator(() -> buildMetadataPlanningSystemTableSplits(numBackends));
    }

    @VisibleForTesting
    List<Split> buildMetadataPlanningSystemTableSplits(int numBackends) throws UserException {
        Snapshot snapshot = Preconditions.checkNotNull(createMetadataPlanningTableScan().snapshot(),
                "Metadata planning snapshot is null");
        String serializedPredicate = extractSerializedPredicate();
        List<List<ManifestFile>> manifestGroups = buildPlanningTaskManifestGroups(snapshot,
                SerializationUtil.deserializeFromBase64(serializedPredicate), numBackends);
        List<Split> splits = new ArrayList<>();
        String serializedTable = SerializationUtil.serializeToBase64(
                SerializableTable.copyOf(getIcebergTableInstance()));
        for (List<ManifestFile> manifestGroup : manifestGroups) {
            splits.add(createMetadataPlanningSysSplit(snapshot.snapshotId(), serializedTable,
                    serializedPredicate, manifestGroup));
        }
        selectedPartitionNum = 0;
        return splits;
    }

    private TableScan createMetadataPlanningTableScan() throws UserException {
        TableScan scan = getIcebergTableInstance().newScan();
        IcebergTableQueryInfo info = getSpecifiedSnapshot();
        if (info != null) {
            if (info.getRef() != null) {
                scan = scan.useRef(info.getRef());
            } else {
                scan = scan.useSnapshot(info.getSnapshotId());
            }
        }
        return scan.planWith(getIcebergSource().getCatalog().getThreadPoolWithPreAuth());
    }

    private String extractSerializedPredicate() throws UserException {
        for (Expr conjunct : conjuncts) {
            if (!(conjunct instanceof BinaryPredicate)) {
                continue;
            }
            BinaryPredicate predicate = (BinaryPredicate) conjunct;
            if (predicate.getOp() != BinaryPredicate.Operator.EQ) {
                continue;
            }
            String serializedPredicate = extractPredicateLiteral(predicate.getChild(0), predicate.getChild(1));
            if (serializedPredicate != null) {
                return serializedPredicate;
            }
            serializedPredicate = extractPredicateLiteral(predicate.getChild(1), predicate.getChild(0));
            if (serializedPredicate != null) {
                return serializedPredicate;
            }
        }
        throw new UserException("Iceberg metadata planning query requires WHERE predicate = '<serialized_expr>'");
    }

    private String extractPredicateLiteral(Expr slotExpr, Expr literalExpr) {
        SlotRef slotRef = slotExpr.unwrapSlotRef();
        if (slotRef == null || literalExpr == null || !literalExpr.isLiteral()) {
            return null;
        }
        String columnName = slotRef.getColumnName();
        if (!IcebergMetadataPlanningExternalTable.CONTROL_PREDICATE_COLUMN.equalsIgnoreCase(columnName)) {
            return null;
        }
        Preconditions.checkState(literalExpr instanceof LiteralExpr,
                "Metadata planning predicate literal must be LiteralExpr");
        String value = ((LiteralExpr) literalExpr).getStringValue();
        Preconditions.checkState(value != null && !value.isEmpty(),
                "Metadata planning predicate literal must not be empty");
        return value;
    }

    private List<List<ManifestFile>> buildPlanningTaskManifestGroups(Snapshot snapshot,
            org.apache.iceberg.expressions.Expression filterExpr, int numBackends) {
        if (snapshot == null) {
            return Collections.emptyList();
        }
        int shardCount = Math.max(1, numBackends);
        List<List<ManifestFile>> shardManifests = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            shardManifests.add(new ArrayList<>());
        }
        try (org.apache.iceberg.io.CloseableIterator<ManifestFile> matchingManifests = IcebergUtils.getMatchingManifest(
                snapshot.dataManifests(getIcebergTableInstance().io()), getIcebergTableInstance().specs(),
                filterExpr).iterator()) {
            // Weighted greedy assignment: assign each manifest to the group with the current
            // lowest estimated file count, so that BEs receive roughly equal scanning work
            // even when individual manifests differ greatly in size.
            PriorityQueue<long[]> heap = new PriorityQueue<>(shardCount,
                    Comparator.comparingLong(a -> a[0])); // [0]=totalFileCount, [1]=groupIndex
            for (int i = 0; i < shardCount; i++) {
                heap.offer(new long[]{0L, i});
            }
            while (matchingManifests.hasNext()) {
                ManifestFile manifest = matchingManifests.next();
                long[] minGroup = heap.poll();
                shardManifests.get((int) minGroup[1]).add(manifest);
                long fileCount = (manifest.addedFilesCount() != null ? manifest.addedFilesCount() : 0L)
                        + (manifest.existingFilesCount() != null ? manifest.existingFilesCount() : 0L);
                minGroup[0] += Math.max(fileCount, 1L);
                heap.offer(minGroup);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to build Iceberg metadata planning task groups", e);
        }
        List<List<ManifestFile>> manifestGroups = new ArrayList<>();
        for (List<ManifestFile> manifests : shardManifests) {
            if (manifests.isEmpty()) {
                continue;
            }
            manifestGroups.add(manifests);
        }
        return manifestGroups;
    }

    private Split createMetadataPlanningSysSplit(long snapshotId, String serializedTable,
            String serializedPredicate, List<ManifestFile> manifestGroup) {
        IcebergSplit split = IcebergSplit.newSysTableSplit(
                serializeSplitInput(new MetadataPlanningSplitInput(snapshotId, serializedTable,
                        serializedPredicate, serializeManifests(manifestGroup))),
                manifestGroup.size());
        return split;
    }

    private String serializeSplitInput(MetadataPlanningSplitInput input) {
        try {
            return OBJECT_MAPPER.writeValueAsString(input);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize metadata planning split input", e);
        }
    }

    private List<String> serializeManifests(List<ManifestFile> manifests) {
        List<String> serializedManifests = new ArrayList<>(manifests.size());
        for (ManifestFile manifest : manifests) {
            try {
                // We intentionally pass serialized ManifestFile descriptors instead of only manifest paths.
                // If we pass only paths, every BE-side planning task must scan the full snapshot manifest list
                // again to recover path -> ManifestFile, which repeats metadata work for each task.
                // The descriptor is still lightweight compared with the manifest file contents themselves,
                // and lets BE read only the FE-selected candidate manifests.
                serializedManifests.add(Base64.getEncoder().encodeToString(ManifestFiles.encode(manifest)));
            } catch (IOException e) {
                throw new RuntimeException("Failed to serialize Iceberg manifest " + manifest.path(), e);
            }
        }
        return serializedManifests;
    }

    private static class MetadataPlanningSplitInput {
        public long snapshotId;
        public String serializedTable;
        public String serializedPredicate;
        public List<String> serializedManifests;

        private MetadataPlanningSplitInput(long snapshotId, String serializedTable, String serializedPredicate,
                List<String> serializedManifests) {
            this.snapshotId = snapshotId;
            this.serializedTable = serializedTable;
            this.serializedPredicate = serializedPredicate;
            this.serializedManifests = serializedManifests;
        }
    }
}
