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

package org.apache.doris.connector.delta;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.Protocol;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Loads a filesystem-managed Delta table snapshot through Delta Kernel. */
public class DeltaKernelSnapshotLoader {
    private static final String CATALOG_MANAGED_FEATURE = "catalogManaged";
    private static final String PARQUET_PROVIDER = "parquet";
    private static final int MAX_SUPPORTED_READER_VERSION = 3;
    /**
     * Features whose read semantics are implemented by the native connector.  This list is
     * intentionally explicit: a new Delta reader feature must be reviewed before a table using it
     * can be scanned as ordinary Parquet.
     */
    private static final Set<String> SUPPORTED_DIRECT_PARQUET_READER_FEATURES = Set.of(
            "columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint",
            "vacuumProtocolCheck");
    private static final Set<String> UNSUPPORTED_DIRECT_PARQUET_READER_FEATURES = Set.of(
            "geospatial",
            "typeWidening",
            "typeWidening-preview",
            "variantShredding",
            "variantShredding-preview",
            "variantType",
            "variantType-preview");

    private final Engine engine;

    public DeltaKernelSnapshotLoader(Engine engine) {
        this.engine = engine;
    }

    public DeltaKernelSnapshot load(String tablePath) throws IOException {
        Snapshot snapshot = Table.forPath(engine, tablePath).getLatestSnapshot(engine);
        return loadSnapshot(snapshot, false);
    }

    public DeltaKernelSnapshot loadLatest(String tablePath) throws IOException {
        return load(tablePath);
    }

    public DeltaKernelSnapshot load(String tablePath, long version) throws IOException {
        Snapshot snapshot = Table.forPath(engine, tablePath).getSnapshotAsOfVersion(engine, version);
        return loadSnapshot(snapshot, false);
    }

    public DeltaKernelSnapshot loadVersion(String tablePath, long version) throws IOException {
        return load(tablePath, version);
    }

    public DeltaKernelSnapshot loadTimestamp(String tablePath, long timestampMillis)
            throws IOException {
        Snapshot snapshot = Table.forPath(engine, tablePath)
                .getSnapshotAsOfTimestamp(engine, timestampMillis);
        return loadSnapshot(snapshot, false);
    }

    DeltaKernelSnapshot loadCatalogManagedSnapshot(Snapshot snapshot) throws IOException {
        return loadSnapshot(snapshot, true);
    }

    private DeltaKernelSnapshot loadSnapshot(
            Snapshot snapshot, boolean catalogManagedRead) throws IOException {
        validateSupportedTableFeatures(snapshot, catalogManagedRead);
        Protocol protocol = ((SnapshotImpl) snapshot).getProtocol();
        Scan scan = snapshot.getScanBuilder().build();
        List<DeltaScanFile> activeFiles = new ArrayList<>();
        List<DeltaRemoveFile> activeRemoveFiles = new ArrayList<>();
        List<String> partitionColumns = snapshot.getPartitionColumnNames();

        try (CloseableIterator<FilteredColumnarBatch> batches =
                ((ScanImpl) scan).getScanFiles(engine, true)) {
            while (batches.hasNext()) {
                try (CloseableIterator<Row> rows = batches.next().getRows()) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        // Kernel's public Scan API returns nested scan rows. Keep this
                        // version-specific extraction in the adapter so the rest of the
                        // connector does not depend on Kernel's internal package.
                        DeletionVectorDescriptor deletionVector =
                                InternalScanFileUtils.getDeletionVectorDescriptorFromRow(row);

                        FileStatus file = InternalScanFileUtils.getAddFileStatus(row);
                        if (file.getSize() <= 0) {
                            throw new IllegalArgumentException(
                                    "Delta data file has an invalid size: " + file.getPath());
                        }
                        Map<String, String> partitionValues =
                                InternalScanFileUtils.getPartitionValues(row);
                        activeFiles.add(new DeltaScanFile(
                                file.getPath(), file.getSize(), file.getModificationTime(),
                                orderedPartitionValues(partitionValues, partitionColumns),
                                toDeletionVector(deletionVector), snapshot.getPath()));
                        activeRemoveFiles.add(new DeltaRemoveFile(
                                InternalScanFileUtils.getFilePath(row), file.getSize(),
                                partitionValues, deletionVector,
                                InternalScanFileUtils.getBaseRowId(row),
                                InternalScanFileUtils.getDefaultRowCommitVersion(row),
                                new AddFile(row.getStruct(InternalScanFileUtils.ADD_FILE_ORDINAL))
                                        .getNumRecords()));
                    }
                }
            }
        }
        activeFiles.sort(Comparator.comparing(DeltaScanFile::getPath));
        activeRemoveFiles.sort(Comparator.comparing(DeltaRemoveFile::getPath));
        return new DeltaKernelSnapshot(snapshot.getPath(), snapshot.getVersion(),
                snapshot.getSchema(), partitionColumns, snapshot.getTableProperties(),
                protocol.getMinWriterVersion(), protocol.getWriterFeatures(), activeFiles,
                activeRemoveFiles);
    }

    private static Map<String, String> orderedPartitionValues(Map<String, String> values,
            List<String> partitionColumns) {
        Map<String, String> orderedValues = new LinkedHashMap<>();
        for (String column : partitionColumns) {
            if (!values.containsKey(column)) {
                throw new IllegalArgumentException(
                        "Delta add-file is missing partition value for column " + column);
            }
            orderedValues.put(column, values.get(column));
        }
        return orderedValues;
    }

    private static DeltaDeletionVector toDeletionVector(DeletionVectorDescriptor descriptor) {
        if (descriptor == null) {
            return null;
        }
        return new DeltaDeletionVector(descriptor.getStorageType(), descriptor.getPathOrInlineDv(),
                descriptor.getOffset(), descriptor.getSizeInBytes(), descriptor.getCardinality());
    }

    private static void validateSupportedTableFeatures(
            Snapshot snapshot, boolean catalogManagedRead) {
        String formatProvider = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
        if (!PARQUET_PROVIDER.equalsIgnoreCase(formatProvider)) {
            throw new UnsupportedOperationException(
                    "Doris native Delta reader only supports Parquet data files, but table format "
                            + "provider is '" + formatProvider + "'");
        }
        Protocol protocol = ((SnapshotImpl) snapshot).getProtocol();
        if (protocol.getMinReaderVersion() > MAX_SUPPORTED_READER_VERSION) {
            throw new UnsupportedOperationException(
                    "Delta reader version " + protocol.getMinReaderVersion()
                            + " is newer than the native Doris reader supports ("
                            + MAX_SUPPORTED_READER_VERSION + ")");
        }
        Set<String> readerFeatures = protocol.getReaderFeatures();
        if (readerFeatures.contains(CATALOG_MANAGED_FEATURE) && !catalogManagedRead) {
            throw new UnsupportedOperationException(
                    "Catalog-managed Delta tables must be loaded through a catalog-aware adapter");
        }

        Set<String> unsupportedFeatures = new TreeSet<>(readerFeatures);
        unsupportedFeatures.removeAll(SUPPORTED_DIRECT_PARQUET_READER_FEATURES);
        if (catalogManagedRead) {
            unsupportedFeatures.remove(CATALOG_MANAGED_FEATURE);
        }
        unsupportedFeatures.addAll(intersect(readerFeatures,
                UNSUPPORTED_DIRECT_PARQUET_READER_FEATURES));
        if (!unsupportedFeatures.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Delta reader features require physical-row transforms that are not supported "
                            + "by the Doris native Parquet scan: " + unsupportedFeatures);
        }

    }

    private static Set<String> intersect(Set<String> values, Set<String> candidates) {
        Set<String> result = new TreeSet<>(values);
        result.retainAll(candidates);
        return result;
    }
}
