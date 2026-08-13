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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimator;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class IcebergSnapshotCacheValue {

    private final IcebergPartitionInfo partitionInfo;
    private final IcebergSnapshot snapshot;
    private final Optional<Map<Integer, List<String>>> nameMapping;
    private Optional<Table> icebergTable;
    private final long retainedNameMappingPayloadBytes;
    private String retainedCurrentSnapshotJson;
    private boolean queryIsolationPrepared;
    private long retainedTablePayloadBytes;
    private MetaCacheSizeEstimate sizeEstimate;

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot) {
        this(partitionInfo, snapshot, Optional.empty(), Optional.empty(), null, false);
    }

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping) {
        this(partitionInfo, snapshot, nameMapping, Optional.empty(), null, false);
    }

    public IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping, Table icebergTable) {
        this(partitionInfo, snapshot, nameMapping, Optional.of(icebergTable), null, false);
    }

    IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping, Table retainedTable,
            String retainedCurrentSnapshotJson) {
        this(partitionInfo, snapshot, nameMapping, Optional.of(retainedTable),
                retainedCurrentSnapshotJson, true);
    }

    private IcebergSnapshotCacheValue(IcebergPartitionInfo partitionInfo, IcebergSnapshot snapshot,
            Optional<Map<Integer, List<String>>> nameMapping, Optional<Table> icebergTable,
            String retainedCurrentSnapshotJson, boolean isolateForQueries) {
        this.partitionInfo = partitionInfo;
        this.snapshot = snapshot;
        // A cached BaseTable shares live TableOperations; retain a metadata-only generation so a
        // later commit through that same Table cannot move an already bound statement forward.
        this.icebergTable = icebergTable.map(IcebergSnapshotCacheValue::retainTableGeneration);
        this.retainedCurrentSnapshotJson = retainedCurrentSnapshotJson;
        if (isolateForQueries) {
            this.icebergTable = this.icebergTable.map(
                    IcebergSnapshotCacheValue::retainNonGrowingGeneration);
            this.queryIsolationPrepared = true;
        }
        if (nameMapping.isPresent()) {
            Map<Integer, List<String>> copy = new HashMap<>();
            long payloadBytes = 0L;
            for (Map.Entry<Integer, List<String>> entry : nameMapping.get().entrySet()) {
                List<String> names = ImmutableList.copyOf(entry.getValue());
                copy.put(entry.getKey(), names);
                for (String name : names) {
                    payloadBytes = MetaCacheWeightUtils.saturatedAdd(payloadBytes,
                            MetaCacheWeightUtils.estimatedStringBytes(name));
                }
            }
            this.nameMapping = Optional.of(Collections.unmodifiableMap(copy));
            this.retainedNameMappingPayloadBytes = payloadBytes;
        } else {
            this.nameMapping = Optional.empty();
            this.retainedNameMappingPayloadBytes = 0L;
        }
    }

    public IcebergPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public IcebergSnapshot getSnapshot() {
        return snapshot;
    }

    public Optional<Map<Integer, List<String>>> getNameMapping() {
        return nameMapping;
    }

    public Optional<Table> getIcebergTable() {
        return queryIsolationPrepared
                ? icebergTable.map(table -> createQueryScopedTable(
                        table, retainedCurrentSnapshotJson))
                : icebergTable;
    }

    MetaCacheSizeEstimate prepareForCachePublication(IcebergSnapshotEntryKey key) {
        if (sizeEstimate == null) {
            if (retainedCurrentSnapshotJson == null) {
                retainedCurrentSnapshotJson = icebergTable
                        .map(IcebergSnapshotCacheValue::retainCurrentSnapshotJson).orElse(null);
            }
            retainedTablePayloadBytes = icebergTable
                    .map(IcebergCacheSizeEstimator::retainedTablePayloadBytes).orElse(0L);
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("iceberg_snapshot_preparation_failed",
                    () -> IcebergCacheSizeEstimator.estimateSnapshotEntry(key, this));
            if (sizeEstimate.isComplete()) {
                icebergTable = icebergTable.map(
                        IcebergSnapshotCacheValue::retainNonGrowingGeneration);
                queryIsolationPrepared = true;
            }
        }
        return sizeEstimate;
    }

    public MetaCacheSizeEstimate getSizeEstimate() {
        return sizeEstimate == null
                ? MetaCacheSizeEstimate.incomplete("not_prepared") : sizeEstimate;
    }

    long getRetainedNameMappingPayloadBytes() {
        return retainedNameMappingPayloadBytes;
    }

    long getRetainedTablePayloadBytes() {
        return retainedTablePayloadBytes;
    }

    long getRetainedCurrentSnapshotPayloadBytes() {
        return retainedSnapshotJsonBytes(retainedCurrentSnapshotJson);
    }

    Optional<Table> getRetainedIcebergTable() {
        return icebergTable;
    }

    static Table retainTableGeneration(Table table) {
        return retainTableGeneration(table, null);
    }

    static Table retainTableGeneration(Table table, ExecutionAuthenticator authenticator) {
        if (!(table instanceof HasTableOperations) || isFrozenGeneration(table)) {
            return table;
        }
        TableOperations operations = ((HasTableOperations) table).operations();
        // Capture current() exactly once so every projection derived from the returned table sees
        // one metadata generation even when the shared catalog handle refreshes concurrently.
        TableOperations frozenOperations = new FrozenTableOperations(
                operations, operations.current(), authenticator);
        return tableWithOperations(table, frozenOperations);
    }

    static Table retainNonGrowingGeneration(Table table) {
        if (!isFrozenGeneration(table)) {
            return table;
        }
        TableOperations retainedOperations = ((HasTableOperations) table).operations();
        TableMetadata source = retainedOperations.current();
        if (source.schemas().isEmpty() || source.specs().isEmpty()
                || source.sortOrders().isEmpty()) {
            return table;
        }
        TableMetadata.Builder builder = TableMetadata.buildFromEmpty(source.formatVersion());
        if (source.uuid() != null) {
            builder.assignUUID(source.uuid());
        }
        for (Schema schema : source.schemas()) {
            builder.addSchema(schema);
        }
        builder.setCurrentSchema(source.currentSchemaId());
        for (PartitionSpec spec : source.specs()) {
            builder.addPartitionSpec(spec);
        }
        builder.setDefaultPartitionSpec(source.defaultSpecId());
        for (SortOrder sortOrder : source.sortOrders()) {
            builder.addSortOrder(sortOrder);
        }
        builder.setDefaultSortOrder(source.defaultSortOrderId());
        builder.setLocation(source.location());
        builder.setProperties(source.properties());
        if (source.currentSnapshot() != null) {
            builder.setBranchSnapshot(
                    new NonGrowingSnapshot(source.currentSnapshot()), SnapshotRef.MAIN_BRANCH);
        }
        TableMetadata retainedMetadata = builder.discardChanges()
                .withMetadataLocation(source.metadataFileLocation()).build();
        return tableWithOperations(table, new FrozenTableOperations(
                retainedOperations, retainedMetadata));
    }

    static String retainCurrentSnapshotJson(Table table) {
        if (!(table instanceof HasTableOperations)) {
            return null;
        }
        TableMetadata metadata = ((HasTableOperations) table).operations().current();
        Snapshot snapshot = metadata == null ? null : metadata.currentSnapshot();
        return snapshot == null ? null : SnapshotParser.toJson(snapshot, false);
    }

    static long retainedSnapshotJsonBytes(String snapshotJson) {
        return MetaCacheWeightUtils.estimatedStringBytes(snapshotJson);
    }

    static Table createQueryScopedTable(Table retainedTable, String currentSnapshotJson) {
        if (!isFrozenGeneration(retainedTable)) {
            return retainedTable;
        }
        TableOperations retainedOperations = ((HasTableOperations) retainedTable).operations();
        if (retainedTable instanceof BaseTable) {
            return new QueryScopedTable(retainedOperations, retainedTable.name(),
                    ((BaseTable) retainedTable).reporter(), currentSnapshotJson);
        }
        return new QueryScopedTable(retainedOperations, retainedTable.name(), null,
                currentSnapshotJson);
    }

    static void loadQueryMetadataForStatement(Table table) {
        if (table instanceof QueryScopedTable) {
            ((QueryScopedTable) table).queryMetadata();
        }
    }

    static boolean isFrozenGeneration(Table table) {
        return table instanceof HasTableOperations
                && ((HasTableOperations) table).operations() instanceof FrozenTableOperations;
    }

    static TableOperations unwrapRetainedTableOperations(TableOperations operations) {
        TableOperations current = Objects.requireNonNull(operations, "operations can not be null");
        while (current instanceof RetainedTableOperations) {
            current = ((RetainedTableOperations) current).delegate;
        }
        return current;
    }

    static Table createWritableTable(
            Table retainedTable, Table liveTable, boolean reloadRetainedMetadata) {
        if (!isFrozenGeneration(retainedTable)) {
            return retainedTable;
        }
        if (!(liveTable instanceof HasTableOperations)
                || isFrozenGeneration(liveTable)) {
            throw new IllegalArgumentException(
                    "Iceberg commit table must provide writable table operations");
        }
        TableOperations retainedOperations = ((HasTableOperations) retainedTable).operations();
        TableMetadata retainedMetadata = reloadRetainedMetadata
                ? loadQueryMetadata(retainedOperations) : retainedOperations.current();
        TableOperations liveOperations = unwrapRetainedTableOperations(
                ((HasTableOperations) liveTable).operations());
        return tableWithOperations(retainedTable,
                new WritableTableOperations(liveOperations, retainedMetadata));
    }

    static Table createWritableTable(Table retainedTable, Table liveTable) {
        return createWritableTable(retainedTable, liveTable,
                isNonGrowingGeneration(retainedTable));
    }

    private static boolean isNonGrowingGeneration(Table table) {
        return isFrozenGeneration(table)
                && ((FrozenTableOperations) ((HasTableOperations) table).operations()).nonGrowing;
    }

    private static TableMetadata loadQueryMetadata(TableOperations retainedOperations) {
        TableMetadata retainedMetadata = retainedOperations.current();
        TableOperations serviceOperations = unwrapRetainedTableOperations(retainedOperations);
        String metadataLocation = retainedMetadata.metadataFileLocation();
        if (metadataLocation != null && !metadataLocation.isEmpty() && serviceOperations.io() != null) {
            ExecutionAuthenticator authenticator = retainedOperations instanceof FrozenTableOperations
                    ? ((FrozenTableOperations) retainedOperations).authenticator : null;
            try {
                return authenticator == null
                        ? TableMetadataParser.read(serviceOperations.io(), metadataLocation)
                        : authenticator.execute(
                                () -> TableMetadataParser.read(serviceOperations.io(), metadataLocation));
            } catch (Exception e) {
                throw new StaleMetadataException(
                        "Iceberg metadata generation is no longer readable: " + metadataLocation, e);
            }
        }
        throw new IllegalStateException(
                "Iceberg query-local metadata requires a stable metadata location and FileIO");
    }

    private static Table tableWithOperations(Table table, TableOperations operations) {
        if (table instanceof BaseTable) {
            return new BaseTable(operations, table.name(), ((BaseTable) table).reporter());
        }
        return new BaseTable(operations, table.name());
    }

    private abstract static class RetainedTableOperations implements TableOperations {
        protected final TableOperations delegate;
        private final TableMetadata metadata;

        private RetainedTableOperations(TableOperations delegate, TableMetadata metadata) {
            this.delegate = delegate;
            this.metadata = metadata;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public FileIO io() {
            return delegate.io();
        }

        @Override
        public EncryptionManager encryption() {
            return delegate.encryption();
        }

        @Override
        public String metadataFileLocation(String fileName) {
            return delegate.metadataFileLocation(fileName);
        }

        @Override
        public LocationProvider locationProvider() {
            return delegate.locationProvider();
        }
    }

    private static class FrozenTableOperations implements TableOperations {
        private final TableMetadata metadata;
        private final FileIO fileIO;
        private final EncryptionManager encryptionManager;
        private final LocationProvider locationProvider;
        private final ExecutionAuthenticator authenticator;
        private final boolean nonGrowing;

        private FrozenTableOperations(TableOperations source, TableMetadata metadata) {
            this(source, metadata, source instanceof FrozenTableOperations
                    ? ((FrozenTableOperations) source).authenticator : null, true);
        }

        private FrozenTableOperations(TableOperations source, TableMetadata metadata,
                ExecutionAuthenticator authenticator) {
            this(source, metadata, authenticator, false);
        }

        private FrozenTableOperations(TableOperations source, TableMetadata metadata,
                ExecutionAuthenticator authenticator, boolean nonGrowing) {
            this.metadata = metadata;
            this.fileIO = source.io();
            this.encryptionManager = source.encryption();
            this.locationProvider = source.locationProvider();
            this.authenticator = authenticator;
            this.nonGrowing = nonGrowing;
        }

        @Override
        public TableMetadata current() {
            return metadata;
        }

        @Override
        public TableMetadata refresh() {
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata) {
            throw new UnsupportedOperationException("Frozen Iceberg table generation is read-only");
        }

        @Override
        public FileIO io() {
            return fileIO;
        }

        @Override
        public EncryptionManager encryption() {
            return encryptionManager;
        }

        @Override
        public String metadataFileLocation(String fileName) {
            String metadataLocation = metadata.metadataFileLocation();
            if (metadataLocation == null) {
                throw new UnsupportedOperationException(
                        "Frozen Iceberg table has no metadata directory");
            }
            int separator = metadataLocation.lastIndexOf('/');
            return separator < 0 ? fileName
                    : metadataLocation.substring(0, separator + 1) + fileName;
        }

        @Override
        public LocationProvider locationProvider() {
            return locationProvider;
        }
    }

    private static class WritableTableOperations extends RetainedTableOperations {
        private final TableMetadata retainedMetadata;
        private TableMetadata currentMetadata;

        private WritableTableOperations(TableOperations delegate, TableMetadata retainedMetadata) {
            super(delegate, retainedMetadata);
            this.retainedMetadata = retainedMetadata;
            this.currentMetadata = retainedMetadata;
        }

        @Override
        public TableMetadata current() {
            return currentMetadata;
        }

        @Override
        public TableMetadata refresh() {
            TableMetadata refreshedMetadata = delegate.refresh();
            // Data-only snapshot advances are safe to replay, but a changed writer contract must
            // fail instead of silently committing files produced for another metadata generation.
            if (!isWriterCompatible(refreshedMetadata)) {
                throw new CommitFailedException(
                        "Cannot retry Iceberg commit after schema, spec, sort order, location, "
                                + "format version, or table properties changed");
            }
            currentMetadata = refreshedMetadata;
            return refreshedMetadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata newMetadata) {
            TableMetadata delegateBase = prepareDelegateCommit(delegate, base, currentMetadata);
            delegate.commit(delegateBase, newMetadata);
            currentMetadata = delegate.current();
        }

        private boolean isWriterCompatible(TableMetadata refreshedMetadata) {
            return retainedMetadata.formatVersion() == refreshedMetadata.formatVersion()
                    && retainedMetadata.currentSchemaId() == refreshedMetadata.currentSchemaId()
                    && retainedMetadata.defaultSpecId() == refreshedMetadata.defaultSpecId()
                    && retainedMetadata.defaultSortOrderId() == refreshedMetadata.defaultSortOrderId()
                    && Objects.equals(retainedMetadata.location(), refreshedMetadata.location())
                    && Objects.equals(retainedMetadata.properties(), refreshedMetadata.properties());
        }
    }

    /** A per-caller view whose Iceberg lazy snapshot state is never written into the cache value. */
    private static final class QueryScopedTable extends BaseTable {
        private final TableOperations retainedOperations;
        private final Snapshot currentSnapshot;
        private TableMetadata queryMetadata;

        private QueryScopedTable(TableOperations retainedOperations, String name,
                org.apache.iceberg.metrics.MetricsReporter reporter, String currentSnapshotJson) {
            super(retainedOperations, name, reporter == null
                    ? org.apache.iceberg.metrics.LoggingMetricsReporter.instance() : reporter);
            this.retainedOperations = retainedOperations;
            this.currentSnapshot = currentSnapshotJson == null
                    ? null : SnapshotParser.fromJson(currentSnapshotJson);
        }

        @Override
        public Snapshot currentSnapshot() {
            return currentSnapshot;
        }

        @Override
        public Snapshot snapshot(long snapshotId) {
            if (currentSnapshot != null && currentSnapshot.snapshotId() == snapshotId) {
                return currentSnapshot;
            }
            return queryMetadata().snapshot(snapshotId);
        }

        @Override
        public Iterable<Snapshot> snapshots() {
            return queryMetadata().snapshots();
        }

        @Override
        public List<HistoryEntry> history() {
            return queryMetadata().snapshotLog();
        }

        @Override
        public Map<String, SnapshotRef> refs() {
            return queryMetadata().refs();
        }

        @Override
        public List<org.apache.iceberg.StatisticsFile> statisticsFiles() {
            return queryMetadata().statisticsFiles();
        }

        @Override
        public List<org.apache.iceberg.PartitionStatisticsFile> partitionStatisticsFiles() {
            return queryMetadata().partitionStatisticsFiles();
        }

        private synchronized TableMetadata queryMetadata() {
            if (queryMetadata == null) {
                queryMetadata = loadQueryMetadata(retainedOperations);
            }
            return queryMetadata;
        }
    }

    static final class StaleMetadataException extends RuntimeException {
        private StaleMetadataException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Scalar-only snapshot retained by the cache-owned metadata generation. */
    private static final class NonGrowingSnapshot implements Snapshot {
        private final long sequenceNumber;
        private final long snapshotId;
        private final Long parentId;
        private final long timestampMillis;
        private final Integer schemaId;
        private final Long firstRowId;
        private final Long addedRows;

        private NonGrowingSnapshot(Snapshot snapshot) {
            this.sequenceNumber = snapshot.sequenceNumber();
            this.snapshotId = snapshot.snapshotId();
            this.parentId = snapshot.parentId();
            this.timestampMillis = snapshot.timestampMillis();
            this.schemaId = snapshot.schemaId();
            this.firstRowId = snapshot.firstRowId();
            this.addedRows = snapshot.addedRows();
        }

        @Override
        public long sequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public long snapshotId() {
            return snapshotId;
        }

        @Override
        public Long parentId() {
            return parentId;
        }

        @Override
        public long timestampMillis() {
            return timestampMillis;
        }

        @Override
        public List<ManifestFile> allManifests(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public List<ManifestFile> dataManifests(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public List<ManifestFile> deleteManifests(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public String operation() {
            return null;
        }

        @Override
        public Map<String, String> summary() {
            return Collections.emptyMap();
        }

        @Override
        public Iterable<DataFile> addedDataFiles(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public Iterable<DataFile> removedDataFiles(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public Iterable<DeleteFile> addedDeleteFiles(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public Iterable<DeleteFile> removedDeleteFiles(FileIO fileIO) {
            throw queryScopedSnapshotRequired();
        }

        @Override
        public String manifestListLocation() {
            return null;
        }

        @Override
        public Integer schemaId() {
            return schemaId;
        }

        @Override
        public Long firstRowId() {
            return firstRowId;
        }

        @Override
        public Long addedRows() {
            return addedRows;
        }

        @Override
        public String keyId() {
            return null;
        }

        private UnsupportedOperationException queryScopedSnapshotRequired() {
            return new UnsupportedOperationException(
                    "Cache-owned Iceberg snapshots cannot materialize manifests or files");
        }
    }

    private static TableMetadata prepareDelegateCommit(TableOperations delegate,
            TableMetadata base, TableMetadata wrapperCurrent) {
        if (base != wrapperCurrent) {
            throw new CommitFailedException("Cannot commit from a stale Iceberg table view");
        }
        TableMetadata delegateCurrent = delegate.current();
        if (!isSameGeneration(base, delegateCurrent)) {
            throw new CommitFailedException("Cannot commit from a stale Iceberg metadata generation");
        }
        return delegateCurrent;
    }

    private static boolean isSameGeneration(TableMetadata retained, TableMetadata live) {
        if (retained == live) {
            return true;
        }
        if (retained == null || live == null) {
            return false;
        }
        if (!Objects.equals(retained.uuid(), live.uuid())) {
            return false;
        }
        if (retained.metadataFileLocation() != null || live.metadataFileLocation() != null) {
            return Objects.equals(retained.metadataFileLocation(), live.metadataFileLocation());
        }
        return retained.lastUpdatedMillis() == live.lastUpdatedMillis()
                && retained.lastSequenceNumber() == live.lastSequenceNumber()
                && retained.currentSchemaId() == live.currentSchemaId()
                && retained.defaultSpecId() == live.defaultSpecId()
                && retained.defaultSortOrderId() == live.defaultSortOrderId()
                && Objects.equals(retained.location(), live.location())
                && Objects.equals(retained.properties(), live.properties());
    }
}
