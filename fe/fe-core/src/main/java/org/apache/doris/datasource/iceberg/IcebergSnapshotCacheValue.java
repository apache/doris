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
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
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
import javax.annotation.Nullable;

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
    /**
     * Execution context captured from the table generation this projection retains. Planning and
     * scanning the retained frozen table must run under this context; it is not part of the
     * per-value counted payload beyond the flat retained-context allowance the estimator adds.
     */
    @Nullable
    private transient volatile ExecutionAuthenticator capturedAuthenticator;

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
                if (names.size() > 1) {
                    // A field with several historical names keeps an element array per name.
                    payloadBytes = MetaCacheWeightUtils.saturatedAdd(payloadBytes,
                            MetaCacheWeightUtils.estimatedObjectArrayBytes(names.size()));
                }
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

    public IcebergSnapshotCacheValue bindCapturedAuthenticator(@Nullable ExecutionAuthenticator authenticator) {
        this.capturedAuthenticator = authenticator;
        return this;
    }

    @Nullable
    public ExecutionAuthenticator getCapturedAuthenticator() {
        return capturedAuthenticator;
    }

    /**
     * A relation pinned to this projection plans and scans the retained frozen table. That work
     * must run on the execution context captured with the projection's table generation: after a
     * credential/storage ALTER has installed a new catalog context, planning would otherwise run
     * the old generation's frozen operations and FileIO under the new authenticator, storage
     * state and pre-authenticated executor. Fail before planning instead; the retried statement
     * binds a coherent current generation.
     */
    public void ensurePlannableUnder(@Nullable ExecutionAuthenticator currentAuthenticator, String tableName) {
        ExecutionAuthenticator captured = capturedAuthenticator;
        if (captured != null && currentAuthenticator != null && captured != currentAuthenticator) {
            throw new IllegalStateException("Catalog execution context changed since this statement pinned"
                    + " its snapshot of " + tableName + ", please retry the query.");
        }
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
            sizeEstimate = MetaCacheSizeEstimator.estimateSafely("iceberg_snapshot_preparation_failed",
                    () -> {
                        // Account before serializing the current snapshot: v1 snapshot JSON
                        // materializes the transient manifest list that accounting rejects.
                        retainedTablePayloadBytes = icebergTable
                                .map(IcebergCacheSizeEstimator::retainedTablePayloadBytes).orElse(0L);
                        if (retainedCurrentSnapshotJson == null) {
                            retainedCurrentSnapshotJson = icebergTable
                                    .map(IcebergSnapshotCacheValue::retainCurrentSnapshotJson).orElse(null);
                        }
                        return IcebergCacheSizeEstimator.estimateSnapshotEntry(key, this);
                    });
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
        if (!(table instanceof HasTableOperations) || isFrozenGeneration(table)) {
            return table;
        }
        if (table instanceof QueryScopedTable) {
            // Already fixed to one admitted metadata generation and isolating its snapshot
            // copies from the cached BaseSnapshot instances. Rebuilding it as a plain BaseTable
            // would hand historical scans the shared snapshots, whose lazily materialized
            // manifest lists would then grow the cached generation past its admitted weight.
            return table;
        }
        TableOperations operations = ((HasTableOperations) table).operations();
        // Capture current() exactly once so every projection derived from the returned table sees
        // one metadata generation even when the shared catalog handle refreshes concurrently.
        TableOperations frozenOperations = new FrozenTableOperations(
                operations, operations.current(), false);
        return tableWithOperations(table, frozenOperations);
    }

    static Table retainNonGrowingGeneration(Table table) {
        if (!isFrozenGeneration(table) || isNonGrowingGeneration(table)) {
            return table;
        }
        TableOperations retainedOperations = ((HasTableOperations) table).operations();
        // Do not rebuild parsed metadata with Iceberg's write-side Builder. Builder validation and
        // ID reuse rules are intentionally stricter than metadata parsing and can reject legal
        // upgraded tables or renumber sparse/equivalent schema histories. The frozen metadata is
        // never exposed after query isolation; each caller receives exact query-local operations.
        return tableWithOperations(table, new FrozenTableOperations(
                retainedOperations, retainedOperations.current(), true));
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

    /**
     * True for any table bound to a retained metadata generation that cannot commit by itself:
     * a frozen generation, or a query-scoped view over one. Writers must re-base such tables onto
     * live operations through {@link #createWritableTable}.
     */
    static boolean isRetainedGeneration(Table table) {
        if (!(table instanceof HasTableOperations)) {
            return false;
        }
        TableOperations operations = ((HasTableOperations) table).operations();
        return operations instanceof FrozenTableOperations
                || operations instanceof QueryScopedTableOperations;
    }

    static TableOperations unwrapRetainedTableOperations(TableOperations operations) {
        TableOperations current = Objects.requireNonNull(operations, "operations can not be null");
        while (current instanceof RetainedTableOperations) {
            current = ((RetainedTableOperations) current).delegate;
        }
        return current;
    }

    static Table createWritableTable(Table retainedTable, Table liveTable) {
        if (!isRetainedGeneration(retainedTable)) {
            return retainedTable;
        }
        if (!(liveTable instanceof HasTableOperations)
                || isRetainedGeneration(liveTable)) {
            throw new IllegalArgumentException(
                    "Iceberg commit table must provide writable table operations");
        }
        TableOperations retainedOperations = ((HasTableOperations) retainedTable).operations();
        TableMetadata retainedMetadata = retainedOperations.current();
        TableOperations liveOperations = unwrapRetainedTableOperations(
                ((HasTableOperations) liveTable).operations());
        return tableWithOperations(retainedTable,
                new WritableTableOperations(liveOperations, retainedMetadata));
    }

    static boolean isNonGrowingGeneration(Table table) {
        return isFrozenGeneration(table)
                && ((FrozenTableOperations) ((HasTableOperations) table).operations()).nonGrowing;
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
        private final boolean nonGrowing;

        private FrozenTableOperations(TableOperations source, TableMetadata metadata,
                boolean nonGrowing) {
            this.metadata = metadata;
            this.fileIO = source.io();
            this.encryptionManager = source.encryption();
            this.locationProvider = source.locationProvider();
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
                        "Cannot retry Iceberg commit after the table UUID, schema, spec, sort "
                                + "order, location, format version, or table properties changed");
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
            // A dropped and recreated table can restart schema/spec/order ids at the same
            // location; only the same table UUID may absorb a retried commit.
            return Objects.equals(retainedMetadata.uuid(), refreshedMetadata.uuid())
                    && retainedMetadata.formatVersion() == refreshedMetadata.formatVersion()
                    && retainedMetadata.currentSchemaId() == refreshedMetadata.currentSchemaId()
                    && retainedMetadata.defaultSpecId() == refreshedMetadata.defaultSpecId()
                    && retainedMetadata.defaultSortOrderId() == refreshedMetadata.defaultSortOrderId()
                    && Objects.equals(retainedMetadata.location(), refreshedMetadata.location())
                    && Objects.equals(retainedMetadata.properties(), refreshedMetadata.properties());
        }
    }

    /**
     * Query-local read-only operations over the exact retained metadata. Only snapshot state is
     * isolated per query (see QueryScopedTable); TableMetadata, Schema, StructType and
     * PartitionSpec are shared with the cached generation, and their lazy indexes grow inside the
     * cache value. IcebergCacheSizeEstimator reserves that growth at publication.
     */
    private static final class QueryScopedTableOperations extends RetainedTableOperations {
        private QueryScopedTableOperations(TableOperations retainedOperations) {
            super(retainedOperations, retainedOperations.current());
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            throw new UnsupportedOperationException("Query-scoped Iceberg table is read-only");
        }
    }

    /**
     * A per-caller view whose Iceberg lazy snapshot state (manifest lists, manifests, files) is
     * never written into the cache value. It does not isolate schema/spec lazy indexes.
     */
    private static final class QueryScopedTable extends BaseTable {
        private final QueryScopedTableOperations queryOperations;
        private final Snapshot currentSnapshot;
        private final Map<Long, Snapshot> querySnapshots = new HashMap<>();

        private QueryScopedTable(TableOperations retainedOperations, String name,
                org.apache.iceberg.metrics.MetricsReporter reporter, String currentSnapshotJson) {
            this(new QueryScopedTableOperations(retainedOperations), name, reporter, currentSnapshotJson);
        }

        private QueryScopedTable(QueryScopedTableOperations queryOperations, String name,
                org.apache.iceberg.metrics.MetricsReporter reporter, String currentSnapshotJson) {
            super(queryOperations, name, reporter == null
                    ? org.apache.iceberg.metrics.LoggingMetricsReporter.instance() : reporter);
            this.queryOperations = queryOperations;
            this.currentSnapshot = currentSnapshotJson == null
                    ? null : SnapshotParser.fromJson(currentSnapshotJson);
            if (currentSnapshot != null) {
                querySnapshots.put(currentSnapshot.snapshotId(), currentSnapshot);
            }
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
            return copyForQuery(queryMetadata().snapshot(snapshotId));
        }

        @Override
        public Iterable<Snapshot> snapshots() {
            ImmutableList.Builder<Snapshot> snapshots = ImmutableList.builder();
            for (Snapshot snapshot : queryMetadata().snapshots()) {
                snapshots.add(copyForQuery(snapshot));
            }
            return snapshots.build();
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
            return queryOperations.current();
        }

        private synchronized Snapshot copyForQuery(Snapshot snapshot) {
            if (snapshot == null) {
                return null;
            }
            return querySnapshots.computeIfAbsent(snapshot.snapshotId(), ignored ->
                    SnapshotParser.fromJson(SnapshotParser.toJson(snapshot, false)));
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
