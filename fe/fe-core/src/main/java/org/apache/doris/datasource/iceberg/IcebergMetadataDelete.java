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

import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache.WritableTableLease;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.function.BooleanSupplier;

/**
 * Whole-file metadata delete for Iceberg tables.
 *
 * <p>This path uses the existing Iceberg metadata and cache-generation machinery. Candidate files
 * must all match in full; partial matches and existing delete files retain row-level DELETE.
 * The final refresh selects the current snapshot, as in the source feature. Later snapshot changes
 * decline the shortcut, while cancellation prevents publication of the pending delete.
 */
public final class IcebergMetadataDelete {
    private static final Logger LOG = LogManager.getLogger(IcebergMetadataDelete.class);

    private IcebergMetadataDelete() {
    }

    /**
     * Attempts the fast path against a statement-bound table generation.
     *
     * @return the number of rows removed when committed, otherwise empty to run row-level DELETE
     */
    public static OptionalLong execute(IcebergExternalTable dorisTable, IcebergSnapshotCacheValue targetSnapshot,
            Expression deleteExpression, BooleanSupplier isCancelled) throws UserException {
        Table targetTable = targetSnapshot.getIcebergTable().orElseThrow(
                () -> new UserException("Iceberg delete target metadata is not available"));
        // HMS catalogs retain their Iceberg ops separately from their Hive metadata ops. Use the
        // context pinned by analysis, so a catalog reinitialization cannot mix generations either.
        IcebergRuntimeContext runtimeContext = Preconditions.checkNotNull(targetSnapshot.getRuntimeContext(),
                "Iceberg delete target runtime context is not available");
        IcebergMetadataOps expectedOps = runtimeContext.getMetadataOps();
        try (WritableTableLease lease = IcebergUtils.acquireWritableIcebergTable(dorisTable, expectedOps)) {
            Table writableTable = lease.getTable();
            if (IcebergSnapshotCacheValue.isRetainedGeneration(targetTable)) {
                writableTable = IcebergSnapshotCacheValue.createWritableTable(targetTable, writableTable);
            }
            Table table = writableTable;
            return lease.getAuthenticator().execute(() -> executeOnTable(
                    table, targetTable, lease.getOps().getThreadPoolWithPreAuth(),
                    deleteExpression, isCancelled));
        } catch (ValidationException e) {
            LOG.info("Metadata delete of {} is no longer whole-file ({}); falling back.",
                    dorisTable.getName(), e.getMessage());
            return OptionalLong.empty();
        } catch (UserException e) {
            throw e;
        } catch (Exception e) {
            throw new UserException("Failed to metadata-delete Iceberg table "
                    + dorisTable.getName() + ": " + e.getMessage(), e);
        }
    }

    /**
     * Core operation over a loaded writable table. This remains separate from catalog acquisition so
     * it can be exercised against an InMemoryCatalog in unit tests.
     */
    @VisibleForTesting
    static OptionalLong executeOnTable(Table table, Table statementTable, ExecutorService executor,
            Expression deleteExpression, BooleanSupplier isCancelled) throws UserException {
        if (isCancelled.getAsBoolean()) {
            return OptionalLong.empty();
        }
        if (!(table instanceof BaseTable)
                || ((BaseTable) table).operations().current().formatVersion() < 2) {
            return OptionalLong.empty();
        }
        validateTableIdentity(statementTable, table);

        Snapshot base = table.currentSnapshot();
        if (base == null || hasLiveDeleteFiles(table, base)) {
            return OptionalLong.empty();
        }

        if (!matchesWholeFilesOnly(table, deleteExpression, executor) || isCancelled.getAsBoolean()) {
            return OptionalLong.empty();
        }

        // Re-read immediately before commit. A racing writer can add a candidate file or a delete
        // file; the required snapshot check below then aborts instead of changing the deleted set.
        table.refresh();
        validateTableIdentity(statementTable, table);
        Snapshot refreshed = table.currentSnapshot();
        if (refreshed == null || hasLiveDeleteFiles(table, refreshed)) {
            return OptionalLong.empty();
        }
        long requiredSnapshotId = refreshed.snapshotId();

        try {
            // The SDK update owns cleanup directly. A discarded BaseTransaction would only
            // enqueue deletion of staged manifests, leaking them on cancellation or validation.
            TableOperations liveOperations = IcebergSnapshotCacheValue.unwrapRetainedTableOperations(
                    ((BaseTable) table).operations());
            RequiredSnapshotTableOperations operations = new RequiredSnapshotTableOperations(
                    liveOperations, requiredSnapshotId, ((BaseTable) table).operations().current(), isCancelled);
            Table deleteTable = new BaseTable(operations, table.name(), ((BaseTable) table).reporter());
            DeleteFiles deleteFiles = deleteTable.newDelete();
            if (executor != null) {
                deleteFiles = deleteFiles.scanManifestsWith(executor);
            }
            deleteFiles = deleteFiles.caseSensitive(false).deleteFromRowFilter(deleteExpression);
            if (isCancelled.getAsBoolean()) {
                return OptionalLong.empty();
            }
            deleteFiles.commit();

            // Read the immutable metadata passed to this successful CAS. The live table can
            // already contain another writer's snapshot when SDK post-commit cleanup refreshes it.
            Snapshot committed = operations.getCommittedSnapshot();
            if (committed == null || committed.snapshotId() == requiredSnapshotId) {
                return OptionalLong.of(0L);
            }
            return OptionalLong.of(deletedRowCount(committed));
        } catch (ValidationException e) {
            // Nothing was committed. A concurrent writer made at least one candidate partial.
            LOG.info("Metadata delete of {} is no longer whole-file ({}); falling back.",
                    table.name(), e.getMessage());
            return OptionalLong.empty();
        }
    }

    /**
     * Every candidate file returned by the same inclusive scan the statement would use must satisfy
     * its strict partition projection or the strict file-metrics evaluator.
     */
    private static boolean matchesWholeFilesOnly(
            Table table, Expression deleteExpression, ExecutorService executor) {
        boolean caseSensitive = false;
        if (ExpressionUtil.selectsPartitions(deleteExpression, table, caseSensitive)) {
            return true;
        }
        TableScan scan = table.newScan()
                .filter(deleteExpression)
                .caseSensitive(caseSensitive)
                .includeColumnStats()
                .ignoreResiduals();
        if (executor != null) {
            // Candidate planning can fan out as well as commit manifest filtering. Both must use
            // the pre-authenticated executor retained with this writable table generation.
            scan = scan.planWith(executor);
        }
        try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
            Map<Integer, Evaluator> partitionEvaluators = new HashMap<>();
            StrictMetricsEvaluator metricsEvaluator =
                    new StrictMetricsEvaluator(table.schema(), deleteExpression, caseSensitive);
            for (FileScanTask task : tasks) {
                DataFile file = task.file();
                PartitionSpec spec = task.spec();
                Evaluator evaluator = partitionEvaluators.computeIfAbsent(spec.specId(), ignored ->
                        new Evaluator(spec.partitionType(),
                                Projections.strict(spec, caseSensitive).project(deleteExpression)));
                if (!evaluator.eval(file.partition()) && !metricsEvaluator.eval(file)) {
                    return false;
                }
            }
            return true;
        } catch (IOException e) {
            LOG.warn("Failed to evaluate whole-file delete candidates for {}: {}",
                    table.name(), e.getMessage());
            return false;
        }
    }

    private static boolean hasLiveDeleteFiles(Table table, Snapshot snapshot) {
        try {
            for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
                try (ManifestReader<DeleteFile> reader =
                        ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                    if (reader.iterator().hasNext()) {
                        return true;
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return false;
    }

    private static long deletedRowCount(Snapshot committed) {
        if (committed == null || committed.summary() == null) {
            return 0L;
        }
        Map<String, String> summary = committed.summary();
        long deletedRecords = Long.parseLong(
                summary.getOrDefault(SnapshotSummary.DELETED_RECORDS_PROP, "0"));
        long removedPositionDeletes = Long.parseLong(
                summary.getOrDefault(SnapshotSummary.REMOVED_POS_DELETES_PROP, "0"));
        return Math.max(0L, deletedRecords - removedPositionDeletes);
    }

    private static void validateTableIdentity(Table expected, Table current) throws UserException {
        Preconditions.checkState(expected instanceof HasTableOperations,
                "Iceberg delete target %s does not expose table operations", expected.name());
        Preconditions.checkState(current instanceof HasTableOperations,
                "Iceberg commit table %s does not expose table operations", current.name());
        TableMetadata expectedMetadata = ((HasTableOperations) expected).operations().current();
        TableMetadata currentMetadata = ((HasTableOperations) current).operations().current();
        if (!sameWriteIdentity(expectedMetadata, currentMetadata)) {
            throw new UserException("Iceberg write metadata changed after the write was bound; "
                    + "retry the statement");
        }
    }

    private static boolean sameWriteIdentity(TableMetadata expected, TableMetadata current) {
        return Objects.equals(expected.uuid(), current.uuid())
                && expected.formatVersion() == current.formatVersion()
                && expected.currentSchemaId() == current.currentSchemaId()
                && expected.defaultSpecId() == current.defaultSpecId()
                && expected.defaultSortOrderId() == current.defaultSortOrderId()
                && Objects.equals(expected.location(), current.location())
                && Objects.equals(expected.properties(), current.properties());
    }

    /**
     * Fences the final CAS, including Iceberg retry replays, to the snapshot inspected by the
     * candidate preflight. This turns a concurrent append/delete into a safe row-level fallback.
     */
    private static final class RequiredSnapshotTableOperations implements TableOperations {
        private final TableOperations delegate;
        private final long requiredSnapshotId;
        private final TableMetadata expectedMetadata;
        private final BooleanSupplier isCancelled;
        private Snapshot committedSnapshot;

        private RequiredSnapshotTableOperations(TableOperations delegate, long requiredSnapshotId,
                TableMetadata expectedMetadata, BooleanSupplier isCancelled) {
            this.delegate = delegate;
            this.requiredSnapshotId = requiredSnapshotId;
            this.expectedMetadata = expectedMetadata;
            this.isCancelled = isCancelled;
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            if (committedSnapshot != null) {
                // SnapshotProducer refreshes for best-effort cleanup after its durable commit.
                // Later changes or cancellation must not turn that success into a fallback.
                return delegate.refresh();
            }
            checkNotCancelled();
            TableMetadata metadata = delegate.refresh();
            checkMetadata(metadata);
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            checkNotCancelled();
            TableMetadata refreshed = Preconditions.checkNotNull(
                    delegate.refresh(), "Iceberg table metadata changed before metadata delete");
            String metadataFileLocation = Preconditions.checkNotNull(
                    refreshed.metadataFileLocation(), "Iceberg table has no current metadata file");
            // Hadoop refresh can reuse a cached object after DROP/recreate at the same vN path.
            // Match the existing writer fence by checking the actual current JSON before the CAS.
            checkMetadata(TableMetadataParser.read(delegate.io(), metadataFileLocation));
            checkNotCancelled();
            delegate.commit(base, metadata);
            committedSnapshot = metadata.currentSnapshot();
        }

        private Snapshot getCommittedSnapshot() {
            return committedSnapshot;
        }

        private void checkNotCancelled() {
            if (isCancelled.getAsBoolean()) {
                // ValidationException is a known pre-commit failure: the SDK reclaims the
                // manifests it wrote. Unknown commit outcomes never enter this cleanup path.
                throw new ValidationException("Metadata delete was cancelled before the remote commit");
            }
        }

        private void checkMetadata(TableMetadata metadata) {
            Snapshot snapshot = metadata.currentSnapshot();
            if (snapshot == null || snapshot.snapshotId() != requiredSnapshotId) {
                throw new ValidationException(
                        "Table changed since the metadata delete preflight; retry the statement");
            }
            if (!sameWriteIdentity(expectedMetadata, metadata)) {
                throw new ValidationException(
                        "Iceberg write metadata changed since the metadata delete preflight");
            }
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

        @Override
        public TableOperations temp(TableMetadata metadata) {
            return delegate.temp(metadata);
        }

        @Override
        public long newSnapshotId() {
            return delegate.newSnapshotId();
        }

        @Override
        public boolean requireStrictCleanup() {
            return delegate.requireStrictCleanup();
        }
    }
}
