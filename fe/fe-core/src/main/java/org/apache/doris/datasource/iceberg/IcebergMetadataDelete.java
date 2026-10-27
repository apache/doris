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
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.Transaction;
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
 * <p>The branch-4.2 path is intentionally implemented over the existing Iceberg metadata,
 * transaction, and cache-generation machinery. Candidate files must all be proven to match in
 * full; any partial match, existing delete file, cancellation, or concurrent snapshot change falls
 * back to the unchanged row-level DELETE path.
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
    public static OptionalLong execute(IcebergExternalTable dorisTable, Table targetTable,
            Expression deleteExpression, BooleanSupplier isCancelled) throws UserException {
        IcebergMetadataOps expectedOps = (IcebergMetadataOps) dorisTable.getCatalog().getMetadataOps();
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

        if (!matchesWholeFilesOnly(table, deleteExpression) || isCancelled.getAsBoolean()) {
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
            Transaction transaction = newMetadataDeleteTransaction(
                    table, requiredSnapshotId, ((HasTableOperations) table).operations().current());
            DeleteFiles deleteFiles = transaction.newDelete();
            if (executor != null) {
                deleteFiles = deleteFiles.scanManifestsWith(executor);
            }
            deleteFiles = deleteFiles.deleteFromRowFilter(deleteExpression);
            if (isCancelled.getAsBoolean()) {
                return OptionalLong.empty();
            }
            deleteFiles.commit();
            if (isCancelled.getAsBoolean()) {
                LOG.info("Metadata delete of {} was cancelled before the remote commit.", table.name());
                return OptionalLong.empty();
            }
            transaction.commitTransaction();

            Snapshot committed = transaction.table().currentSnapshot();
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

    private static Transaction newMetadataDeleteTransaction(
            Table table, long requiredSnapshotId, TableMetadata expectedMetadata) {
        Preconditions.checkState(table instanceof HasTableOperations,
                "Iceberg table %s does not expose table operations", table.name());
        TableOperations operations = ((HasTableOperations) table).operations();
        TableOperations validatingOperations =
                new RequiredSnapshotTableOperations(operations, requiredSnapshotId, expectedMetadata);
        BaseTable validatingTable = table instanceof BaseTable
                ? new BaseTable(validatingOperations, table.name(), ((BaseTable) table).reporter())
                : new BaseTable(validatingOperations, table.name());
        return validatingTable.newTransaction();
    }

    /**
     * Every candidate file returned by the same inclusive scan the statement would use must satisfy
     * its strict partition projection or the strict file-metrics evaluator.
     */
    private static boolean matchesWholeFilesOnly(Table table, Expression deleteExpression) {
        boolean caseSensitive = false;
        if (ExpressionUtil.selectsPartitions(deleteExpression, table, caseSensitive)) {
            return true;
        }
        TableScan scan = table.newScan()
                .filter(deleteExpression)
                .caseSensitive(caseSensitive)
                .includeColumnStats()
                .ignoreResiduals();
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
        if (!(expected instanceof HasTableOperations) || !(current instanceof HasTableOperations)) {
            return;
        }
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

        private RequiredSnapshotTableOperations(
                TableOperations delegate, long requiredSnapshotId, TableMetadata expectedMetadata) {
            this.delegate = delegate;
            this.requiredSnapshotId = requiredSnapshotId;
            this.expectedMetadata = expectedMetadata;
        }

        @Override
        public TableMetadata current() {
            return delegate.current();
        }

        @Override
        public TableMetadata refresh() {
            TableMetadata metadata = delegate.refresh();
            checkMetadata(metadata);
            return metadata;
        }

        @Override
        public void commit(TableMetadata base, TableMetadata metadata) {
            TableMetadata refreshed = Preconditions.checkNotNull(
                    delegate.refresh(), "Iceberg table metadata changed before metadata delete");
            checkMetadata(refreshed);
            delegate.commit(base, metadata);
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
