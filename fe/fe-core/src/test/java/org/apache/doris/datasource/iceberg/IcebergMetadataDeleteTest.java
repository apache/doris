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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.iceberg.IcebergExternalMetaCache.WritableTableLease;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.TimestampTzLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Pins the branch-4.2 whole-file metadata-delete path and its strict plan extraction.
 */
public class IcebergMetadataDeleteTest {
    private static final TableIdentifier TABLE_ID = TableIdentifier.of("db1", "t1");
    private static final long DORIS_TARGET_ID = 100L;
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "region", Types.StringType.get()));

    @ParameterizedTest
    @ValueSource(strings = {"2", "3"})
    public void wholePartitionDeleteDropsMatchingFilesOnly(String formatVersion) throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", formatVersion));
        table.newAppend()
                .appendFile(partitionedDataFile(
                        spec, "s3://b/db1/t1/region=us/a.parquet", 10L, "region=us"))
                .appendFile(partitionedDataFile(
                        spec, "s3://b/db1/t1/region=us/b.parquet", 10L, "region=us"))
                .appendFile(partitionedDataFile(
                        spec, "s3://b/db1/t1/region=eu/c.parquet", 20L, "region=eu"))
                .commit();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.equal("region", "us"), () -> false);

        Assertions.assertEquals(OptionalLong.of(20L), deleted);
        Assertions.assertEquals(Collections.singletonList("s3://b/db1/t1/region=eu/c.parquet"),
                dataFilePaths(reload(catalog)));
        Assertions.assertEquals(0, liveDeleteFileCount(reload(catalog)));
    }

    @Test
    public void wholePartitionDeleteCoversHistoricalSpecs() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec originalSpec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, originalSpec, props("format-version", "2"));
        table.newAppend()
                .appendFile(partitionedDataFile(
                        originalSpec, "s3://b/db1/t1/old-us.parquet", 10L, "region=us"))
                .appendFile(partitionedDataFile(
                        originalSpec, "s3://b/db1/t1/old-eu.parquet", 20L, "region=eu"))
                .commit();
        table.updateSpec().addField("id").commit();
        table.newAppend().appendFile(partitionedDataFile(
                table.spec(), "s3://b/db1/t1/new-us.parquet", 5L, "region=us/id=1")).commit();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.equal("region", "us"), () -> false);

        Assertions.assertEquals(OptionalLong.of(15L), deleted);
        Assertions.assertEquals(Collections.singletonList("s3://b/db1/t1/old-eu.parquet"),
                dataFilePaths(reload(catalog)));
    }

    @Test
    public void unfilteredDeleteRemovesEveryFile() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L))
                .appendFile(dataFile(spec, "s3://b/db1/t1/b.parquet", 5L))
                .commit();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.alwaysTrue(), () -> false);

        Assertions.assertEquals(OptionalLong.of(15L), deleted);
        Assertions.assertTrue(dataFilePaths(reload(catalog)).isEmpty());
    }

    @Test
    public void partialMatchFallsBackWithoutCommitting() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.greaterThan("id", 5), () -> false);

        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
    }

    @Test
    public void existingDeleteFilesFallBackToTheRowLevelPath() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("region").build();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "3"));
        DataFile dataFile = partitionedDataFile(
                spec, "s3://b/db1/t1/region=us/a.parquet", 10L, "region=us");
        table.newAppend().appendFile(dataFile).commit();
        table.newRowDelta().addDeletes(
                deletionVector(spec, "s3://b/db1/t1/dv.puffin", dataFile, 3L)).commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.equal("region", "us"), () -> false);

        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
        Assertions.assertEquals(1,
                attachedDeleteFileCount(reload(catalog), dataFile.path().toString()));
    }

    @Test
    public void wholeFileMetricsDeleteKeepsNonmatchingFiles() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(dataFileWithId(spec, "s3://b/db1/t1/a.parquet", 10L, 0))
                .appendFile(dataFileWithId(spec, "s3://b/db1/t1/b.parquet", 5L, 2))
                .commit();
        IcebergExternalTable target = targetTable();
        SlotReference id = slot(target, "id");
        Plan plan = new LogicalFilter<>(ImmutableSet.of(new LessThan(new IntegerLiteral(1), id)),
                new TargetScan(target, ImmutableList.of(id)));
        org.apache.iceberg.expressions.Expression predicate =
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID).get();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, predicate, () -> false);

        Assertions.assertEquals(OptionalLong.of(5L), deleted);
        Assertions.assertEquals(Collections.singletonList("s3://b/db1/t1/a.parquet"),
                dataFilePaths(reload(catalog)));
        Assertions.assertEquals(0, liveDeleteFileCount(reload(catalog)));
    }

    @Test
    public void schemaRebindRejectsTheStatementGeneration() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        Table statementTable = reload(catalog);
        statementTable.schema();
        table.updateSchema().renameColumn("id", "renamed_id").commit();

        Assertions.assertThrows(UserException.class, () -> IcebergMetadataDelete.executeOnTable(
                table, statementTable, null, Expressions.alwaysTrue(), () -> false));
        Assertions.assertEquals(1, dataFilePaths(reload(catalog)).size());
    }

    @Test
    public void cancelledDeleteDoesNotCommit() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L))
                .appendFile(dataFile(spec, "s3://b/db1/t1/b.parquet", 5L))
                .commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.alwaysTrue(), () -> true);

        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
        Assertions.assertEquals(2, dataFilePaths(reload(catalog)).size());
    }

    @Test
    public void cancellationAfterStagingCleansUncommittedMetadata() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();
        TrackingFileIO io = new TrackingFileIO(table.io());
        Table tracked = tableWithFileIO(table, io);
        AtomicBoolean cancelled = new AtomicBoolean();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                tracked, table, null, Expressions.alwaysTrue(), () -> {
                    // Deliver cancellation after the SDK has physically written staging metadata,
                    // independently of the number of cancellation checks in the implementation.
                    if (io.hasCreatedFile()) {
                        cancelled.set(true);
                    }
                    return cancelled.get();
                });

        Assertions.assertTrue(cancelled.get());
        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertFalse(io.createdFiles.isEmpty(), "the cancellation must follow real staging");
        Assertions.assertTrue(io.existingCreatedFiles().isEmpty(), "staged metadata must be reclaimed");
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
        Assertions.assertEquals(1, dataFilePaths(reload(catalog)).size());
    }

    @Test
    public void failedStagingCleansCompletedManifestRewrites() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(dataFileWithId(spec, "s3://b/db1/t1/whole.parquet", 10L, 0))
                .appendFile(dataFileWithId(spec, "s3://b/db1/t1/keep.parquet", 5L, 2))
                .commit();
        Table statementTable = IcebergSnapshotCacheValue.retainTableGeneration(table);
        TableOperations realOps = ((HasTableOperations) table).operations();
        TrackingFileIO io = new TrackingFileIO(table.io());
        TableOperations racingOps = operationsWithFileIO(realOps, io);
        AtomicBoolean appended = new AtomicBoolean();
        AtomicBoolean staging = new AtomicBoolean();
        AtomicReference<String> partialManifest = new AtomicReference<>();
        CountDownLatch wholeManifestFinished = new CountDownLatch(1);
        ThreadLocal<Boolean> partialWorker = ThreadLocal.withInitial(() -> false);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                2, 2, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>()) {
            @Override
            protected void beforeExecute(Thread thread, Runnable task) {
                super.beforeExecute(thread, task);
                partialWorker.set(false);
            }

            @Override
            protected void afterExecute(Runnable task, Throwable failure) {
                try {
                    if (staging.get() && !partialWorker.get()) {
                        wholeManifestFinished.countDown();
                    }
                } finally {
                    partialWorker.remove();
                    super.afterExecute(task, failure);
                }
            }
        };
        io.beforeRead = path -> {
            if (staging.get() && path.equals(partialManifest.get())) {
                partialWorker.set(true);
                try {
                    Assertions.assertTrue(wholeManifestFinished.await(30, TimeUnit.SECONDS),
                            "the whole-file manifest worker must finish before partial-file validation");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
            }
        };
        Mockito.doAnswer(invocation -> {
            if (appended.compareAndSet(false, true)) {
                Table current = reload(catalog);
                current.newAppend().appendFile(dataFileWithBounds(spec,
                        "s3://b/db1/t1/partial.parquet", 2L, Types.IntegerType.get(), 0, 2)).commit();
                Snapshot snapshot = current.currentSnapshot();
                partialManifest.set(snapshot.dataManifests(current.io()).stream()
                        .filter(manifest -> manifest.snapshotId() == snapshot.snapshotId())
                        .findFirst().get().path());
                staging.set(true);
            }
            return realOps.refresh();
        }).when(racingOps).refresh();
        Table racingTable = new BaseTable(racingOps, table.name());
        try {
            OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                    racingTable, statementTable, executor, Expressions.lessThan("id", 1), () -> false);

            Assertions.assertTrue(appended.get());
            Assertions.assertFalse(deleted.isPresent());
            Assertions.assertFalse(io.createdFiles.isEmpty(), "a successful worker must have rewritten a manifest");
            Assertions.assertTrue(io.existingCreatedFiles().isEmpty(), "failed staging must reclaim rewrites");
            Assertions.assertEquals(3, dataFilePaths(reload(catalog)).size());
            Assertions.assertEquals("append", reload(catalog).currentSnapshot().operation());
        } finally {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    @Test
    public void hadoopSameVersionRecreationCannotReplaceTheNewTable(@TempDir Path warehouse) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        try (HadoopCatalog catalog = new HadoopCatalog(configuration, warehouse.toUri().toString())) {
            catalog.createNamespace(Namespace.of("db1"));
            PartitionSpec spec = PartitionSpec.unpartitioned();
            Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
            table.newAppend().appendFile(dataFile(spec, warehouse.resolve("old.parquet").toString(), 10L)).commit();
            Table statementTable = IcebergSnapshotCacheValue.retainTableGeneration(table);
            TrackingFileIO io = new TrackingFileIO(table.io());
            Table writableTable = IcebergSnapshotCacheValue.createWritableTable(
                    statementTable, tableWithFileIO(table, io));
            String oldUuid = ((HasTableOperations) table).operations().current().uuid();
            AtomicReference<String> replacementUuid = new AtomicReference<>();
            AtomicBoolean recreated = new AtomicBoolean();
            String replacementPath = warehouse.resolve("replacement.parquet").toString();

            OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                    writableTable, statementTable, null, Expressions.alwaysTrue(), () -> {
                        if (io.hasCreatedFile() && recreated.compareAndSet(false, true)) {
                            // Replacement completes after physical staging and before the final
                            // validation. Both generations end at v2, so SDK refresh alone is stale.
                            Assertions.assertTrue(catalog.dropTable(TABLE_ID, true));
                            Table replacement = catalog.createTable(
                                    TABLE_ID, SCHEMA, spec, props("format-version", "2"));
                            replacement.newAppend().appendFile(dataFile(spec, replacementPath, 5L)).commit();
                            replacementUuid.set(((HasTableOperations) replacement).operations().current().uuid());
                            Assertions.assertNotEquals(oldUuid, replacementUuid.get());
                            Assertions.assertTrue(((HasTableOperations) replacement).operations().current()
                                    .metadataFileLocation().endsWith("v2.metadata.json"));
                        }
                        return false;
                    });

            Assertions.assertTrue(recreated.get());
            Assertions.assertFalse(deleted.isPresent());
            Table replacement = catalog.loadTable(TABLE_ID);
            Assertions.assertEquals(replacementUuid.get(),
                    ((HasTableOperations) replacement).operations().current().uuid());
            Assertions.assertEquals(Collections.singletonList(replacementPath), dataFilePaths(replacement));
        }
    }

    @Test
    public void noMatchAfterADeleteReportsZeroInsteadOfThePreviousSummary() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend()
                .appendFile(dataFileWithId(spec, "s3://b/db1/t1/match.parquet", 10L, 0))
                .appendFile(dataFileWithId(spec, "s3://b/db1/t1/other.parquet", 5L, 2))
                .commit();

        Assertions.assertEquals(OptionalLong.of(10L), IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.equal("id", 0), () -> false));
        Assertions.assertEquals(OptionalLong.of(0L), IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.equal("id", 0), () -> false));
        Assertions.assertEquals(Collections.singletonList("s3://b/db1/t1/other.parquet"),
                dataFilePaths(reload(catalog)));
    }

    @ParameterizedTest
    @CsvSource({"false,comparison", "false,in", "false,between", "true,comparison", "true,in", "true,between"})
    public void temporalPredicatesPreserveSqlFileSelection(boolean zoned, String kind) throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("+08:00");
        context.setThreadLocalInfo();
        try {
            Type fieldType = zoned ? Types.TimestampType.withZone() : Types.TimestampType.withoutZone();
            DataType sqlType = zoned ? TimeStampTzType.of(6) : DateTimeV2Type.of(6);
            Schema schema = new Schema(Types.NestedField.optional(1, "ts", fieldType));
            InMemoryCatalog catalog = freshCatalog();
            Table table = catalog.createTable(
                    TABLE_ID, schema, PartitionSpec.unpartitioned(), props("format-version", "2"));
            IcebergExternalTable target = targetTable();
            SlotReference ts = slot(target, "ts", sqlType);
            String threshold = zoned ? "2026-01-01 08:00:00.000001" : "2026-01-01 00:00:00.500001";
            Literal literal = temporalLiteral(zoned, threshold);
            Expression sqlPredicate;
            String fileInstant;
            switch (kind) {
                case "comparison":
                    sqlPredicate = zoned ? new GreaterThan(ts, literal) : new LessThan(ts, literal);
                    fileInstant = zoned ? "2025-12-31T20:00:00.000001Z" : "2026-01-01T00:00:00.750000Z";
                    break;
                case "in":
                    sqlPredicate = new InPredicate(ts, ImmutableList.of(literal));
                    fileInstant = zoned ? "2025-12-31T16:00:00.000001Z" : "2026-01-01T00:00:01.000001Z";
                    break;
                case "between":
                    Literal lower = temporalLiteral(zoned,
                            zoned ? threshold : "2026-01-01 00:00:00.000001");
                    Literal upper = temporalLiteral(zoned,
                            zoned ? "2026-01-01 08:00:01.000001" : threshold);
                    sqlPredicate = new Between(ts, lower, upper);
                    fileInstant = zoned ? "2025-12-31T16:00:00.500000Z" : "2026-01-01T00:00:00.750000Z";
                    break;
                default:
                    throw new AssertionError(kind);
            }
            String path = "s3://b/db1/t1/temporal.parquet";
            // These values are outside the SQL predicate. Encode file metrics independently
            // from Doris literal conversion so a shared encoding bug cannot make the oracle pass.
            Instant instant = Instant.parse(fileInstant);
            long value = instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
            table.newAppend().appendFile(dataFileWithBounds(table.spec(), path, 3L, fieldType, value, value)).commit();
            org.apache.iceberg.expressions.Expression predicate = exactPredicate(table, target, ts, sqlPredicate);

            Assertions.assertEquals(OptionalLong.of(0L), IcebergMetadataDelete.executeOnTable(
                    table, table, null, predicate, () -> false));
            Assertions.assertEquals(Collections.singletonList(path), dataFilePaths(reload(catalog)));
        } finally {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"lt", "le", "left_lt", "left_le", "or_null"})
    public void nullableRangesKeepSqlNullSemanticsInMixedManifests(String kind) throws Exception {
        Schema schema = new Schema(Types.NestedField.optional(1, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("p").build();
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(TABLE_ID, schema, spec, props("format-version", "2"));
        String nullPath = "s3://b/db1/t1/a-null.parquet";
        String matchPath = "s3://b/db1/t1/b-match.parquet";
        String otherPath = "s3://b/db1/t1/c-other.parquet";
        table.newAppend()
                .appendFile(dataFileInPartition(spec, nullPath, 2L, null))
                .appendFile(dataFileInPartition(spec, matchPath, 3L, 1))
                .appendFile(dataFileInPartition(spec, otherPath, 5L, 3))
                .commit();
        Assertions.assertEquals(1, table.currentSnapshot().dataManifests(table.io()).size(),
                "the NULL and non-NULL partitions must share a manifest");
        IcebergExternalTable target = targetTable();
        SlotReference partition = slot(target, "p", IntegerType.INSTANCE);
        Expression sqlPredicate;
        switch (kind) {
            case "lt":
                sqlPredicate = new LessThan(partition, new IntegerLiteral(2));
                break;
            case "le":
                sqlPredicate = new LessThanEqual(partition, new IntegerLiteral(1));
                break;
            case "left_lt":
                sqlPredicate = new GreaterThan(new IntegerLiteral(2), partition);
                break;
            case "left_le":
                sqlPredicate = new GreaterThanEqual(new IntegerLiteral(1), partition);
                break;
            case "or_null":
                sqlPredicate = new Or(new LessThan(partition, new IntegerLiteral(2)), new IsNull(partition));
                break;
            default:
                throw new AssertionError(kind);
        }
        org.apache.iceberg.expressions.Expression predicate = exactPredicate(table, target, partition, sqlPredicate);

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(table, table, null, predicate, () -> false);

        boolean includesNull = kind.equals("or_null");
        Assertions.assertEquals(OptionalLong.of(includesNull ? 5L : 3L), deleted);
        List<String> expected = includesNull
                ? Collections.singletonList(otherPath) : ImmutableList.of(nullPath, otherPath);
        Assertions.assertEquals(expected, dataFilePaths(reload(catalog)));
    }

    @ParameterizedTest
    @CsvSource({"float,-0.0", "float,0.0", "double,-0.0", "double,0.0"})
    public void floatingValuePredicatesKeepTheRowLevelPath(String kind, String zero) throws Exception {
        Type fieldType;
        DataType sqlType;
        Object value;
        Literal literal;
        if (kind.equals("float")) {
            fieldType = Types.FloatType.get();
            sqlType = FloatType.INSTANCE;
            value = Float.valueOf(zero);
            literal = new FloatLiteral(0.0f);
        } else {
            fieldType = Types.DoubleType.get();
            sqlType = DoubleType.INSTANCE;
            value = Double.valueOf(zero);
            literal = new DoubleLiteral(0.0d);
        }
        Assertions.assertFalse(((Number) value).doubleValue() < 0.0d, "both zero signs are equal in numeric SQL");
        Schema schema = new Schema(Types.NestedField.optional(1, "f", fieldType));
        InMemoryCatalog catalog = freshCatalog();
        Table table = catalog.createTable(
                TABLE_ID, schema, PartitionSpec.unpartitioned(), props("format-version", "2"));
        String path = "s3://b/db1/t1/zero.parquet";
        table.newAppend().appendFile(dataFileWithBounds(table.spec(), path, 2L, fieldType, value, value)).commit();
        IcebergExternalTable target = targetTable();
        SlotReference field = slot(target, "f", sqlType);
        for (Expression predicate : ImmutableList.of(new LessThan(field, literal), new EqualTo(field, literal),
                new InPredicate(field, ImmutableList.of(literal)), new Between(field, literal, literal))) {
            Plan plan = new LogicalFilter<>(
                    ImmutableSet.of(predicate), new TargetScan(target, ImmutableList.of(field)));
            Assertions.assertFalse(IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID).isPresent());
        }
        org.apache.iceberg.expressions.Expression isNull = exactPredicate(table, target, field, new IsNull(field));
        Assertions.assertEquals(OptionalLong.of(0L), IcebergMetadataDelete.executeOnTable(
                table, table, null, isNull, () -> false));
        Assertions.assertEquals(Collections.singletonList(path), dataFilePaths(reload(catalog)));
    }

    @Test
    public void concurrentAppendAtFinalCommitFallsBack() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        TableOperations realOps = ((HasTableOperations) table).operations();
        TableOperations racingOps = Mockito.mock(TableOperations.class, AdditionalAnswers.delegatesTo(realOps));
        AtomicBoolean appended = new AtomicBoolean();
        Mockito.doAnswer(invocation -> {
            if (appended.compareAndSet(false, true)) {
                reload(catalog).newAppend().appendFile(
                        dataFile(spec, "s3://b/db1/t1/concurrent.parquet", 5L)).commit();
            }
            realOps.commit(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(racingOps).commit(Mockito.any(), Mockito.any());
        Table racingTable = new BaseTable(racingOps, table.name());

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                racingTable, table, null, Expressions.alwaysTrue(), () -> false);

        Assertions.assertTrue(appended.get());
        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertEquals(2, dataFilePaths(reload(catalog)).size());
        Assertions.assertEquals("append", reload(catalog).currentSnapshot().operation());
    }

    @Test
    public void unknownCommitOutcomeIsNeverReportedAsFallback() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        TableOperations realOps = ((HasTableOperations) table).operations();
        TableOperations uncertainOps = Mockito.mock(TableOperations.class, AdditionalAnswers.delegatesTo(realOps));
        Mockito.doAnswer(invocation -> {
            realOps.commit(invocation.getArgument(0), invocation.getArgument(1));
            throw new CommitStateUnknownException(new IOException("Commit response was lost"));
        }).when(uncertainOps).commit(Mockito.any(), Mockito.any());
        Table uncertainTable = new BaseTable(uncertainOps, table.name());

        Assertions.assertThrows(CommitStateUnknownException.class,
                () -> IcebergMetadataDelete.executeOnTable(
                        uncertainTable, table, null, Expressions.alwaysTrue(), () -> false));
        Assertions.assertTrue(dataFilePaths(reload(catalog)).isEmpty());
    }

    @Test
    public void affectedRowsUseThisDeletesSnapshot() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        TableOperations realOps = ((HasTableOperations) table).operations();
        TableOperations racingOps = Mockito.mock(TableOperations.class, AdditionalAnswers.delegatesTo(realOps));
        Mockito.doAnswer(invocation -> {
            realOps.commit(invocation.getArgument(0), invocation.getArgument(1));
            reload(catalog).newAppend().appendFile(
                    dataFile(spec, "s3://b/db1/t1/concurrent.parquet", 5L)).commit();
            return null;
        }).when(racingOps).commit(Mockito.any(), Mockito.any());
        Table racingTable = new BaseTable(racingOps, table.name());

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                racingTable, table, null, Expressions.alwaysTrue(), () -> false);

        Assertions.assertEquals(OptionalLong.of(10L), deleted);
        Assertions.assertEquals(Collections.singletonList("s3://b/db1/t1/concurrent.parquet"),
                dataFilePaths(reload(catalog)));
        Assertions.assertEquals("append", reload(catalog).currentSnapshot().operation());
    }

    @Test
    public void v1TableKeepsTheRowLevelFormatVersionContract() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "1"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.alwaysTrue(), () -> false);

        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
    }

    @Test
    public void metadataDeleteUsesPinnedIcebergOpsAndClosesTheLease() throws Exception {
        Table table = createTable();
        IcebergExternalTable dorisTable = targetTable();
        IcebergSnapshotCacheValue snapshot = Mockito.mock(IcebergSnapshotCacheValue.class);
        IcebergRuntimeContext runtimeContext = Mockito.mock(IcebergRuntimeContext.class);
        IcebergMetadataOps pinnedOps = Mockito.mock(IcebergMetadataOps.class);
        WritableTableLease lease = Mockito.mock(WritableTableLease.class);
        Mockito.when(snapshot.getIcebergTable()).thenReturn(Optional.of(table));
        Mockito.when(snapshot.getRuntimeContext()).thenReturn(runtimeContext);
        Mockito.when(runtimeContext.getMetadataOps()).thenReturn(pinnedOps);
        Mockito.when(lease.getTable()).thenReturn(table);
        Mockito.when(lease.getOps()).thenReturn(pinnedOps);
        Mockito.when(lease.getAuthenticator()).thenReturn(new ExecutionAuthenticator() { });

        try (MockedStatic<IcebergUtils> utils = Mockito.mockStatic(IcebergUtils.class)) {
            utils.when(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, pinnedOps)).thenReturn(lease);

            Assertions.assertFalse(IcebergMetadataDelete.execute(
                    dorisTable, snapshot, Expressions.alwaysTrue(), () -> false).isPresent());

            utils.verify(() -> IcebergUtils.acquireWritableIcebergTable(dorisTable, pinnedOps));
        }
        Mockito.verify(lease).close();
        // In HMS catalogs getMetadataOps() is the Hive implementation. The retained snapshot is
        // the authority for Iceberg ops on both catalog routes.
        Mockito.verify(dorisTable, Mockito.never()).getCatalog();
    }

    @Test
    public void exactPredicateIsBuiltFromTheTargetFilter() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        SlotReference id = slot(target, "id");
        Plan plan = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(id, new IntegerLiteral(1))),
                new TargetScan(target, ImmutableList.of(id)));

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(ExpressionUtil.equivalent(
                Expressions.equal("id", 1), result.get(), SCHEMA.asStruct(), false));
    }

    @Test
    public void unfilteredTargetPlanMeansAlwaysTrue() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        SlotReference id = slot(target, "id");
        Plan plan = new TargetScan(target, ImmutableList.of(id));

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(ExpressionUtil.equivalent(
                Expressions.alwaysTrue(), result.get(), SCHEMA.asStruct(), false));
    }

    @Test
    public void literalLeftComparisonIsCommutatedExactly() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        SlotReference id = slot(target, "id");
        Plan plan = new LogicalFilter<>(
                ImmutableSet.of(new LessThan(new IntegerLiteral(1), id)),
                new TargetScan(target, ImmutableList.of(id)));

        Optional<org.apache.iceberg.expressions.Expression> result =
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID);

        Assertions.assertTrue(result.isPresent());
        Assertions.assertTrue(ExpressionUtil.equivalent(
                Expressions.greaterThan("id", 1), result.get(), SCHEMA.asStruct(), false));
    }

    @Test
    public void unsafeNegationDisablesTheFastPath() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        SlotReference id = slot(target, "id");
        Plan plan = new LogicalFilter<>(
                ImmutableSet.of(new Not(new EqualTo(id, new IntegerLiteral(1)))),
                new TargetScan(target, ImmutableList.of(id)));

        Assertions.assertFalse(
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID).isPresent());
    }

    @Test
    public void nullTestOfComputedExpressionDisablesTheWholeFastPath() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        SlotReference id = slot(target, "id");
        Plan plan = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(id, new IntegerLiteral(1)),
                new IsNull(new Add(id, new IntegerLiteral(1)))),
                new TargetScan(target, ImmutableList.of(id)));

        Assertions.assertFalse(
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID).isPresent());
    }

    @Test
    public void crossTableConjunctDisablesTheFastPath() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        TableIf other = Mockito.mock(TableIf.class);
        Mockito.when(other.getId()).thenReturn(DORIS_TARGET_ID + 1);
        SlotReference targetId = slot(target, "id");
        SlotReference otherId = slot(other, "id");
        Plan plan = new LogicalFilter<>(
                ImmutableSet.of(
                        new EqualTo(targetId, new IntegerLiteral(1)),
                        new EqualTo(otherId, new IntegerLiteral(2))),
                new TargetScan(target, ImmutableList.of(targetId, otherId)));

        Assertions.assertFalse(
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID).isPresent());
    }

    @Test
    public void renamedSlotDisablesTheFastPath() {
        Table table = createTable();
        IcebergExternalTable target = targetTable();
        SlotReference physical = slot(target, "region");
        SlotReference renamed = physical.withName("id");
        Plan plan = new LogicalFilter<>(
                ImmutableSet.of(new EqualTo(renamed, new IntegerLiteral(1))),
                new TargetScan(target, ImmutableList.of(renamed)));

        Assertions.assertFalse(
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID).isPresent());
    }

    @Test
    public void nonlinearPlanDisablesTheFastPath() {
        Table table = createTable();
        Plan join = Mockito.mock(LogicalJoin.class);

        Assertions.assertFalse(
                IcebergMetadataDeletePredicate.build(table, join, DORIS_TARGET_ID).isPresent());
    }

    private static IcebergExternalTable targetTable() {
        IcebergExternalTable table = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(table.getId()).thenReturn(DORIS_TARGET_ID);
        return table;
    }

    private static SlotReference slot(TableIf table, String name) {
        Column column = name.equals("id")
                ? new Column(name, org.apache.doris.catalog.ScalarType.INT)
                : new Column(name, org.apache.doris.catalog.ScalarType.STRING);
        return SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, column, ImmutableList.of("t"));
    }

    private static Table createTable() {
        InMemoryCatalog catalog = freshCatalog();
        return catalog.createTable(
                TABLE_ID, SCHEMA, PartitionSpec.unpartitioned(), props("format-version", "2"));
    }

    private static InMemoryCatalog freshCatalog() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("test", Collections.emptyMap());
        catalog.createNamespace(Namespace.of("db1"));
        return catalog;
    }

    private static Table reload(InMemoryCatalog catalog) {
        return catalog.loadTable(TABLE_ID);
    }

    private static Map<String, String> props(String... keyValues) {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }

    private static DataFile dataFile(PartitionSpec spec, String path, long records) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .build();
    }

    private static DataFile partitionedDataFile(
            PartitionSpec spec, String path, long records, String partitionPath) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024)
                .withRecordCount(records)
                .withFormat(FileFormat.PARQUET)
                .withPartitionPath(partitionPath)
                .build();
    }

    private static DataFile dataFileWithId(PartitionSpec spec, String path, long records, int id) {
        return dataFileWithBounds(spec, path, records, Types.IntegerType.get(), id, id);
    }

    private static DataFile dataFileWithBounds(
            PartitionSpec spec, String path, long records, Type type, Object lower, Object upper) {
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024L)
                .withFormat(FileFormat.PARQUET)
                .withMetrics(new Metrics(records, Collections.emptyMap(), Collections.singletonMap(1, records),
                        Collections.singletonMap(1, 0L), Collections.emptyMap(),
                        Collections.singletonMap(1, Conversions.toByteBuffer(type, lower)),
                        Collections.singletonMap(1, Conversions.toByteBuffer(type, upper))))
                .build();
    }

    private static DataFile dataFileInPartition(PartitionSpec spec, String path, long records, Object value) {
        PartitionData partition = new PartitionData(spec.partitionType());
        partition.set(0, value);
        return DataFiles.builder(spec).withPath(path).withFileSizeInBytes(1024L).withRecordCount(records)
                .withFormat(FileFormat.PARQUET).withPartition(partition).build();
    }

    private static Literal temporalLiteral(boolean zoned, String value) {
        return zoned ? TimestampTzLiteral.fromSessionTimeZone(TimeStampTzType.of(6), value)
                : new DateTimeV2Literal(DateTimeV2Type.of(6), value);
    }

    private static SlotReference slot(TableIf table, String name, DataType type) {
        return SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), table,
                new Column(name, type.toCatalogDataType()), ImmutableList.of("t"));
    }

    private static org.apache.iceberg.expressions.Expression exactPredicate(
            Table table, IcebergExternalTable target, SlotReference field, Expression expression) {
        Plan plan = new LogicalFilter<>(ImmutableSet.of(expression), new TargetScan(target, ImmutableList.of(field)));
        Optional<org.apache.iceberg.expressions.Expression> predicate =
                IcebergMetadataDeletePredicate.build(table, plan, DORIS_TARGET_ID);
        Assertions.assertTrue(predicate.isPresent());
        return predicate.get();
    }

    private static Table tableWithFileIO(Table table, FileIO io) {
        return new BaseTable(operationsWithFileIO(((HasTableOperations) table).operations(), io), table.name());
    }

    private static TableOperations operationsWithFileIO(TableOperations delegate, FileIO io) {
        TableOperations operations = Mockito.mock(TableOperations.class, AdditionalAnswers.delegatesTo(delegate));
        Mockito.when(operations.io()).thenReturn(io);
        // A transaction uses temporary operations for its IO. Preserve the same decorated IO
        // there, as the production retained writable operations do, rather than losing the observer.
        Mockito.doAnswer(invocation -> {
            TableMetadata metadata = invocation.getArgument(0);
            TableOperations temporary = Mockito.mock(TableOperations.class,
                    AdditionalAnswers.delegatesTo(delegate.temp(metadata)));
            Mockito.when(temporary.io()).thenReturn(io);
            return temporary;
        }).when(operations).temp(Mockito.any(TableMetadata.class));
        return operations;
    }

    private static final class TrackingFileIO implements FileIO {
        private static final long serialVersionUID = 1L;
        private final FileIO delegate;
        private final List<String> createdFiles = new CopyOnWriteArrayList<>();
        private Consumer<String> beforeRead = path -> { };

        private TrackingFileIO(FileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public InputFile newInputFile(String path) {
            beforeRead.accept(path);
            return delegate.newInputFile(path);
        }

        @Override
        public InputFile newInputFile(String path, long length) {
            beforeRead.accept(path);
            return delegate.newInputFile(path, length);
        }

        @Override
        public OutputFile newOutputFile(String path) {
            createdFiles.add(path);
            return delegate.newOutputFile(path);
        }

        @Override
        public void deleteFile(String path) {
            delegate.deleteFile(path);
        }

        @Override
        public void close() {
            delegate.close();
        }

        private boolean hasCreatedFile() {
            return !existingCreatedFiles().isEmpty();
        }

        private List<String> existingCreatedFiles() {
            List<String> existing = new ArrayList<>();
            for (String path : createdFiles) {
                if (exists(path)) {
                    existing.add(path);
                }
            }
            return existing;
        }

        private boolean exists(String path) {
            try {
                return delegate.newInputFile(path).exists();
            } catch (NotFoundException e) {
                // InMemoryFileIO reports an already deleted object when constructing InputFile;
                // HadoopFileIO instead returns an InputFile whose exists() is false.
                return false;
            }
        }
    }

    private static DeleteFile deletionVector(
            PartitionSpec spec, String puffin, DataFile referencedDataFile, long cardinality) {
        return FileMetadata.deleteFileBuilder(spec)
                .ofPositionDeletes()
                .withPath(puffin)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(128L)
                .withRecordCount(cardinality)
                .withPartition(referencedDataFile.partition())
                .withReferencedDataFile(referencedDataFile.path().toString())
                .withContentOffset(0L)
                .withContentSizeInBytes(64L)
                .build();
    }

    private static List<String> dataFilePaths(Table table) {
        List<String> paths = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().path().toString());
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        Collections.sort(paths);
        return paths;
    }

    private static int liveDeleteFileCount(Table table) {
        Snapshot snapshot = table.currentSnapshot();
        if (snapshot == null) {
            return 0;
        }
        int count = 0;
        for (ManifestFile manifest : snapshot.deleteManifests(table.io())) {
            try (ManifestReader<DeleteFile> reader =
                    ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                count += Iterables.size(reader);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }
        return count;
    }

    private static int attachedDeleteFileCount(Table table, String dataFilePath) {
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                if (task.file().path().toString().equals(dataFilePath)) {
                    return task.deletes().size();
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return -1;
    }

    private static final class TargetScan extends LogicalCatalogRelation {
        private final List<Slot> output;

        private TargetScan(TableIf table, List<? extends Slot> output) {
            super(new RelationId(0), PlanType.LOGICAL_FILE_SCAN, table, ImmutableList.of("db1"));
            this.output = ImmutableList.copyOf(output);
        }

        @Override
        public List<Slot> computeOutput() {
            return output;
        }

        @Override
        public LogicalCatalogRelation withRelationId(RelationId relationId) {
            return this;
        }

        @Override
        public LogicalCatalogRelation withTableAlias(String tableAlias) {
            return this;
        }

        @Override
        public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                Optional<LogicalProperties> logicalProperties, List<Plan> children) {
            return this;
        }

        @Override
        public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
            return this;
        }
    }
}
