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
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.AdditionalAnswers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    public void cancellationAfterStagingDoesNotCommit() throws Exception {
        InMemoryCatalog catalog = freshCatalog();
        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = catalog.createTable(TABLE_ID, SCHEMA, spec, props("format-version", "2"));
        table.newAppend().appendFile(dataFile(spec, "s3://b/db1/t1/a.parquet", 10L)).commit();
        long snapshotBefore = table.currentSnapshot().snapshotId();
        AtomicInteger cancellationChecks = new AtomicInteger();

        OptionalLong deleted = IcebergMetadataDelete.executeOnTable(
                table, table, null, Expressions.alwaysTrue(),
                () -> cancellationChecks.incrementAndGet() == 4);

        Assertions.assertFalse(deleted.isPresent());
        Assertions.assertEquals(4, cancellationChecks.get());
        Assertions.assertEquals(snapshotBefore, reload(catalog).currentSnapshot().snapshotId());
        Assertions.assertEquals(1, dataFilePaths(reload(catalog)).size());
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
        return DataFiles.builder(spec)
                .withPath(path)
                .withFileSizeInBytes(1024L)
                .withFormat(FileFormat.PARQUET)
                .withMetrics(new Metrics(records, Collections.emptyMap(), Collections.singletonMap(1, records),
                        Collections.singletonMap(1, 0L), Collections.emptyMap(),
                        Collections.singletonMap(1, Conversions.toByteBuffer(Types.IntegerType.get(), id)),
                        Collections.singletonMap(1, Conversions.toByteBuffer(Types.IntegerType.get(), id))))
                .build();
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
