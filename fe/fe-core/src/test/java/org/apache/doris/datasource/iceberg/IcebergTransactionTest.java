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
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.NameMapping;
import org.apache.doris.datasource.iceberg.helper.IcebergWriterHelper;
import org.apache.doris.foundation.util.SerializationUtils;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergTransactionTest {

    private static String dbName = "db3";
    private static String tbWithPartition = "tbWithPartition";
    private static String tbWithoutPartition = "tbWithoutPartition";

    private IcebergExternalCatalog spyExternalCatalog;
    private IcebergMetadataOps ops;

    @Before
    public void init() throws IOException {
        createCatalog();
        createTable();
    }

    private void createCatalog() throws IOException {
        Path warehousePath = Files.createTempDirectory("test_warehouse_");
        String warehouse = "file://" + warehousePath.toAbsolutePath() + "/";
        HadoopCatalog hadoopCatalog = new HadoopCatalog();
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        hadoopCatalog.setConf(new Configuration());
        hadoopCatalog.initialize("df", props);
        this.spyExternalCatalog = Mockito.mock(IcebergExternalCatalog.class);
        Mockito.when(spyExternalCatalog.getCatalog()).thenReturn(hadoopCatalog);
        Mockito.when(spyExternalCatalog.getExecutionAuthenticator()).thenReturn(new ExecutionAuthenticator() {
        });
        ops = new IcebergMetadataOps(spyExternalCatalog, hadoopCatalog);
    }

    private void createTable() {
        HadoopCatalog icebergCatalog = (HadoopCatalog) ops.getCatalog();
        icebergCatalog.createNamespace(Namespace.of(dbName));
        Schema schema = new Schema(
                Types.NestedField.required(11, "ts1", Types.TimestampType.withoutZone()),
                Types.NestedField.required(12, "ts2", Types.TimestampType.withoutZone()),
                Types.NestedField.required(13, "ts3", Types.TimestampType.withoutZone()),
                Types.NestedField.required(14, "ts4", Types.TimestampType.withoutZone()),
                Types.NestedField.required(15, "dt1", Types.DateType.get()),
                Types.NestedField.required(16, "dt2", Types.DateType.get()),
                Types.NestedField.required(17, "dt3", Types.DateType.get()),
                Types.NestedField.required(18, "dt4", Types.DateType.get()),
                Types.NestedField.required(19, "str1", Types.StringType.get()),
                Types.NestedField.required(20, "str2", Types.StringType.get()),
                Types.NestedField.required(21, "int1", Types.IntegerType.get()),
                Types.NestedField.required(22, "int2", Types.IntegerType.get())
        );

        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .year("ts1")
                .month("ts2")
                .day("ts3")
                .hour("ts4")
                .year("dt1")
                .month("dt2")
                .day("dt3")
                .identity("dt4")
                .identity("str1")
                .truncate("str2", 10)
                .bucket("int1", 2)
                .build();
        icebergCatalog.createTable(TableIdentifier.of(dbName, tbWithPartition), schema, partitionSpec);
        icebergCatalog.createTable(TableIdentifier.of(dbName, tbWithoutPartition), schema);
    }

    private List<String> createPartitionValues() {
        return createPartitionValues(Instant.parse("2024-12-11T12:34:56.123456Z"),
                "2024-12-11", "2024-12-11", 1);
    }

    private List<String> createPartitionValues(Instant instant, String str1, String str2, Integer int1) {
        long ts = DateTimeUtil.microsFromInstant(instant);
        int dt = DateTimeUtil.daysFromInstant(instant);
        String dateString = numToDay(dt);

        List<String> partitionValues = new ArrayList<>();

        // reference: org.apache.iceberg.transforms.Timestamps
        partitionValues.add(Integer.valueOf(DateTimeUtil.microsToYears(ts)).toString());
        partitionValues.add(Integer.valueOf(DateTimeUtil.microsToMonths(ts)).toString());
        partitionValues.add("2024-12-11");
        partitionValues.add(Integer.valueOf(DateTimeUtil.microsToHours(ts)).toString());

        // reference: org.apache.iceberg.transforms.Dates
        partitionValues.add(Integer.valueOf(DateTimeUtil.daysToYears(dt)).toString());
        partitionValues.add(Integer.valueOf(DateTimeUtil.daysToMonths(dt)).toString());
        partitionValues.add(dateString);

        // identity dt4
        partitionValues.add(dateString);
        // identity str1
        partitionValues.add(str1);
        // truncate str2
        partitionValues.add(str2);
        // bucket int1
        partitionValues.add(int1.toString());

        return partitionValues;
    }

    @Test
    public void testPartitionedTable() throws UserException {
        List<String> partitionValues = createPartitionValues();

        List<TIcebergCommitData> ctdList = new ArrayList<>();
        TIcebergCommitData ctd1 = new TIcebergCommitData();
        ctd1.setFilePath("f1.parquet");
        ctd1.setPartitionValues(partitionValues);
        ctd1.setFileContent(TFileContent.DATA);
        ctd1.setRowCount(2);
        ctd1.setFileSize(2);

        TIcebergCommitData ctd2 = new TIcebergCommitData();
        ctd2.setFilePath("f2.parquet");
        ctd2.setPartitionValues(partitionValues);
        ctd2.setFileContent(TFileContent.DATA);
        ctd2.setRowCount(4);
        ctd2.setFileSize(4);

        ctdList.add(ctd1);
        ctdList.add(ctd2);

        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithPartition));

        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getDbName()).thenReturn(dbName);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithPartition);

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            // Allow parsePartitionValueFromString to call the real implementation
            mockedStatic.when(() -> IcebergUtils.parsePartitionValueFromString(
                    ArgumentMatchers.any(), ArgumentMatchers.any()))
                    .thenCallRealMethod();
            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(ctdList);
            txn.beginInsert(icebergExternalTable, table, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithPartition));
            txn.commit();
        }

        checkSnapshotAddProperties(table.currentSnapshot().summary(), "6", "2", "6");
        checkPushDownByPartitionForTs(table, "ts1");
        checkPushDownByPartitionForTs(table, "ts2");
        checkPushDownByPartitionForTs(table, "ts3");
        checkPushDownByPartitionForTs(table, "ts4");

        checkPushDownByPartitionForDt(table, "dt1");
        checkPushDownByPartitionForDt(table, "dt2");
        checkPushDownByPartitionForDt(table, "dt3");
        checkPushDownByPartitionForDt(table, "dt4");

        checkPushDownByPartitionForString(table, "str1");
        checkPushDownByPartitionForString(table, "str2");

        checkPushDownByPartitionForBucketInt(table, "int1");
    }

    private void checkPushDownByPartitionForBucketInt(Table table, String column) {
        // (BucketUtil.hash(15) & Integer.MAX_VALUE) % 2 = 0
        Integer i1 = 15;

        UnboundPredicate<Integer> lessThan = Expressions.lessThan(column, i1);
        checkPushDownByPartition(table, lessThan, 2);
        // can only filter this case
        UnboundPredicate<Integer> equal = Expressions.equal(column, i1);
        checkPushDownByPartition(table, equal, 0);
        UnboundPredicate<Integer> greaterThan = Expressions.greaterThan(column, i1);
        checkPushDownByPartition(table, greaterThan, 2);

        // (BucketUtil.hash(25) & Integer.MAX_VALUE) % 2 = 1
        Integer i2 = 25;

        UnboundPredicate<Integer> lessThan2 = Expressions.lessThan(column, i2);
        checkPushDownByPartition(table, lessThan2, 2);
        UnboundPredicate<Integer> equal2 = Expressions.equal(column, i2);
        checkPushDownByPartition(table, equal2, 2);
        UnboundPredicate<Integer> greaterThan2 = Expressions.greaterThan(column, i2);
        checkPushDownByPartition(table, greaterThan2, 2);
    }

    private void checkPushDownByPartitionForString(Table table, String column) {
        // Since the string used to create the partition is in date format, the date check can be reused directly
        checkPushDownByPartitionForDt(table, column);
    }

    private void checkPushDownByPartitionForTs(Table table, String column) {
        String lessTs = "2023-12-11T12:34:56.123456";
        String eqTs = "2024-12-11T12:34:56.123456";
        String greaterTs = "2025-12-11T12:34:56.123456";

        UnboundPredicate<String> lessThan = Expressions.lessThan(column, lessTs);
        checkPushDownByPartition(table, lessThan, 0);
        UnboundPredicate<String> equal = Expressions.equal(column, eqTs);
        checkPushDownByPartition(table, equal, 2);
        UnboundPredicate<String> greaterThan = Expressions.greaterThan(column, greaterTs);
        checkPushDownByPartition(table, greaterThan, 0);
    }

    private void checkPushDownByPartitionForDt(Table table, String column) {
        String less = "2023-12-11";
        String eq = "2024-12-11";
        String greater = "2025-12-11";

        UnboundPredicate<String> lessThan = Expressions.lessThan(column, less);
        checkPushDownByPartition(table, lessThan, 0);
        UnboundPredicate<String> equal = Expressions.equal(column, eq);
        checkPushDownByPartition(table, equal, 2);
        UnboundPredicate<String> greaterThan = Expressions.greaterThan(column, greater);
        checkPushDownByPartition(table, greaterThan, 0);
    }

    private void checkPushDownByPartition(Table table, Expression expr, Integer expectFiles) {
        CloseableIterable<FileScanTask> fileScanTasks = table.newScan().filter(expr).planFiles();
        AtomicReference<Integer> cnt = new AtomicReference<>(0);
        fileScanTasks.forEach(notUse -> cnt.updateAndGet(v -> v + 1));
        Assert.assertEquals(expectFiles, cnt.get());
    }

    @Test
    public void testUnPartitionedTable() throws UserException {
        ArrayList<TIcebergCommitData> ctdList = new ArrayList<>();
        TIcebergCommitData ctd1 = new TIcebergCommitData();
        ctd1.setFilePath("f1.parquet");
        ctd1.setFileContent(TFileContent.DATA);
        ctd1.setRowCount(2);
        ctd1.setFileSize(2);

        TIcebergCommitData ctd2 = new TIcebergCommitData();
        ctd2.setFilePath("f2.parquet");
        ctd2.setFileContent(TFileContent.DATA);
        ctd2.setRowCount(4);
        ctd2.setFileSize(4);

        ctdList.add(ctd1);
        ctdList.add(ctd2);

        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getDbName()).thenReturn(dbName);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithoutPartition);

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);

            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(ctdList);
            txn.beginInsert(icebergExternalTable, table, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithPartition));
            txn.commit();
        }

        checkSnapshotAddProperties(table.currentSnapshot().summary(), "6", "2", "6");
    }

    private IcebergTransaction getTxn() {
        return new IcebergTransaction(ops);
    }

    @Test
    public void testSchemaSkewFailsBeforeOpeningInsertOrMergeTransaction() {
        Schema pinnedSchema = new Schema(90,
                Collections.singletonList(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        Schema currentSchema = new Schema(91,
                Collections.singletonList(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(currentSchema);
        Mockito.when(table.properties()).thenReturn(
                Collections.singletonMap(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3"));

        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn("schema_skew_table");
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setWriteSchemaContext(Optional.of(
                IcebergWriteSchemaContext.forSchema(pinnedSchema, 3, true, true)));

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            mockedStatic.when(() -> IcebergUtils.getFormatVersion(table)).thenReturn(3);

            UserException insertException = Assert.assertThrows(UserException.class,
                    () -> getTxn().beginInsert(dorisTable, Optional.of(insertContext)));
            Assert.assertTrue(insertException.getMessage().contains("retry the statement"));

            UserException mergeException = Assert.assertThrows(UserException.class,
                    () -> getTxn().beginMerge(dorisTable, Optional.of(insertContext)));
            Assert.assertTrue(mergeException.getMessage().contains("retry the statement"));
            Mockito.verify(table, Mockito.never()).newTransaction();
            Mockito.verify(table, Mockito.never()).refresh();
        }
    }

    @Test
    public void testRecreatedTableFailsInsertOverwriteAndUpdateMergePreflight() {
        Schema schema = new Schema(92,
                Collections.singletonList(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        UUID pinnedUuid = UUID.fromString("00000000-0000-0000-0000-000000000001");
        IcebergWriteSchemaContext context =
                IcebergWriteSchemaContext.forSchemaWithUuidIdentity(schema, 3, pinnedUuid);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        SortOrder sortOrder = SortOrder.unsorted();
        Table replacementTable = Mockito.mock(Table.class);
        Mockito.when(replacementTable.schema()).thenReturn(schema);
        Mockito.when(replacementTable.uuid()).thenReturn(
                UUID.fromString("00000000-0000-0000-0000-000000000002"));
        Mockito.when(replacementTable.properties()).thenReturn(
                Collections.singletonMap(
                        org.apache.iceberg.TableProperties.FORMAT_VERSION, "3"));
        Mockito.when(replacementTable.specs()).thenReturn(
                Collections.singletonMap(spec.specId(), spec));
        Mockito.when(replacementTable.sortOrders()).thenReturn(
                Collections.singletonMap(sortOrder.orderId(), sortOrder));

        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn("recreated_table");

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(replacementTable);
            mockedStatic.when(() -> IcebergUtils.getFormatVersion(replacementTable))
                    .thenReturn(3);

            IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
            insertContext.setWriteSchemaContext(Optional.of(context));
            UserException insertException = Assert.assertThrows(
                    UserException.class,
                    () -> getTxn().beginInsert(dorisTable, Optional.of(insertContext)));
            Assert.assertTrue(insertException.getMessage().contains("identity changed"));

            IcebergInsertCommandContext overwriteContext = new IcebergInsertCommandContext();
            overwriteContext.setOverwrite(true);
            overwriteContext.setWriteSchemaContext(Optional.of(context));
            UserException overwriteException = Assert.assertThrows(
                    UserException.class,
                    () -> getTxn().beginInsert(dorisTable, Optional.of(overwriteContext)));
            Assert.assertTrue(overwriteException.getMessage().contains("identity changed"));

            IcebergInsertCommandContext mergeContext = new IcebergInsertCommandContext();
            mergeContext.setWriteSchemaContext(Optional.of(context));
            UserException mergeException = Assert.assertThrows(
                    UserException.class,
                    () -> getTxn().beginMerge(dorisTable, Optional.of(mergeContext)));
            Assert.assertTrue(mergeException.getMessage().contains("identity changed"));
            Mockito.verify(replacementTable, Mockito.never()).newTransaction();
        }
    }

    @Test
    public void testStaticOverwriteRejectsConcurrentCurrentSpecReplacement() {
        Schema schema = new Schema(93, Arrays.asList(
                Types.NestedField.required(1, "p", Types.IntegerType.get()),
                Types.NestedField.required(2, "q", Types.IntegerType.get())));
        PartitionSpec pinnedSpec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .identity("p")
                .build();
        PartitionSpec currentSpec = PartitionSpec.builderFor(schema)
                .withSpecId(2)
                .identity("q")
                .build();
        SortOrder sortOrder = SortOrder.unsorted();
        Map<String, String> writerProperties =
                Collections.singletonMap(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3");
        String dataLocation = "file:///tmp/static_overwrite/data";
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 3, pinnedSpec, sortOrder, FileFormat.PARQUET,
                MetricsConfig.getDefault(),
                org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                dataLocation, writerProperties, true, true);
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.properties()).thenReturn(writerProperties);
        Mockito.when(table.specs()).thenReturn(ImmutableMap.of(
                pinnedSpec.specId(), pinnedSpec,
                currentSpec.specId(), currentSpec));
        Mockito.when(table.spec()).thenReturn(currentSpec);
        Mockito.when(table.sortOrders()).thenReturn(
                Collections.singletonMap(sortOrder.orderId(), sortOrder));

        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn("static_overwrite_table");
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setOverwrite(true);
        insertContext.setStaticPartitionValues(Collections.singletonMap("p", "7"));
        insertContext.setWriteSchemaContext(Optional.of(context));

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            mockedStatic.when(() -> IcebergUtils.getFormatVersion(table)).thenReturn(3);
            mockedStatic.when(() -> IcebergUtils.dataLocation(table)).thenReturn(dataLocation);

            UserException exception = Assert.assertThrows(UserException.class,
                    () -> getTxn().beginInsert(dorisTable, Optional.of(insertContext)));
            Assert.assertTrue(exception.getMessage().contains("current partition spec changed"));
            Assert.assertTrue(exception.getMessage().contains("retry the statement"));
            Mockito.verify(table, Mockito.never()).newTransaction();
        }
    }

    @Test
    public void testDynamicOverwriteRejectsPartitionedToUnpartitionedSpecDrift() throws UserException {
        verifyDynamicOverwriteRejectsPartitionedToUnpartitionedSpecDrift(false);
        verifyDynamicOverwriteRejectsPartitionedToUnpartitionedSpecDrift(true);
    }

    private void verifyDynamicOverwriteRejectsPartitionedToUnpartitionedSpecDrift(
            boolean hasOutputFile) throws UserException {
        String tableName = "dynamic_overwrite_drift_" + hasOutputFile;
        Schema schema = new Schema(
                Types.NestedField.required(1, "p", Types.IntegerType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .withSpecId(1)
                .identity("p")
                .build();
        TableIdentifier identifier = TableIdentifier.of(dbName, tableName);
        Table table = ops.getCatalog().createTable(identifier, schema, spec);
        PartitionSpec activeSpec = table.spec();
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 2, activeSpec, table.sortOrder(), FileFormat.PARQUET,
                MetricsConfig.getDefault(),
                org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                table.location() + "/data", table.properties(), true, true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tableName);
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setOverwrite(true);
        insertContext.setWriteSchemaContext(Optional.of(context));

        IcebergTransaction txn = getTxn();
        if (hasOutputFile) {
            TIcebergCommitData commitData = new TIcebergCommitData();
            commitData.setFilePath(table.location() + "/data/output.parquet");
            commitData.setPartitionValues(Collections.singletonList("7"));
            commitData.setPartitionSpecId(activeSpec.specId());
            commitData.setFileContent(TFileContent.DATA);
            commitData.setRowCount(1);
            commitData.setFileSize(1);
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
        }

        try (MockedStatic<IcebergUtils> mockedUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            txn.beginInsert(dorisTable, Optional.of(insertContext));
            if (hasOutputFile) {
                txn.finishInsert(NameMapping.createForTest(dbName, tableName));
            }

            table.updateSpec().removeField("p").commit();
            table.refresh();

            RuntimeException exception;
            if (hasOutputFile) {
                exception = Assert.assertThrows(RuntimeException.class, txn::commit);
            } else {
                exception = Assert.assertThrows(RuntimeException.class,
                        () -> txn.finishInsert(NameMapping.createForTest(dbName, tableName)));
            }
            Assert.assertTrue(exception.getMessage().contains("current partition spec changed"));
            Assert.assertTrue(exception.getMessage().contains("retry the statement"));
            Assert.assertNull(table.currentSnapshot());
        }
    }

    @Test
    public void testCommitReplayRejectsRequiredSchemaChangeAfterStaging() throws UserException {
        String tableName = "commit_replay_schema_drift";
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        Table table = ops.getCatalog().createTable(
                TableIdentifier.of(dbName, tableName), schema);
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 2, table.spec(), table.sortOrder(), FileFormat.PARQUET,
                MetricsConfig.getDefault(),
                org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                table.location() + "/data", table.properties(), true, true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tableName);
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setWriteSchemaContext(Optional.of(context));
        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath(table.location() + "/data/output.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(Collections.singletonList(commitData));
        try (MockedStatic<IcebergUtils> mockedUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            txn.beginInsert(dorisTable, Optional.of(insertContext));
            txn.finishInsert(NameMapping.createForTest(dbName, tableName));

            table.updateSchema()
                    .allowIncompatibleChanges()
                    .addRequiredColumn("required_after_begin", Types.IntegerType.get())
                    .commit();
            table.refresh();

            RuntimeException exception =
                    Assert.assertThrows(RuntimeException.class, txn::commit);
            Assert.assertTrue(exception.getMessage().contains("schema changed during write planning"));
            Assert.assertTrue(exception.getMessage().contains("retry the statement"));
            Assert.assertNull(table.currentSnapshot());
        }
    }

    @Test
    public void testMergeCommitReplayRejectsRequiredSchemaChangeAfterStaging() throws UserException {
        String tableName = "merge_commit_replay_schema_drift";
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        Table table = ops.getCatalog().createTable(
                TableIdentifier.of(dbName, tableName), schema);
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 2, table.spec(), table.sortOrder(), FileFormat.PARQUET,
                MetricsConfig.getDefault(),
                org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                table.location() + "/data", table.properties(), true, true);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tableName);
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setWriteSchemaContext(Optional.of(context));
        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath(table.location() + "/data/output.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        IcebergTransaction txn = getTxn();
        try (MockedStatic<IcebergUtils> mockedUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            txn.beginMerge(dorisTable, Optional.of(insertContext));
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
            txn.finishMerge(NameMapping.createForTest(dbName, tableName));
            // RowDelta is staged in the Iceberg transaction, but is not visible before final commit.
            Assert.assertNull(table.currentSnapshot());

            table.updateSchema()
                    .allowIncompatibleChanges()
                    .addRequiredColumn("required_after_staging", Types.IntegerType.get())
                    .commit();
            table.refresh();

            RuntimeException exception =
                    Assert.assertThrows(RuntimeException.class, txn::commit);
            Assert.assertTrue(exception.getMessage().contains("schema changed during write planning"));
            Assert.assertTrue(exception.getMessage().contains("retry the statement"));
            Assert.assertNull(table.currentSnapshot());
        }
    }

    @Test
    public void testCommitRejectsTableReplacementAfterStaging() throws UserException {
        String tableName = "commit_table_replacement";
        TableIdentifier identifier = TableIdentifier.of(dbName, tableName);
        Schema schema = new Schema(
                Types.NestedField.optional(1, "id", Types.IntegerType.get()));
        Table table = ops.getCatalog().createTable(identifier, schema);
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tableName);
        Mockito.when(dorisTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(dorisTable.getIcebergTable()).thenReturn(table);
        IcebergWriteSchemaContext context =
                IcebergWriteSchemaContext.create(dorisTable, Optional.empty());
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setWriteSchemaContext(Optional.of(context));
        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath(table.location() + "/data/output.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(Collections.singletonList(commitData));
        try (MockedStatic<IcebergUtils> mockedUtils =
                Mockito.mockStatic(IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            txn.beginInsert(dorisTable, Optional.of(insertContext));
            txn.finishInsert(NameMapping.createForTest(dbName, tableName));

            Assert.assertTrue(ops.getCatalog().dropTable(identifier, true));
            Table replacement = ops.getCatalog().createTable(identifier, schema);
            Assert.assertNotEquals(table.uuid(), replacement.uuid());
            Assert.assertThrows(
                    RuntimeException.class, () -> context.validateCurrentSchema(replacement));
            RuntimeException exception =
                    Assert.assertThrows(RuntimeException.class, txn::commit);
            Assert.assertTrue(exception.getMessage().contains("identity changed"));
            Assert.assertNull(replacement.currentSnapshot());
        }
    }

    @Test
    public void testInsertCommitUsesStatementPinnedWriterMetadata() throws UserException {
        Schema schema = new Schema(92,
                Collections.singletonList(Types.NestedField.optional(
                        1, "id", Types.IntegerType.get())));
        PartitionSpec spec = PartitionSpec.unpartitioned();
        SortOrder sortOrder = SortOrder.unsorted();
        Map<String, String> writerProperties =
                Collections.singletonMap(org.apache.iceberg.TableProperties.FORMAT_VERSION, "3");
        String dataLocation = "file:///tmp/pinned/data";
        IcebergWriteSchemaContext context = IcebergWriteSchemaContext.forSchema(
                schema, 3, spec, sortOrder, FileFormat.PARQUET,
                MetricsConfig.getDefault(),
                org.apache.iceberg.TableProperties.PARQUET_COMPRESSION_DEFAULT_SINCE_1_4_0,
                dataLocation, writerProperties, true, true);
        Table table = Mockito.mock(Table.class);
        org.apache.iceberg.Transaction icebergTxn = Mockito.mock(org.apache.iceberg.Transaction.class);
        AppendFiles appendFiles = Mockito.mock(AppendFiles.class, Mockito.RETURNS_SELF);
        DataFile dataFile = Mockito.mock(DataFile.class);
        Mockito.when(table.schema()).thenReturn(schema);
        Mockito.when(table.properties()).thenReturn(writerProperties);
        Mockito.when(table.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
        Mockito.when(table.sortOrders()).thenReturn(
                Collections.singletonMap(sortOrder.orderId(), sortOrder));
        Mockito.when(table.newTransaction()).thenReturn(icebergTxn);
        Mockito.when(icebergTxn.table()).thenReturn(table);
        Mockito.when(icebergTxn.newAppend()).thenReturn(appendFiles);
        Mockito.when(appendFiles.scanManifestsWith(ArgumentMatchers.any()))
                .thenReturn(appendFiles);

        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn("pinned_writer_table");
        IcebergInsertCommandContext insertContext = new IcebergInsertCommandContext();
        insertContext.setWriteSchemaContext(Optional.of(context));
        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("file:///tmp/pinned/data.parquet");
        commitData.setRowCount(1);
        commitData.setFileSize(128);
        WriteResult writeResult = WriteResult.builder().addDataFiles(dataFile).build();

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(IcebergUtils.class);
                MockedStatic<IcebergWriterHelper> mockedWriterHelper =
                        Mockito.mockStatic(IcebergWriterHelper.class)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                            ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            mockedUtils.when(() -> IcebergUtils.getFormatVersion(table)).thenReturn(3);
            mockedUtils.when(() -> IcebergUtils.dataLocation(table)).thenReturn(dataLocation);
            mockedWriterHelper.when(() -> IcebergWriterHelper.convertToWriterResult(
                            ArgumentMatchers.same(context), ArgumentMatchers.anyList()))
                    .thenReturn(writeResult);

            IcebergTransaction txn = Mockito.spy(getTxn());
            Mockito.doReturn(icebergTxn).when(txn).newWriteTransaction();
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
            txn.beginInsert(dorisTable, Optional.of(insertContext));
            txn.finishInsert(NameMapping.createForTest(dbName, "pinned_writer_table"));

            mockedWriterHelper.verify(() -> IcebergWriterHelper.convertToWriterResult(
                    ArgumentMatchers.same(context), ArgumentMatchers.anyList()));
            mockedWriterHelper.verify(() -> IcebergWriterHelper.convertToWriterResult(
                    ArgumentMatchers.any(Table.class), ArgumentMatchers.anyList()), Mockito.never());
        }
        Mockito.verify(appendFiles).appendFile(dataFile);
        Mockito.verify(appendFiles).commit();
    }

    private void checkSnapshotAddProperties(Map<String, String> props,
                                            String addRecords,
                                            String addFileCnt,
                                            String addFileSize) {
        Assert.assertEquals(addRecords, props.get("added-records"));
        Assert.assertEquals(addFileCnt, props.get("added-data-files"));
        Assert.assertEquals(addFileSize, props.get("added-files-size"));
    }

    private void checkSnapshotTotalProperties(Map<String, String> props,
                                              String totalRecords,
                                              String totalFileCnt,
                                              String totalFileSize) {
        Assert.assertEquals(totalRecords, props.get("total-records"));
        Assert.assertEquals(totalFileCnt, props.get("total-data-files"));
        Assert.assertEquals(totalFileSize, props.get("total-files-size"));
    }

    private String numToYear(Integer num) {
        Transform<Object, Integer> year = Transforms.year();
        return year.toHumanString(Types.IntegerType.get(), num);
    }

    private String numToMonth(Integer num) {
        Transform<Object, Integer> month = Transforms.month();
        return month.toHumanString(Types.IntegerType.get(), num);
    }

    private String numToDay(Integer num) {
        Transform<Object, Integer> day = Transforms.day();
        return day.toHumanString(Types.IntegerType.get(), num);
    }

    private String numToHour(Integer num) {
        Transform<Object, Integer> hour = Transforms.hour();
        return hour.toHumanString(Types.IntegerType.get(), num);
    }

    @Test
    public void tableCloneTest() {
        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        Table cloneTable = (Table) SerializationUtils.clone((Serializable) table);
        Assert.assertNotNull(cloneTable);
    }

    @Test
    public void testTransform() {
        Instant instant = Instant.parse("2024-12-11T12:34:56.123456Z");
        long ts = DateTimeUtil.microsFromInstant(instant);
        Assert.assertEquals("2024", numToYear(DateTimeUtil.microsToYears(ts)));
        Assert.assertEquals("2024-12", numToMonth(DateTimeUtil.microsToMonths(ts)));
        Assert.assertEquals("2024-12-11", numToDay(DateTimeUtil.microsToDays(ts)));
        Assert.assertEquals("2024-12-11-12", numToHour(DateTimeUtil.microsToHours(ts)));

        int dt = DateTimeUtil.daysFromInstant(instant);
        Assert.assertEquals("2024", numToYear(DateTimeUtil.daysToYears(dt)));
        Assert.assertEquals("2024-12", numToMonth(DateTimeUtil.daysToMonths(dt)));
        Assert.assertEquals("2024-12-11", numToDay(dt));
    }

    @Test
    public void testUnPartitionedTableOverwriteWithData() throws UserException {

        testUnPartitionedTable();

        ArrayList<TIcebergCommitData> ctdList = new ArrayList<>();
        TIcebergCommitData ctd1 = new TIcebergCommitData();
        ctd1.setFilePath("f3.parquet");
        ctd1.setFileContent(TFileContent.DATA);
        ctd1.setRowCount(6);
        ctd1.setFileSize(6);

        TIcebergCommitData ctd2 = new TIcebergCommitData();
        ctd2.setFilePath("f4.parquet");
        ctd2.setFileContent(TFileContent.DATA);
        ctd2.setRowCount(8);
        ctd2.setFileSize(8);

        TIcebergCommitData ctd3 = new TIcebergCommitData();
        ctd3.setFilePath("f5.parquet");
        ctd3.setFileContent(TFileContent.DATA);
        ctd3.setRowCount(10);
        ctd3.setFileSize(10);

        ctdList.add(ctd1);
        ctdList.add(ctd2);
        ctdList.add(ctd3);

        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getDbName()).thenReturn(dbName);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithoutPartition);
        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);

            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(ctdList);
            IcebergInsertCommandContext ctx = new IcebergInsertCommandContext();
            txn.beginInsert(icebergExternalTable, table, Optional.of(ctx));
            ctx.setOverwrite(true);
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithPartition));
            txn.commit();
        }

        checkSnapshotTotalProperties(table.currentSnapshot().summary(), "24", "3", "24");
    }

    @Test
    public void testUnpartitionedTableOverwriteWithoutData() throws UserException {

        testUnPartitionedTableOverwriteWithData();

        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getDbName()).thenReturn(dbName);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithoutPartition);
        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);

            IcebergTransaction txn = getTxn();
            IcebergInsertCommandContext ctx = new IcebergInsertCommandContext();
            txn.beginInsert(icebergExternalTable, table, Optional.of(ctx));
            ctx.setOverwrite(true);
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithPartition));
            txn.commit();
        }

        checkSnapshotTotalProperties(table.currentSnapshot().summary(), "0", "0", "0");
    }

    @Test
    public void testStaticPartitionOverwriteWithoutDataDeletesMatchingPartition() throws UserException {
        List<TIcebergCommitData> ctdList = new ArrayList<>();
        TIcebergCommitData ctd1 = new TIcebergCommitData();
        ctd1.setFilePath("partition-a.parquet");
        ctd1.setPartitionValues(createPartitionValues(
                Instant.parse("2024-12-11T12:34:56.123456Z"),
                "partition-a", "truncate-a", 11));
        ctd1.setFileContent(TFileContent.DATA);
        ctd1.setRowCount(2);
        ctd1.setFileSize(2);

        TIcebergCommitData ctd2 = new TIcebergCommitData();
        ctd2.setFilePath("partition-b.parquet");
        ctd2.setPartitionValues(createPartitionValues(
                Instant.parse("2024-12-12T12:34:56.123456Z"),
                "partition-b", "truncate-b", 25));
        ctd2.setFileContent(TFileContent.DATA);
        ctd2.setRowCount(4);
        ctd2.setFileSize(4);

        ctdList.add(ctd1);
        ctdList.add(ctd2);

        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithPartition));
        IcebergWriteSchemaContext writeSchemaContext = IcebergWriteSchemaContext.forSchema(
                table.schema(), IcebergUtils.getFormatVersion(table), table.spec(), table.sortOrder(),
                IcebergUtils.getFileFormat(table), MetricsConfig.forTable(table),
                IcebergUtils.getFileCompress(table), IcebergUtils.dataLocation(table), table.properties(),
                true, true);
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getDbName()).thenReturn(dbName);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithPartition);

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            mockedStatic.when(() -> IcebergUtils.parsePartitionValueFromString(
                    ArgumentMatchers.any(), ArgumentMatchers.any()))
                    .thenCallRealMethod();
            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(ctdList);
            txn.beginInsert(icebergExternalTable, table, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithPartition));
            txn.commit();
        }

        checkPushDownByPartition(table, Expressions.equal("str1", "partition-a"), 1);
        checkPushDownByPartition(table, Expressions.equal("str1", "partition-b"), 1);

        try (MockedStatic<IcebergUtils> mockedStatic = Mockito.mockStatic(IcebergUtils.class)) {
            mockedStatic.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(table);
            mockedStatic.when(() -> IcebergUtils.parsePartitionValueFromString(
                    ArgumentMatchers.any(), ArgumentMatchers.any()))
                    .thenCallRealMethod();
            mockedStatic.when(() -> IcebergUtils.getFormatVersion(
                            ArgumentMatchers.any(Table.class)))
                    .thenReturn(writeSchemaContext.getFormatVersion());
            mockedStatic.when(() -> IcebergUtils.dataLocation(
                            ArgumentMatchers.any(Table.class)))
                    .thenReturn(writeSchemaContext.getDataLocation());

            IcebergTransaction txn = getTxn();
            IcebergInsertCommandContext ctx = new IcebergInsertCommandContext();
            ctx.setOverwrite(true);
            Map<String, String> staticPartitions = new LinkedHashMap<>();
            staticPartitions.put("dt4", "2024-12-11");
            staticPartitions.put("str1", "partition-a");
            ctx.setStaticPartitionValues(staticPartitions);
            ctx.setWriteSchemaContext(Optional.of(writeSchemaContext));
            txn.beginInsert(icebergExternalTable, table, Optional.of(ctx));
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithPartition));
            txn.commit();
        }

        checkPushDownByPartition(table, Expressions.equal("str1", "partition-a"), 0);
        checkPushDownByPartition(table, Expressions.equal("str1", "partition-b"), 1);
        checkSnapshotTotalProperties(table.currentSnapshot().summary(), "4", "1", "4");
    }

    @Test
    public void testFinishDeleteDoesNotRewritePreviousDeleteFilesForV2() throws UserException {
        verifyFinishDeleteRewriteBehavior(2, false);
    }

    @Test
    public void testFinishDeleteRewritesAllSharedPuffinDeleteFilesForV3() throws UserException {
        String referencedDataFile = "s3a://warehouse/wh/db3/tbWithoutPartition/data/data-file.parquet";

        Table icebergTable = Mockito.mock(Table.class);
        org.apache.iceberg.Transaction icebergTxn = Mockito.mock(org.apache.iceberg.Transaction.class);
        RowDelta rowDelta = Mockito.mock(RowDelta.class, Mockito.RETURNS_SELF);
        DeleteFile newDeleteFile = Mockito.mock(DeleteFile.class);
        DeleteFile oldDeleteFile1 = buildDeletionVectorDeleteFile(
                "s3a://warehouse/wh/db3/tbWithoutPartition/data/delete-shared.puffin",
                referencedDataFile, 4L, 21L);
        DeleteFile oldDeleteFile2 = buildDeletionVectorDeleteFile(
                "s3a://warehouse/wh/db3/tbWithoutPartition/data/delete-shared.puffin",
                referencedDataFile, 25L, 19L);
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Mockito.when(icebergTable.newTransaction()).thenReturn(icebergTxn);
        Mockito.when(icebergTable.currentSnapshot()).thenReturn(null);
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
        Mockito.when(icebergTable.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(icebergTable.name()).thenReturn(tbWithoutPartition);
        Mockito.when(icebergTxn.table()).thenReturn(icebergTable);
        Mockito.when(icebergTxn.newRowDelta()).thenReturn(rowDelta);
        Mockito.when(newDeleteFile.path()).thenReturn("s3a://warehouse/wh/db3/tbWithoutPartition/data/delete-new.puffin");

        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithoutPartition);

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("delete-dv-shared.puffin");
        commitData.setFileContent(TFileContent.POSITION_DELETES);
        commitData.setRowCount(3);
        commitData.setFileSize(44);
        commitData.setContentOffset(4);
        commitData.setContentSizeInBytes(21);
        commitData.setReferencedDataFilePath(referencedDataFile);

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(Collections.singletonList(commitData));

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(IcebergUtils.class);
                MockedStatic<IcebergWriterHelper> mockedWriterHelper =
                        Mockito.mockStatic(IcebergWriterHelper.class)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(icebergTable);
            mockedUtils.when(() -> IcebergUtils.getFileFormat(icebergTable)).thenReturn(FileFormat.PARQUET);
            mockedUtils.when(() -> IcebergUtils.getFormatVersion(icebergTable)).thenReturn(3);
            mockedWriterHelper.when(() -> IcebergWriterHelper.convertToDeleteFiles(
                            ArgumentMatchers.any(FileFormat.class),
                            ArgumentMatchers.eq(spec),
                            ArgumentMatchers.anyList()))
                    .thenReturn(Collections.singletonList(newDeleteFile));

            txn.beginDelete(icebergExternalTable, icebergTable);
            txn.setRewrittenDeleteFilesByReferencedDataFile(
                    Collections.singletonMap(referencedDataFile, Arrays.asList(oldDeleteFile1, oldDeleteFile2)));
            txn.finishDelete(NameMapping.createForTest(dbName, tbWithoutPartition));
        }

        Mockito.verify(rowDelta).addDeletes(newDeleteFile);
        Mockito.verify(rowDelta).removeDeletes(oldDeleteFile1);
        Mockito.verify(rowDelta).removeDeletes(oldDeleteFile2);
        Mockito.verify(rowDelta).commit();
    }

    private void verifyFinishDeleteRewriteBehavior(int formatVersion, boolean expectRewrite)
            throws UserException {
        String referencedDataFile = "s3a://warehouse/wh/db3/tbWithoutPartition/data/data-file.parquet";

        Table icebergTable = Mockito.mock(Table.class);
        org.apache.iceberg.Transaction icebergTxn = Mockito.mock(org.apache.iceberg.Transaction.class);
        RowDelta rowDelta = Mockito.mock(RowDelta.class, Mockito.RETURNS_SELF);
        DeleteFile newDeleteFile = Mockito.mock(DeleteFile.class);
        DeleteFile oldDeleteFile = Mockito.mock(DeleteFile.class);
        IcebergExternalTable icebergExternalTable = Mockito.mock(IcebergExternalTable.class);

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Mockito.when(icebergTable.newTransaction()).thenReturn(icebergTxn);
        Mockito.when(icebergTable.currentSnapshot()).thenReturn(null);
        Mockito.when(icebergTable.spec()).thenReturn(spec);
        Mockito.when(icebergTable.specs()).thenReturn(Collections.singletonMap(spec.specId(), spec));
        Mockito.when(icebergTable.properties()).thenReturn(Collections.emptyMap());
        Mockito.when(icebergTable.name()).thenReturn(tbWithoutPartition);
        Mockito.when(icebergTxn.table()).thenReturn(icebergTable);
        Mockito.when(icebergTxn.newRowDelta()).thenReturn(rowDelta);
        Mockito.when(newDeleteFile.path()).thenReturn("s3a://warehouse/wh/db3/tbWithoutPartition/data/delete-new.puffin");
        Mockito.when(oldDeleteFile.path()).thenReturn("s3a://warehouse/wh/db3/tbWithoutPartition/data/delete-old.parquet");

        Mockito.when(icebergExternalTable.getCatalog()).thenReturn(spyExternalCatalog);
        Mockito.when(icebergExternalTable.getName()).thenReturn(tbWithoutPartition);

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("delete-dv.puffin");
        commitData.setFileContent(TFileContent.POSITION_DELETES);
        commitData.setRowCount(3);
        commitData.setFileSize(33);
        commitData.setReferencedDataFilePath(referencedDataFile);

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(Collections.singletonList(commitData));

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(IcebergUtils.class);
                MockedStatic<IcebergWriterHelper> mockedWriterHelper =
                        Mockito.mockStatic(IcebergWriterHelper.class)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(ArgumentMatchers.any(ExternalTable.class)))
                    .thenReturn(icebergTable);
            mockedUtils.when(() -> IcebergUtils.getFileFormat(icebergTable)).thenReturn(FileFormat.PARQUET);
            mockedUtils.when(() -> IcebergUtils.getFormatVersion(icebergTable)).thenReturn(formatVersion);
            mockedWriterHelper.when(() -> IcebergWriterHelper.convertToDeleteFiles(
                            ArgumentMatchers.any(FileFormat.class),
                            ArgumentMatchers.eq(spec),
                            ArgumentMatchers.anyList()))
                    .thenReturn(Collections.singletonList(newDeleteFile));

            txn.beginDelete(icebergExternalTable, icebergTable);
            txn.setRewrittenDeleteFilesByReferencedDataFile(
                    Collections.singletonMap(referencedDataFile, Collections.singletonList(oldDeleteFile)));
            txn.finishDelete(NameMapping.createForTest(dbName, tbWithoutPartition));
        }

        Mockito.verify(rowDelta).addDeletes(newDeleteFile);
        if (expectRewrite) {
            Mockito.verify(rowDelta).removeDeletes(oldDeleteFile);
        } else {
            Mockito.verify(rowDelta, Mockito.never()).removeDeletes(ArgumentMatchers.any(DeleteFile.class));
        }
        Mockito.verify(rowDelta).commit();
    }

    @Test
    public void testBeginInsertUsesRetainedTargetTable() throws UserException {
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn("retained_target");
        Table retainedTable = Mockito.mock(Table.class);
        org.apache.iceberg.Transaction retainedTransaction =
                Mockito.mock(org.apache.iceberg.Transaction.class);
        Mockito.when(retainedTable.newTransaction()).thenReturn(retainedTransaction);

        IcebergTransaction txn = getTxn();
        txn.beginInsert(dorisTable, retainedTable, Optional.empty());

        Mockito.verify(retainedTable).newTransaction();
    }

    @Test
    public void testBeginDeleteUsesRetainedTargetTable() throws UserException {
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn("retained_delete_target");
        Table retainedTable = Mockito.mock(Table.class);
        org.apache.iceberg.Transaction retainedTransaction =
                Mockito.mock(org.apache.iceberg.Transaction.class);
        Mockito.when(retainedTable.newTransaction()).thenReturn(retainedTransaction);

        IcebergTransaction txn = getTxn();
        txn.beginDelete(dorisTable, retainedTable);

        Mockito.verify(retainedTable).newTransaction();
    }

    @Test
    public void testQueryScopedGenerationCommitsThroughWritableOperations() throws UserException {
        // A weight-bounded snapshot cache hands query-scoped (read-only) tables to the sink;
        // commits must still be re-based onto the live table operations.
        Table liveTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergTableCacheValue tableValue = new IcebergTableCacheValue(liveTable);
        tableValue.prepareForCachePublication(NameMapping.createForTest(dbName, tbWithoutPartition));
        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                Mockito.mock(IcebergPartitionInfo.class), Mockito.mock(IcebergSnapshot.class),
                Optional.empty(), tableValue.getRetainedIcebergTable(),
                tableValue.getRetainedCurrentSnapshotJson());
        Table queryScopedTable = cacheValue.getIcebergTable().get();
        Assert.assertFalse(IcebergSnapshotCacheValue.isFrozenGeneration(queryScopedTable));
        Assert.assertTrue(IcebergSnapshotCacheValue.isRetainedGeneration(queryScopedTable));
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tbWithoutPartition);

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("query-scoped-generation.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(
                IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                    Mockito.eq(dorisTable), ArgumentMatchers.any())).thenReturn(liveTable);
            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
            txn.beginInsert(dorisTable, queryScopedTable, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithoutPartition));
            txn.commit();
        }

        Assert.assertNotNull(ops.getCatalog().loadTable(
                TableIdentifier.of(dbName, tbWithoutPartition)).currentSnapshot());
    }

    @Test
    public void testRetainedGenerationCommitsThroughWritableOperations() throws UserException {
        Table liveTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                Mockito.mock(IcebergPartitionInfo.class), Mockito.mock(IcebergSnapshot.class),
                Optional.empty(), liveTable);
        Table retainedTable = cacheValue.getIcebergTable().get();
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tbWithoutPartition);

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("retained-generation.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(
                IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                    Mockito.eq(dorisTable), ArgumentMatchers.any())).thenReturn(liveTable);
            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
            txn.beginInsert(dorisTable, retainedTable, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithoutPartition));
            txn.commit();
        }

        Assert.assertNotNull(ops.getCatalog().loadTable(
                TableIdentifier.of(dbName, tbWithoutPartition)).currentSnapshot());
    }

    @Test
    public void testWeightedTableSupportsSchemaAndPartitionSpecCommits() {
        Table liveTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergTableCacheValue cacheValue = new IcebergTableCacheValue(liveTable);
        cacheValue.prepareForCachePublication(NameMapping.createForTest(dbName, tbWithoutPartition));

        Table writableTable = cacheValue.getWritableIcebergTable(liveTable);
        writableTable.updateSchema()
                .addColumn("new_col", Types.StringType.get())
                .commit();
        writableTable.updateSpec()
                .addField("int1")
                .commit();

        Table refreshed = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        Assert.assertNotNull(refreshed.schema().findField("new_col"));
        Assert.assertEquals(1, refreshed.spec().fields().size());
        Assert.assertEquals("int1", refreshed.spec().fields().get(0).name());
    }

    @Test
    public void testRetainedGenerationRejectsConcurrentMetadataAdvance() throws UserException {
        Table liveTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                Mockito.mock(IcebergPartitionInfo.class), Mockito.mock(IcebergSnapshot.class),
                Optional.empty(), liveTable);
        Table retainedTable = cacheValue.getIcebergTable().get();
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tbWithoutPartition);

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("stale-retained-generation.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(
                IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                    Mockito.eq(dorisTable), ArgumentMatchers.any())).thenReturn(liveTable);
            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
            txn.beginInsert(dorisTable, retainedTable, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithoutPartition));

            liveTable.updateProperties().set("concurrent-update", "true").commit();
            Assert.assertThrows(CommitFailedException.class, txn::commit);
        }
    }

    @Test
    public void testRetainedGenerationRetriesAfterConcurrentDataCommit() throws UserException, IOException {
        Table liveTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                Mockito.mock(IcebergPartitionInfo.class), Mockito.mock(IcebergSnapshot.class),
                Optional.empty(), liveTable);
        Table retainedTable = cacheValue.getIcebergTable().get();
        IcebergExternalTable dorisTable = Mockito.mock(IcebergExternalTable.class);
        Mockito.when(dorisTable.getName()).thenReturn(tbWithoutPartition);

        TIcebergCommitData commitData = new TIcebergCommitData();
        commitData.setFilePath("retry-after-concurrent-data-commit.parquet");
        commitData.setFileContent(TFileContent.DATA);
        commitData.setRowCount(1);
        commitData.setFileSize(1);

        try (MockedStatic<IcebergUtils> mockedUtils = Mockito.mockStatic(
                IcebergUtils.class, Mockito.CALLS_REAL_METHODS)) {
            mockedUtils.when(() -> IcebergUtils.getWritableIcebergTable(
                    Mockito.eq(dorisTable), ArgumentMatchers.any())).thenReturn(liveTable);
            IcebergTransaction txn = getTxn();
            txn.updateIcebergCommitData(Collections.singletonList(commitData));
            txn.beginInsert(dorisTable, retainedTable, Optional.empty());
            txn.finishInsert(NameMapping.createForTest(dbName, tbWithoutPartition));

            Path concurrentFile = Files.createTempFile("concurrent-data-commit-", ".parquet");
            liveTable.newFastAppend()
                    .appendFile(DataFiles.builder(liveTable.spec())
                            .withPath(concurrentFile.toString())
                            .withFileSizeInBytes(1)
                            .withRecordCount(1)
                            .withFormat(FileFormat.PARQUET)
                            .build())
                    .commit();
            txn.commit();
        }

        Table refreshedTable = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        Assert.assertEquals(2, refreshedTable.history().size());
    }

    @Test
    public void testRetainedGenerationRefusesRetryAgainstRecreatedTable() {
        HadoopCatalog icebergCatalog = (HadoopCatalog) ops.getCatalog();
        TableIdentifier identifier = TableIdentifier.of(dbName, tbWithoutPartition);
        Table originalTable = icebergCatalog.loadTable(identifier);
        IcebergSnapshotCacheValue cacheValue = new IcebergSnapshotCacheValue(
                Mockito.mock(IcebergPartitionInfo.class), Mockito.mock(IcebergSnapshot.class),
                Optional.empty(), originalTable);
        Table retainedTable = cacheValue.getIcebergTable().get();
        String retainedUuid = ((HasTableOperations) retainedTable).operations().current().uuid();

        // Drop and recreate at the same location: schema, spec and sort-order ids restart, and
        // the writer contract looks identical except for the table UUID.
        icebergCatalog.dropTable(identifier, true);
        Table recreatedTable = icebergCatalog.createTable(identifier, originalTable.schema());
        Assert.assertNotEquals(retainedUuid,
                ((HasTableOperations) recreatedTable).operations().current().uuid());

        Table writableTable = IcebergSnapshotCacheValue.createWritableTable(retainedTable, recreatedTable);
        CommitFailedException failure = Assert.assertThrows(CommitFailedException.class,
                () -> ((HasTableOperations) writableTable).operations().refresh());
        Assert.assertTrue(failure.getMessage(), failure.getMessage().contains("table UUID"));
    }

    @Test
    public void testStaticPartitionFilterRejectsUnknownKey() {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.required(2, "day", Types.StringType.get()));
        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("day").build();

        Assert.assertThrows(IllegalArgumentException.class,
                () -> getTxn().buildPartitionFilter(
                        Collections.singletonMap("unknown", "2026-01-01"), spec, schema));
    }

    private DeleteFile buildDeletionVectorDeleteFile(String puffinPath, String referencedDataFile,
            long contentOffset, long contentLength) {
        return FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofPositionDeletes()
                .withPath(puffinPath)
                .withFormat(FileFormat.PUFFIN)
                .withFileSizeInBytes(128)
                .withRecordCount(2)
                .withContentOffset(contentOffset)
                .withContentSizeInBytes(contentLength)
                .withReferencedDataFile(referencedDataFile)
                .build();
    }
}
