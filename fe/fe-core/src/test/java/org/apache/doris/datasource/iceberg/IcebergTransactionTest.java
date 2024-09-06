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
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergInsertCommandContext;
import org.apache.doris.thrift.TFileContent;
import org.apache.doris.thrift.TIcebergCommitData;

import com.google.common.collect.Maps;
import mockit.Mock;
import mockit.MockUp;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class IcebergTransactionTest {

    private static String dbName = "db3";
    private static String tbWithPartition = "tbWithPartition";
    private static String tbWithoutPartition = "tbWithoutPartition";

    private IcebergExternalCatalog externalCatalog;
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
        this.externalCatalog = new IcebergHMSExternalCatalog(1L, "iceberg", "", Maps.newHashMap(), "");
        new MockUp<IcebergHMSExternalCatalog>() {
            @Mock
            public Catalog getCatalog() {
                return hadoopCatalog;
            }
        };
        ops = new IcebergMetadataOps(externalCatalog, hadoopCatalog);
    }

    private void createTable() throws IOException {
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

        Instant instant = Instant.parse("2024-12-11T12:34:56.123456Z");
        long ts = DateTimeUtil.microsFromInstant(instant);
        int dt = DateTimeUtil.daysFromInstant(instant);

        List<String> partitionValues = new ArrayList<>();

        // reference: org.apache.iceberg.transforms.Timestamps
        partitionValues.add(Integer.valueOf(DateTimeUtil.microsToYears(ts)).toString());
        partitionValues.add(Integer.valueOf(DateTimeUtil.microsToMonths(ts)).toString());
        partitionValues.add("2024-12-11");
        partitionValues.add(Integer.valueOf(DateTimeUtil.microsToHours(ts)).toString());

        // reference: org.apache.iceberg.transforms.Dates
        partitionValues.add(Integer.valueOf(DateTimeUtil.daysToYears(dt)).toString());
        partitionValues.add(Integer.valueOf(DateTimeUtil.daysToMonths(dt)).toString());
        partitionValues.add("2024-12-11");

        // identity dt4
        partitionValues.add("2024-12-11");
        // identity str1
        partitionValues.add("2024-12-11");
        // truncate str2
        partitionValues.add("2024-12-11");
        // bucket int1
        partitionValues.add("1");

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

        new MockUp<IcebergUtils>() {
            @Mock
            public Table getRemoteTable(ExternalCatalog catalog, SimpleTableInfo tableInfo) {
                return table;
            }
        };

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(ctdList);
        SimpleTableInfo tableInfo = new SimpleTableInfo(dbName, tbWithPartition);
        txn.beginInsert(tableInfo);
        txn.finishInsert(tableInfo, Optional.empty());
        txn.commit();

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
        new MockUp<IcebergUtils>() {
            @Mock
            public Table getRemoteTable(ExternalCatalog catalog, SimpleTableInfo tableInfo) {
                return table;
            }
        };

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(ctdList);
        SimpleTableInfo tableInfo = new SimpleTableInfo(dbName, tbWithPartition);
        txn.beginInsert(tableInfo);
        txn.finishInsert(tableInfo, Optional.empty());
        txn.commit();

        checkSnapshotAddProperties(table.currentSnapshot().summary(), "6", "2", "6");
    }

    private IcebergTransaction getTxn() {
        return new IcebergTransaction(ops);
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
        new MockUp<IcebergUtils>() {
            @Mock
            public Table getRemoteTable(ExternalCatalog catalog, SimpleTableInfo tableInfo) {
                return table;
            }
        };

        IcebergTransaction txn = getTxn();
        txn.updateIcebergCommitData(ctdList);
        SimpleTableInfo tableInfo = new SimpleTableInfo(dbName, tbWithPartition);
        txn.beginInsert(tableInfo);
        IcebergInsertCommandContext ctx = new IcebergInsertCommandContext();
        ctx.setOverwrite(true);
        txn.finishInsert(tableInfo, Optional.of(ctx));
        txn.commit();

        checkSnapshotTotalProperties(table.currentSnapshot().summary(), "24", "3", "24");
    }

    @Test
    public void testUnpartitionedTableOverwriteWithoutData() throws UserException {

        testUnPartitionedTableOverwriteWithData();

        Table table = ops.getCatalog().loadTable(TableIdentifier.of(dbName, tbWithoutPartition));
        new MockUp<IcebergUtils>() {
            @Mock
            public Table getRemoteTable(ExternalCatalog catalog, SimpleTableInfo tableInfo) {
                return table;
            }
        };

        IcebergTransaction txn = getTxn();
        SimpleTableInfo tableInfo = new SimpleTableInfo(dbName, tbWithPartition);
        txn.beginInsert(tableInfo);
        IcebergInsertCommandContext ctx = new IcebergInsertCommandContext();
        ctx.setOverwrite(true);
        txn.finishInsert(tableInfo, Optional.of(ctx));
        txn.commit();

        checkSnapshotTotalProperties(table.currentSnapshot().summary(), "0", "0", "0");
    }
}
