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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.datasource.TestHMSCachedClient;
import org.apache.doris.fs.LocalDfsFileSystem;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUpdateMode;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class HmsCommitTest {

    private static HiveMetadataOps hmsOps;
    private static HMSCachedClient hmsClient;
    private static final String dbName = "test_db";
    private static final String tbWithPartition = "test_tb_with_partition";
    private static final String tbWithoutPartition = "test_tb_without_partition";
    private static LocalDfsFileSystem fs;
    static String dbLocation;
    static String writeLocation;
    static String uri = "thrift://127.0.0.1:9083";
    static boolean hasRealHmsService = false;

    @BeforeClass
    public static void beforeClass() throws Throwable {
        Path warehousePath = Files.createTempDirectory("test_warehouse_");
        Path writePath = Files.createTempDirectory("test_write_");
        dbLocation = "file://" + warehousePath.toAbsolutePath() + "/";
        writeLocation = "file://" + writePath.toAbsolutePath() + "/";
        createTestHiveCatalog();
        createTestHiveDatabase();

        // context
        ConnectContext connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
    }

    @AfterClass
    public static void afterClass() {
        hmsClient.dropDatabase(dbName);
    }

    public static void createTestHiveCatalog() throws IOException {
        fs = new LocalDfsFileSystem();

        if (hasRealHmsService) {
            // If you have a real HMS service, then you can use this client to create real connections for testing
            HiveConf entries = new HiveConf();
            entries.set("hive.metastore.uris", uri);
            hmsClient = new ThriftHMSCachedClient(entries, 2);
        } else {
            hmsClient = new TestHMSCachedClient();
        }
        hmsOps = new HiveMetadataOps(null, hmsClient, fs);
    }

    public static void createTestHiveDatabase() {
        // create database
        HiveDatabaseMetadata dbMetadata = new HiveDatabaseMetadata();
        dbMetadata.setDbName(dbName);
        dbMetadata.setLocationUri(dbLocation);
        hmsClient.createDatabase(dbMetadata);
    }

    @Before
    public void before() {
        // create table for tbWithPartition
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", PrimitiveType.INT, true));
        columns.add(new Column("c2", PrimitiveType.STRING, true));
        columns.add(new Column("c3", PrimitiveType.STRING, false));
        List<String> partitionKeys = new ArrayList<>();
        partitionKeys.add("c3");
        String fileFormat = "orc";
        HiveTableMetadata tableMetadata = new HiveTableMetadata(
                dbName, tbWithPartition, Optional.of(dbLocation + tbWithPartition),
                columns, partitionKeys,
                new HashMap<>(), fileFormat, "");
        hmsClient.createTable(tableMetadata, true);

        // create table for tbWithoutPartition
        HiveTableMetadata tableMetadata2 = new HiveTableMetadata(
                    dbName, tbWithoutPartition, Optional.of(dbLocation + tbWithPartition),
                    columns, new ArrayList<>(),
                    new HashMap<>(), fileFormat, "");
        hmsClient.createTable(tableMetadata2, true);
    }

    @After
    public void after() {
        hmsClient.dropTable(dbName, tbWithoutPartition);
        hmsClient.dropTable(dbName, tbWithPartition);
    }

    @Test
    public void testNewPartitionForUnPartitionedTable() throws IOException {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew(null));
        Assert.assertThrows(Exception.class, () -> commit(dbName, tbWithoutPartition, pus));
    }

    @Test
    public void testAppendPartitionForUnPartitionedTable() throws IOException {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomAppend(null));
        pus.add(createRandomAppend(null));
        pus.add(createRandomAppend(null));
        commit(dbName, tbWithoutPartition, pus);
        Table table = hmsClient.getTable(dbName, tbWithoutPartition);
        assertNumRows(3, table);

        List<THivePartitionUpdate> pus2 = new ArrayList<>();
        pus2.add(createRandomAppend(null));
        pus2.add(createRandomAppend(null));
        pus2.add(createRandomAppend(null));
        commit(dbName, tbWithoutPartition, pus2);
        table = hmsClient.getTable(dbName, tbWithoutPartition);
        assertNumRows(6, table);
    }

    @Test
    public void testOverwritePartitionForUnPartitionedTable() throws IOException {
        testAppendPartitionForUnPartitionedTable();
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomOverwrite(null));
        pus.add(createRandomOverwrite(null));
        pus.add(createRandomOverwrite(null));
        commit(dbName, tbWithoutPartition, pus);
        Table table = hmsClient.getTable(dbName, tbWithoutPartition);
        assertNumRows(3, table);
    }

    @Test
    public void testNewPartitionForPartitionedTable() throws IOException {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("a"));
        pus.add(createRandomNew("a"));
        pus.add(createRandomNew("a"));
        pus.add(createRandomNew("b"));
        pus.add(createRandomNew("b"));
        pus.add(createRandomNew("c"));
        commit(dbName, tbWithPartition, pus);

        Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        assertNumRows(3, pa);
        Partition pb = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("b"));
        assertNumRows(2, pb);
        Partition pc = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("c"));
        assertNumRows(1, pc);
    }

    @Test
    public void testAppendPartitionForPartitionedTable() throws IOException {
        testNewPartitionForPartitionedTable();

        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("b"));
        pus.add(createRandomAppend("b"));
        pus.add(createRandomAppend("c"));
        commit(dbName, tbWithPartition, pus);

        Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        assertNumRows(6, pa);
        Partition pb = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("b"));
        assertNumRows(4, pb);
        Partition pc = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("c"));
        assertNumRows(2, pc);
    }

    @Test
    public void testOverwritePartitionForPartitionedTable() throws IOException {
        testAppendPartitionForPartitionedTable();
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomOverwrite("a"));
        pus.add(createRandomOverwrite("b"));
        pus.add(createRandomOverwrite("c"));
        commit(dbName, tbWithPartition, pus);

        Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        assertNumRows(1, pa);
        Partition pb = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("b"));
        assertNumRows(1, pb);
        Partition pc = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("c"));
        assertNumRows(1, pc);
    }

    @Test
    public void testNewManyPartitionForPartitionedTable() throws IOException {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        int nums = 150;
        for (int i = 0; i < nums; i++) {
            pus.add(createRandomNew("" + i));
        }

        commit(dbName, tbWithPartition, pus);
        for (int i = 0; i < nums; i++) {
            Partition p = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("" + i));
            assertNumRows(1, p);
        }

        try {
            commit(dbName, tbWithPartition, Collections.singletonList(createRandomNew("1")));
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("failed to add partitions"));
        }
    }

    @Test
    public void testErrorPartitionTypeFromHmsCheck() throws IOException {
        // first add three partition: a,b,c
        testNewPartitionForPartitionedTable();

        // second append two partition: a,x
        // but there is no 'x' partition in the previous table, so when verifying based on HMS,
        // it will throw exception
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("x"));

        Assert.assertThrows(
                Exception.class,
                () -> commit(dbName, tbWithPartition, pus)
        );
    }

    public void assertNumRows(long expected, Partition p) {
        Assert.assertEquals(expected, Long.parseLong(p.getParameters().get("numRows")));
    }

    public void assertNumRows(long expected, Table t) {
        Assert.assertEquals(expected, Long.parseLong(t.getParameters().get("numRows")));
    }

    public THivePartitionUpdate genOnePartitionUpdate(TUpdateMode mode) throws IOException {
        return genOnePartitionUpdate("", mode);
    }

    public THivePartitionUpdate genOnePartitionUpdate(String partitionValue, TUpdateMode mode) throws IOException {

        String uuid = UUID.randomUUID().toString();
        THiveLocationParams location = new THiveLocationParams();
        String targetPath = dbLocation + uuid + "/" + partitionValue;

        location.setTargetPath(targetPath);
        location.setWritePath(writeLocation + partitionValue);

        THivePartitionUpdate pu = new THivePartitionUpdate();
        if (partitionValue != null) {
            pu.setName(partitionValue);
        }
        pu.setUpdateMode(mode);
        pu.setRowCount(1);
        pu.setFileSize(1);
        pu.setLocation(location);
        String f1 = uuid + "f1";
        String f2 = uuid + "f2";
        String f3 = uuid + "f3";

        pu.setFileNames(new ArrayList<String>() {
            {
                add(f1);
                add(f2);
                add(f3);
            }
        });

        if (mode != TUpdateMode.NEW) {
            fs.makeDir(targetPath);
        }

        fs.createFile(writeLocation + partitionValue + "/" + f1);
        fs.createFile(writeLocation + partitionValue + "/" + f2);
        fs.createFile(writeLocation + partitionValue + "/" + f3);
        return pu;
    }

    public THivePartitionUpdate createRandomNew(String partition) throws IOException {
        return partition == null ? genOnePartitionUpdate(TUpdateMode.NEW) :
            genOnePartitionUpdate("c3=" + partition, TUpdateMode.NEW);
    }

    public THivePartitionUpdate createRandomAppend(String partition) throws IOException {
        return partition == null ? genOnePartitionUpdate(TUpdateMode.APPEND) :
            genOnePartitionUpdate("c3=" + partition, TUpdateMode.APPEND);
    }

    public THivePartitionUpdate createRandomOverwrite(String partition) throws IOException {
        return partition == null ? genOnePartitionUpdate(TUpdateMode.OVERWRITE) :
            genOnePartitionUpdate("c3=" + partition, TUpdateMode.OVERWRITE);
    }

    public void commit(String dbName,
                       String tableName,
                       List<THivePartitionUpdate> hivePUs) {
        HMSTransaction hmsTransaction = new HMSTransaction(hmsOps);
        hmsTransaction.setHivePartitionUpdates(hivePUs);
        hmsTransaction.finishInsertTable(dbName, tableName);
        hmsTransaction.commit();
    }

    public void mockAddPartitionTaskException(Runnable runnable) {
        new MockUp<HMSTransaction.AddPartitionsTask>(HMSTransaction.AddPartitionsTask.class) {
            @Mock
            private void run(HiveMetadataOps hiveOps) {
                runnable.run();
                throw new RuntimeException("failed to add partition");
            }
        };
    }

    public void mockDoOther(Runnable runnable) {
        new MockUp<HMSTransaction.HmsCommitter>(HMSTransaction.HmsCommitter.class) {
            @Mock
            private void doNothing() {
                runnable.run();
                throw new RuntimeException("failed to do nothing");
            }
        };
    }

    public void mockUpdateStatisticsTaskException(Runnable runnable) {
        new MockUp<HMSTransaction.UpdateStatisticsTask>(HMSTransaction.UpdateStatisticsTask.class) {
            @Mock
            private void run(HiveMetadataOps hiveOps) {
                runnable.run();
                throw new RuntimeException("failed to update partition");
            }
        };
    }

    @Test
    public void testRollbackNewPartitionForPartitionedTableForFilesystem() throws IOException {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("a"));

        THiveLocationParams location = pus.get(0).getLocation();

        // For new partition, there should be no target path
        Assert.assertFalse(fs.exists(location.getTargetPath()).ok());
        Assert.assertTrue(fs.exists(location.getWritePath()).ok());

        mockAddPartitionTaskException(() -> {
            // When the commit is completed, these files should be renamed successfully
            String targetPath = location.getTargetPath();
            Assert.assertTrue(fs.exists(targetPath).ok());
            for (String file : pus.get(0).getFileNames()) {
                Assert.assertTrue(fs.exists(targetPath + "/" + file).ok());
            }
        });

        try {
            commit(dbName, tbWithPartition, pus);
            Assert.assertTrue(false);
        } catch (Exception e) {
            // ignore
        }

        // After rollback, these files will be deleted
        String targetPath = location.getTargetPath();
        Assert.assertFalse(fs.exists(targetPath).ok());
        for (String file : pus.get(0).getFileNames()) {
            Assert.assertFalse(fs.exists(targetPath + "/" + file).ok());
        }
    }


    @Test
    public void testRollbackNewPartitionForPartitionedTableWithNewPartition() throws IOException {
        // first create three partitions: a,b,c
        testNewPartitionForPartitionedTable();

        // second add 'new partition' for 'x'
        //        add 'append partition' for 'a'
        // when 'doCommit', 'new partition' will be executed before 'append partition'
        // so, when 'rollback', the 'x' partition will be added and then deleted
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("x"));
        pus.add(createRandomAppend("a"));

        THiveLocationParams location = pus.get(0).getLocation();

        // For new partition, there should be no target path
        Assert.assertFalse(fs.exists(location.getTargetPath()).ok());
        Assert.assertTrue(fs.exists(location.getWritePath()).ok());

        mockUpdateStatisticsTaskException(() -> {
            // When the commit is completed, these files should be renamed successfully
            String targetPath = location.getTargetPath();
            Assert.assertTrue(fs.exists(targetPath).ok());
            for (String file : pus.get(0).getFileNames()) {
                Assert.assertTrue(fs.exists(targetPath + "/" + file).ok());
            }
            // new partition will be executed before append partition,
            // so, we can get the new partition
            Partition px = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("x"));
            assertNumRows(1, px);
        });

        try {
            commit(dbName, tbWithPartition, pus);
            Assert.assertTrue(false);
        } catch (Exception e) {
            // ignore
        }

        // After rollback, these files will be deleted
        String targetPath = location.getTargetPath();
        Assert.assertFalse(fs.exists(targetPath).ok());
        for (String file : pus.get(0).getFileNames()) {
            Assert.assertFalse(fs.exists(targetPath + "/" + file).ok());
        }
        // x partition will be deleted
        Assert.assertThrows(
                "the 'x' partition should be deleted",
                Exception.class,
                () -> hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("x"))
        );
    }

    @Test
    public void testRollbackNewPartitionForPartitionedTableWithNewAppendPartition() throws IOException {
        // first create three partitions: a,b,c
        testNewPartitionForPartitionedTable();

        // second add 'new partition' for 'x'
        //        add 'append partition' for 'a'
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("x"));
        pus.add(createRandomAppend("a"));

        THiveLocationParams location = pus.get(0).getLocation();

        // For new partition, there should be no target path
        Assert.assertFalse(fs.exists(location.getTargetPath()).ok());
        Assert.assertTrue(fs.exists(location.getWritePath()).ok());

        mockDoOther(() -> {
            // When the commit is completed, these files should be renamed successfully
            String targetPath = location.getTargetPath();
            Assert.assertTrue(fs.exists(targetPath).ok());
            for (String file : pus.get(0).getFileNames()) {
                Assert.assertTrue(fs.exists(targetPath + "/" + file).ok());
            }
            // new partition will be executed,
            // so, we can get the new partition
            Partition px = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("x"));
            assertNumRows(1, px);
            // append partition will be executed,
            // so, we can get the updated partition
            Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
            assertNumRows(4, pa);
        });

        try {
            commit(dbName, tbWithPartition, pus);
            Assert.assertTrue(false);
        } catch (Exception e) {
            // ignore
        }

        // After rollback, these files will be deleted
        String targetPath = location.getTargetPath();
        Assert.assertFalse(fs.exists(targetPath).ok());
        for (String file : pus.get(0).getFileNames()) {
            Assert.assertFalse(fs.exists(targetPath + "/" + file).ok());
        }
        // x partition will be deleted
        Assert.assertThrows(
                "the 'x' partition should be deleted",
                Exception.class,
                () -> hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("x"))
        );
        // the 'a' partition should be rollback
        Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        assertNumRows(3, pa);
    }
}
