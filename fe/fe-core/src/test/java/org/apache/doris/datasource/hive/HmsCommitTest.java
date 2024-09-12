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

import org.apache.doris.backup.Status;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.info.SimpleTableInfo;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.TestHMSCachedClient;
import org.apache.doris.fs.FileSystem;
import org.apache.doris.fs.FileSystemProvider;
import org.apache.doris.fs.LocalDfsFileSystem;
import org.apache.doris.fs.remote.SwitchingFileSystem;
import org.apache.doris.nereids.trees.plans.commands.insert.HiveInsertCommandContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUniqueId;
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
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HmsCommitTest {

    private static HiveMetadataOps hmsOps;
    private static HMSCachedClient hmsClient;

    private static FileSystemProvider fileSystemProvider;
    private static final String dbName = "test_db";
    private static final String tbWithPartition = "test_tb_with_partition";
    private static final String tbWithoutPartition = "test_tb_without_partition";
    private static FileSystem fs;
    private static LocalDfsFileSystem localDFSFileSystem;
    private static Executor fileSystemExecutor;
    static String dbLocation;
    static String writeLocation;
    static String uri = "thrift://127.0.0.1:9083";
    static boolean hasRealHmsService = false;
    private ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Throwable {
        Path warehousePath = Files.createTempDirectory("test_warehouse_");
        Path writePath = Files.createTempDirectory("test_write_");
        dbLocation = "file://" + warehousePath.toAbsolutePath() + "/";
        writeLocation = "file://" + writePath.toAbsolutePath() + "/";
        createTestHiveCatalog();
        createTestHiveDatabase();
    }

    @AfterClass
    public static void afterClass() {
        hmsClient.dropDatabase(dbName);
    }

    public static void createTestHiveCatalog() throws IOException {
        localDFSFileSystem = new LocalDfsFileSystem();
        new MockUp<SwitchingFileSystem>(SwitchingFileSystem.class) {
            @Mock
            public FileSystem fileSystem(String location) {
                return localDFSFileSystem;
            }
        };
        fs = new SwitchingFileSystem(null, null, null);

        if (hasRealHmsService) {
            // If you have a real HMS service, then you can use this client to create real connections for testing
            HiveConf entries = new HiveConf();
            entries.set("hive.metastore.uris", uri);
            hmsClient = new ThriftHMSCachedClient(entries, 2);
        } else {
            hmsClient = new TestHMSCachedClient();
        }
        hmsOps = new HiveMetadataOps(null, hmsClient);
        fileSystemProvider = ctx -> fs;
        fileSystemExecutor = Executors.newFixedThreadPool(16);
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
                dbName, tbWithPartition, Optional.of(dbLocation + tbWithPartition + UUID.randomUUID()),
                columns, partitionKeys,
                new HashMap<>(), fileFormat, "");
        hmsClient.createTable(tableMetadata, true);

        // create table for tbWithoutPartition
        HiveTableMetadata tableMetadata2 = new HiveTableMetadata(
                dbName, tbWithoutPartition, Optional.of(dbLocation + tbWithPartition + UUID.randomUUID()),
                columns, new ArrayList<>(),
                new HashMap<>(), fileFormat, "");
        hmsClient.createTable(tableMetadata2, true);

        // context
        connectContext = new ConnectContext();
        connectContext.setThreadLocalInfo();
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
        genQueryID();
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomAppend(null));
        pus.add(createRandomAppend(null));
        pus.add(createRandomAppend(null));
        new MockUp<HMSTransaction.HmsCommitter>(HMSTransaction.HmsCommitter.class) {
            @Mock
            private void doNothing() {
                Assert.assertEquals(Status.ErrCode.NOT_FOUND, fs.exists(getWritePath()).getErrCode());
            }
        };
        commit(dbName, tbWithoutPartition, pus);
        Table table = hmsClient.getTable(dbName, tbWithoutPartition);
        assertNumRows(3, table);


        genQueryID();
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

        genQueryID();
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
        new MockUp<HMSTransaction.HmsCommitter>(HMSTransaction.HmsCommitter.class) {
            @Mock
            private void doNothing() {
                Assert.assertEquals(Status.ErrCode.NOT_FOUND, fs.exists(getWritePath()).getErrCode());
            }
        };
        genQueryID();
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

        genQueryID();
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

        genQueryID();
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
        genQueryID();
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

        genQueryID();
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

        genQueryID();
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

        String queryId = "";
        if (connectContext.queryId() != null) {
            queryId = DebugUtil.printId(connectContext.queryId());
        }

        THiveLocationParams location = new THiveLocationParams();
        String targetPath = dbLocation + queryId + "/" + partitionValue;

        location.setTargetPath(targetPath);
        String writePath = writeLocation + queryId + "/" + partitionValue;
        location.setWritePath(writePath);

        THivePartitionUpdate pu = new THivePartitionUpdate();
        if (partitionValue != null) {
            pu.setName(partitionValue);
        }
        pu.setUpdateMode(mode);
        pu.setRowCount(1);
        pu.setFileSize(1);
        pu.setLocation(location);
        String uuid = UUID.randomUUID().toString();
        String f1 = queryId + "_" + uuid + "_f1.orc";
        String f2 = queryId + "_" + uuid + "_f2.orc";
        String f3 = queryId + "_" + uuid + "_f3.orc";

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

        localDFSFileSystem.createFile(writePath + "/" + f1);
        localDFSFileSystem.createFile(writePath + "/" + f2);
        localDFSFileSystem.createFile(writePath + "/" + f3);
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

    private String getWritePath() {
        String queryId = DebugUtil.printId(ConnectContext.get().queryId());
        return writeLocation + queryId + "/";
    }

    public void commit(String dbName,
            String tableName,
            List<THivePartitionUpdate> hivePUs) {
        HMSTransaction hmsTransaction = new HMSTransaction(hmsOps, fileSystemProvider, fileSystemExecutor);
        hmsTransaction.setHivePartitionUpdates(hivePUs);
        HiveInsertCommandContext ctx = new HiveInsertCommandContext();
        String queryId = DebugUtil.printId(ConnectContext.get().queryId());
        ctx.setQueryId(queryId);
        ctx.setWritePath(getWritePath());
        hmsTransaction.beginInsertTable(ctx);
        hmsTransaction.finishInsertTable(new SimpleTableInfo(dbName, tableName));
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

    public void mockAsyncRenameDir(Runnable runnable) {
        new MockUp<HMSTransaction>(HMSTransaction.class) {
            @Mock
            private void wrapperAsyncRenameDirWithProfileSummary(Executor executor,
                    List<CompletableFuture<?>> renameFileFutures,
                    AtomicBoolean cancelled,
                    String origFilePath,
                    String destFilePath,
                    Runnable runWhenPathNotExist) {
                runnable.run();
                throw new RuntimeException("failed to rename dir");
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

    public void genQueryID() {
        connectContext.setQueryId(new TUniqueId(new Random().nextInt(), new Random().nextInt()));
    }

    @Test
    public void testRollbackWritePath() throws IOException {
        genQueryID();
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("a"));

        THiveLocationParams location = pus.get(0).getLocation();

        // For new partition, there should be no target path
        Assert.assertFalse(fs.exists(location.getTargetPath()).ok());
        Assert.assertTrue(fs.exists(location.getWritePath()).ok());

        mockAsyncRenameDir(() -> {
            // commit will be failed, and it will remain some files in write path
            String writePath = location.getWritePath();
            Assert.assertTrue(fs.exists(writePath).ok());
            for (String file : pus.get(0).getFileNames()) {
                Assert.assertTrue(fs.exists(writePath + "/" + file).ok());
            }
        });

        try {
            commit(dbName, tbWithPartition, pus);
            Assert.assertTrue(false);
        } catch (Exception e) {
            // ignore
        }

        // After rollback, these files in write path will be deleted
        String writePath = location.getWritePath();
        Assert.assertFalse(fs.exists(writePath).ok());
        for (String file : pus.get(0).getFileNames()) {
            Assert.assertFalse(fs.exists(writePath + "/" + file).ok());
        }
    }

    @Test
    public void testRollbackNewPartitionForPartitionedTableForFilesystem() throws IOException {
        genQueryID();
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

        genQueryID();

        // second add 'new partition' for 'x'
        //        add 'append partition' for 'a'
        // when 'doCommit', 'new partition' will be executed before 'append partition'
        // so, when 'rollback', the 'x' partition will be added and then deleted
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("x"));
        pus.add(createRandomAppend("a"));

        THiveLocationParams locationForX = pus.get(0).getLocation();
        THiveLocationParams locationForA = pus.get(0).getLocation();

        // For new partition, there should be no target path
        Assert.assertFalse(fs.exists(locationForX.getTargetPath()).ok());
        Assert.assertTrue(fs.exists(locationForX.getWritePath()).ok());

        mockUpdateStatisticsTaskException(() -> {
            // When the commit is completed, these files should be renamed successfully
            String targetPath = locationForX.getTargetPath();
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
        String targetPath = locationForX.getTargetPath();
        Assert.assertFalse(fs.exists(targetPath).ok());
        for (String file : pus.get(0).getFileNames()) {
            Assert.assertFalse(fs.exists(targetPath + "/" + file).ok());
        }
        Assert.assertFalse(fs.exists(locationForX.getWritePath()).ok());
        Assert.assertFalse(fs.exists(locationForA.getWritePath()).ok());
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

        genQueryID();
        // second add 'new partition' for 'x'
        //        add 'append partition' for 'a'
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("x"));
        pus.add(createRandomAppend("a"));

        THiveLocationParams locationForParX = pus.get(0).getLocation();
        // in test, targetPath is a random path
        // but when appending a partition, it uses the location of the original partition as the targetPath
        // so here we need to get the path of partition a
        Partition a = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        String location = a.getSd().getLocation();
        pus.get(1).getLocation().setTargetPath(location);
        THiveLocationParams locationForParA = pus.get(1).getLocation();

        // For new partition, there should be no target path
        Assert.assertFalse(fs.exists(locationForParX.getTargetPath()).ok());
        Assert.assertTrue(fs.exists(locationForParX.getWritePath()).ok());

        // For exist partition
        Assert.assertTrue(fs.exists(locationForParA.getTargetPath()).ok());

        mockDoOther(() -> {
            // When the commit is completed, these files should be renamed successfully
            String targetPathForX = locationForParX.getTargetPath();
            Assert.assertTrue(fs.exists(targetPathForX).ok());
            for (String file : pus.get(0).getFileNames()) {
                Assert.assertTrue(fs.exists(targetPathForX + "/" + file).ok());
            }
            String targetPathForA = locationForParA.getTargetPath();
            for (String file : pus.get(1).getFileNames()) {
                Assert.assertTrue(fs.exists(targetPathForA + "/" + file).ok());
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
        String targetPathForX = locationForParX.getTargetPath();
        Assert.assertFalse(fs.exists(targetPathForX).ok());
        for (String file : pus.get(0).getFileNames()) {
            Assert.assertFalse(fs.exists(targetPathForX + "/" + file).ok());
        }
        Assert.assertFalse(fs.exists(locationForParX.getWritePath()).ok());
        String targetPathForA = locationForParA.getTargetPath();
        for (String file : pus.get(1).getFileNames()) {
            Assert.assertFalse(fs.exists(targetPathForA + "/" + file).ok());
        }
        Assert.assertTrue(fs.exists(targetPathForA).ok());
        Assert.assertFalse(fs.exists(locationForParA.getWritePath()).ok());
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

