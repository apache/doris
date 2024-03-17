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
import org.apache.doris.thrift.THiveLocationParams;
import org.apache.doris.thrift.THivePartitionUpdate;
import org.apache.doris.thrift.TUpdateMode;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Ignore
public class HmsCommitTest {

    private static HMSExternalCatalog hmsCatalog;
    private static HiveMetadataOps hmsOps;
    private static HMSCachedClient hmsClient;
    private static final String dbName = "test_db";
    private static final String tbWithPartition = "test_tb_with_partition";
    private static final String tbWithoutPartition = "test_tb_without_partition";
    private static Path warehousePath;
    static String dbLocation;
    private String inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat";
    private String outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat";
    private String serde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde";

    @BeforeClass
    public static void beforeClass() throws Throwable {
        warehousePath = Files.createTempDirectory("test_warehouse_");
        dbLocation = "file://" + warehousePath.toAbsolutePath() + "/";
        createTestHiveCatalog();
        createTestHiveDatabase();
    }

    @AfterClass
    public static void afterClass() {
        hmsClient.dropTable(dbName, tbWithPartition);
        hmsClient.dropTable(dbName, tbWithoutPartition);
        hmsClient.dropDatabase(dbName);
    }

    public static void createTestHiveCatalog() {
        Map<String, String> props = new HashMap<>();
        props.put("type", "hms");
        props.put("hive.metastore.uris", "thrift://127.0.0.1:9083");
        props.put("hadoop.username", "hadoop");
        hmsCatalog = new HMSExternalCatalog(1, "hive_catalog", null, props, "comment");
        hmsCatalog.setInitialized();
        hmsCatalog.initLocalObjectsImpl();
        hmsOps = (HiveMetadataOps) hmsCatalog.getMetadataOps();
        hmsClient = hmsOps.getClient();
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
        // create table
        List<Column> columns = new ArrayList<>();
        columns.add(new Column("c1", PrimitiveType.INT, true));
        columns.add(new Column("c2", PrimitiveType.STRING, true));
        List<FieldSchema> partitionKeys = new ArrayList<>();
        partitionKeys.add(new FieldSchema("c3", "string", "comment"));
        HiveTableMetadata tableMetadata = new HiveTableMetadata(
                dbName, tbWithPartition, columns, partitionKeys,
                new HashMap<>(), inputFormat, outputFormat, serde);
        hmsClient.createTable(tableMetadata, true);
        HiveTableMetadata tableMetadata2 = new HiveTableMetadata(
                dbName, tbWithoutPartition, columns, new ArrayList<>(),
                new HashMap<>(), inputFormat, outputFormat, serde);
        hmsClient.createTable(tableMetadata2, true);
    }

    @After
    public void after() {
        hmsClient.dropTable(dbName, tbWithoutPartition);
        hmsClient.dropTable(dbName, tbWithPartition);
    }

    @Test
    public void testNewPartitionForUnPartitionedTable() {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("a"));
        try {
            hmsOps.commit(dbName, tbWithoutPartition, pus);
        } catch (Exception e) {
            Assert.assertEquals("Not support mode:[NEW] in unPartitioned table", e.getMessage());
        }
    }

    @Test
    public void testAppendPartitionForUnPartitionedTable() {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomAppend(""));
        pus.add(createRandomAppend(""));
        pus.add(createRandomAppend(""));
        hmsOps.commit(dbName, tbWithoutPartition, pus);
        Table table = hmsClient.getTable(dbName, tbWithoutPartition);
        Assert.assertEquals(3, Long.parseLong(table.getParameters().get("numRows")));

        List<THivePartitionUpdate> pus2 = new ArrayList<>();
        pus2.add(createRandomAppend(""));
        pus2.add(createRandomAppend(""));
        pus2.add(createRandomAppend(""));
        hmsOps.commit(dbName, tbWithoutPartition, pus2);
        table = hmsClient.getTable(dbName, tbWithoutPartition);
        Assert.assertEquals(6, Long.parseLong(table.getParameters().get("numRows")));
    }

    @Test
    public void testOverwritePartitionForUnPartitionedTable() {
        // TODO
    }

    @Test
    public void testNewPartitionForPartitionedTable() {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomNew("a"));
        pus.add(createRandomNew("a"));
        pus.add(createRandomNew("a"));
        pus.add(createRandomNew("b"));
        pus.add(createRandomNew("b"));
        pus.add(createRandomNew("c"));
        hmsOps.commit(dbName, tbWithPartition, pus);

        Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        Assert.assertEquals(3, Long.parseLong(pa.getParameters().get("numRows")));
        Partition pb = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("b"));
        Assert.assertEquals(2, Long.parseLong(pb.getParameters().get("numRows")));
        Partition pc = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("c"));
        Assert.assertEquals(1, Long.parseLong(pc.getParameters().get("numRows")));
    }

    @Test
    public void testAppendPartitionForPartitionedTable() {
        testNewPartitionForPartitionedTable();

        List<THivePartitionUpdate> pus = new ArrayList<>();
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("a"));
        pus.add(createRandomAppend("b"));
        pus.add(createRandomAppend("b"));
        pus.add(createRandomAppend("c"));
        hmsOps.commit(dbName, tbWithPartition, pus);

        Partition pa = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("a"));
        Assert.assertEquals(6, Long.parseLong(pa.getParameters().get("numRows")));
        Partition pb = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("b"));
        Assert.assertEquals(4, Long.parseLong(pb.getParameters().get("numRows")));
        Partition pc = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("c"));
        Assert.assertEquals(2, Long.parseLong(pc.getParameters().get("numRows")));
    }

    @Test
    public void testNewManyPartitionForPartitionedTable() {
        List<THivePartitionUpdate> pus = new ArrayList<>();
        int nums = 150;
        for (int i = 0; i < nums; i++) {
            pus.add(createRandomNew("" + i));
        }

        hmsOps.commit(dbName, tbWithPartition, pus);
        for (int i = 0; i < nums; i++) {
            Partition p = hmsClient.getPartition(dbName, tbWithPartition, Lists.newArrayList("" + i));
            Assert.assertEquals(1, Long.parseLong(p.getParameters().get("numRows")));
        }

        try {
            hmsOps.commit(dbName, tbWithPartition, pus);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("failed to add partitions"));
        }
    }

    public THivePartitionUpdate genOnePartitionUpdate(String partitionValue, TUpdateMode mode) {

        String uuid = UUID.randomUUID().toString();
        THiveLocationParams location = new THiveLocationParams();
        String targetPath = dbLocation + uuid;
        location.setTargetPath(targetPath);
        location.setWritePath(targetPath);

        THivePartitionUpdate pu = new THivePartitionUpdate();
        pu.setName(partitionValue);
        pu.setUpdateMode(mode);
        pu.setRowCount(1);
        pu.setFileSize(1);
        pu.setLocation(location);
        pu.setFileNames(new ArrayList<String>() {
            {
                add(targetPath + "/f1");
                add(targetPath + "/f2");
                add(targetPath + "/f3");
            }
        });
        return pu;
    }

    public THivePartitionUpdate createRandomNew(String partition) {
        return genOnePartitionUpdate("c3=" + partition, TUpdateMode.NEW);
    }

    public THivePartitionUpdate createRandomAppend(String partition) {
        return genOnePartitionUpdate("c3=" + partition, TUpdateMode.APPEND);
    }
}
