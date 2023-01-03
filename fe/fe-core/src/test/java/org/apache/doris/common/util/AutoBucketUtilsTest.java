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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ResultSetMetaData;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDisk;
import org.apache.doris.thrift.TStorageMedium;
import org.apache.doris.utframe.UtFrameUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class AutoBucketUtilsTest {
    private static String databaseName = "AutoBucketUtilsTest";
    // use a unique dir so that it won't be conflict with other unit test which
    // may also start a Mocked Frontend
    private static String runningDirBase = "fe";
    private static String runningDir = runningDirBase + "/mocked/AutoBucketUtilsTest/" + UUID.randomUUID().toString()
            + "/";
    private static List<Backend> backends = Lists.newArrayList();
    private static Random random = new Random(System.currentTimeMillis());
    private ConnectContext connectContext;

    // // create backends by be num, disk num, disk capacity
    private static void createBackends(int beNum, int diskNum, long diskCapacity) throws Exception {
        UtFrameUtils.createDorisClusterWithMultiTag(runningDir, beNum);

        // must set disk info, or the tablet scheduler won't work
        backends = Env.getCurrentSystemInfo().getClusterBackends(SystemInfoService.DEFAULT_CLUSTER);
        for (Backend be : backends) {
            Map<String, TDisk> backendDisks = Maps.newHashMap();
            for (int i = 0; i < diskNum; ++i) {
                TDisk disk = new TDisk();
                disk.setRootPath("/home/doris/" + UUID.randomUUID().toString());
                disk.setDiskTotalCapacity(diskCapacity);
                disk.setDataUsedCapacity(0);
                disk.setUsed(true);
                disk.setDiskAvailableCapacity(disk.disk_total_capacity - disk.data_used_capacity);
                disk.setPathHash(random.nextLong());
                disk.setStorageMedium(TStorageMedium.HDD);
                backendDisks.put(disk.getRootPath(), disk);
            }
            be.updateDisks(backendDisks);
        }
    }

    @Before
    public void setUp() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.tablet_checker_interval_ms = 1000;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.tablet_repair_delay_factor_second = 1;
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @After
    public void tearDown() {
        Env.getCurrentEnv().clear();
        UtFrameUtils.cleanDorisFeDir(runningDirBase);
    }

    private static String genTableNameWithoutDatabase(String estimatePartitionSize) {
        return "size_" + estimatePartitionSize;
    }

    private static String genTableName(String estimatePartitionSize) {
        return databaseName + "." + genTableNameWithoutDatabase(estimatePartitionSize);
    }

    private static String genTableNameByTag(String estimatePartitionSize, String tag) {
        return databaseName + "." + genTableNameWithoutDatabase(estimatePartitionSize) + "_" + tag;
    }

    private static String genCreateTableSql(String estimatePartitionSize) {
        return "CREATE TABLE IF NOT EXISTS " + genTableName(estimatePartitionSize) + "\n"
                + "(\n"
                + "`user_id` LARGEINT NOT NULL\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"estimate_partition_size\" = \"" + estimatePartitionSize + "\",\n"
                + "\"replication_num\" = \"1\"\n"
                + ")";
    }

    private void createTable(String sql) throws Exception {
        // create database first
        UtFrameUtils.createDatabase(connectContext, databaseName);
        UtFrameUtils.createTable(connectContext, sql);
    }

    private void createTableBySize(String estimatePartitionSize) throws Exception {
        createTable(genCreateTableSql(estimatePartitionSize));
    }

    private int getPartitionBucketNum(String tableName) throws Exception {
        ShowResultSet result = UtFrameUtils.showPartitionsByName(connectContext, tableName);
        ResultSetMetaData metaData = result.getMetaData();

        for (int i = 0; i < metaData.getColumnCount(); ++i) {
            if (metaData.getColumn(i).getName().equalsIgnoreCase("buckets")) {
                return Integer.valueOf(result.getResultRows().get(0).get(i));
            }
        }

        throw new Exception("No buckets column in show partitions result");
    }

    // also has checked create table && show partitions
    @Test
    public void testWithoutEstimatePartitionSize() throws Exception {
        String tableName = genTableName("");
        String sql = "CREATE TABLE IF NOT EXISTS " + tableName + "\n"
                + "(\n"
                + "`user_id` LARGEINT NOT NULL\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"replication_num\" = \"1\"\n"
                + ")";
        String showResultExpected = "CREATE TABLE `" + genTableNameWithoutDatabase("") + "` (\n"
                + "  `user_id` largeint(40) NOT NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`user_id`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`user_id`) BUCKETS AUTO\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"disable_auto_compaction\" = \"false\"\n"
                + ");";

        createBackends(1, 1, 2000000000);

        createTable(sql);
        ShowResultSet showCreateTableResult = UtFrameUtils.showCreateTableByName(connectContext, tableName);
        Assert.assertEquals(showResultExpected, showCreateTableResult.getResultRows().get(0).get(1));
        int bucketNum = getPartitionBucketNum(tableName);
        Assert.assertEquals(FeConstants.default_bucket_num, bucketNum);
    }

    @Test
    public void test100MB() throws Exception {
        long estimatePartitionSize = AutoBucketUtils.SIZE_100MB;
        createBackends(10, 3, 2000000000);
        Assert.assertEquals(1, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test500MB() throws Exception {
        long estimatePartitionSize = 5 * AutoBucketUtils.SIZE_100MB;
        createBackends(10, 3, 2000000000);
        Assert.assertEquals(1, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test1G() throws Exception {
        long estimatePartitionSize = AutoBucketUtils.SIZE_1GB;
        createBackends(3, 2, 500 * AutoBucketUtils.SIZE_1GB);
        Assert.assertEquals(2, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test100G() throws Exception {
        long estimatePartitionSize = 100 * AutoBucketUtils.SIZE_1GB;
        createBackends(3, 2, 500 * AutoBucketUtils.SIZE_1GB);
        Assert.assertEquals(20, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test500G_0() throws Exception {
        long estimatePartitionSize = 500 * AutoBucketUtils.SIZE_1GB;
        createBackends(3, 1, AutoBucketUtils.SIZE_1TB);
        Assert.assertEquals(63, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test500G_1() throws Exception {
        long estimatePartitionSize = 500 * AutoBucketUtils.SIZE_1GB;
        createBackends(10, 3, 2 * AutoBucketUtils.SIZE_1TB);
        Assert.assertEquals(100, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test500G_2() throws Exception {
        long estimatePartitionSize = 500 * AutoBucketUtils.SIZE_1GB;
        createBackends(1, 1, 100 * AutoBucketUtils.SIZE_1TB);
        Assert.assertEquals(100, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test1T_0() throws Exception {
        long estimatePartitionSize = AutoBucketUtils.SIZE_1TB;
        createBackends(10, 3, 2 * AutoBucketUtils.SIZE_1TB);
        Assert.assertEquals(128, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }

    @Test
    public void test1T_1() throws Exception {
        long estimatePartitionSize = AutoBucketUtils.SIZE_1TB;
        createBackends(200, 7, 4 * AutoBucketUtils.SIZE_1TB);
        Assert.assertEquals(200, AutoBucketUtils.getBucketsNum(estimatePartitionSize));
    }
}
