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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ConnectContext;

import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.UUID;

public class CreateTableTest {

    private static String runningDir = "fe/mocked/CreateTableTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    @AfterClass
    public static void TearDown() {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @Test
    public void testNormalOlap() throws Exception {
        String createOlapTblStmt = "create table test.tb1(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '1');";
        createTable(createOlapTblStmt);
    }

    @Test
    public void testUnknownDatabase() throws Exception {
        String createOlapTblStmt = "create table testDb.tb2(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '1');";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Unknown database 'default_cluster:testDb'");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testShortKeyTooLarge() throws Exception {
        String createOlapTblStmt = "create table test.tb3(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'short_key' = '3');";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Short key is too large. should less than: 2");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testShortKeyVarcharMiddle() throws Exception {
        String createOlapTblStmt = "create table test.tb4(key1 varchar(10), key2 int) distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'short_key' = '2');";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Varchar should not in the middle of short keys.");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNotEnoughBackend() throws Exception {
        String createOlapTblStmt = "create table test.tb5(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '3');";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Failed to find enough host with storage medium is HDD. need: 3");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testOlapTableExists() throws Exception {
        String createOlapTblStmt = "create table test.tb6(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '1');";
        createTable(createOlapTblStmt);
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Table 'tb6' already exists");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testTableNotFoundStorageMedium() throws Exception {
        ConfigBase.setMutableConfig("enable_strict_storage_medium_check", "true");
        String createOlapTblStmt = "create table test.tb7(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');";
        expectedException.expect(DdlException.class);
        expectedException.expectMessage("errCode = 2, detailMessage = Failed to find enough host with storage medium is SSD in all backends. need: 1");
        createTable(createOlapTblStmt);
    }

    @Test
    public void testNotSetStorageMediumCheck() throws Exception {
        ConfigBase.setMutableConfig("enable_strict_storage_medium_check", "false");
        String createOlapTblStmt = "create table test.tb7(key1 int, key2 varchar(10)) distributed by hash(key1) buckets 1 properties('replication_num' = '1', 'storage_medium' = 'ssd');";
        createTable(createOlapTblStmt);
    }
}