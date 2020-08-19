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

import avro.shaded.com.google.common.collect.Lists;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.UUID;

public class CreateTableLikeTest {
    private static String runningDir = "fe/mocked/CreateTableLikeTest/" + UUID.randomUUID().toString() + "/";

    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinDorisCluster(runningDir);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test;";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    private static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private static void createTableLike(String sql) throws Exception {
        CreateTableLikeStmt createTableLikeStmt = (CreateTableLikeStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTableLike(createTableLikeStmt);
    }

    @Test
    public void testNormal() throws Exception {
        String createTableWithoutPartitionSql = "create table test.tbl1\n" + "(k1 int, k2 int)\n" + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ";
        ExceptionChecker.expectThrowsNoException(() -> createTable(createTableWithoutPartitionSql));

        String createTableLikeStmt = "create table test.tbl1_like like test.tbl1";
        ExceptionChecker.expectThrowsNoException(() -> createTableLike(createTableLikeStmt));
        Database db = Catalog.getCurrentCatalog().getDb("default_cluster:tbl1");
        OlapTable tbl1Like = (OlapTable) db.getTable("tbl1_like");

        List<String> createTableStmt = Lists.newArrayList();
        Catalog.getDdlStmt(tbl1Like, createTableStmt, null, null, false, true /* hide password */);
        System.out.println(createTableLikeStmt);
    }
}
