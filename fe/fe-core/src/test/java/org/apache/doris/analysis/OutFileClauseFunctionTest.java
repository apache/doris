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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.utframe.DorisAssert;
import org.apache.doris.utframe.UtFrameUtils;

import java.io.File;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class OutFileClauseFunctionTest {

    private static String baseDir = "fe";
    private static String runningDir = baseDir + "/mocked/MaterializedViewFunctionTest/"
            + UUID.randomUUID().toString() + "/";
    private static ConnectContext ctx;

    private static final String DB_NAME = "db1";

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        FeConstants.runningUnitTest = true;
        Config.enable_outfile_to_local = true;
        ctx = UtFrameUtils.createDefaultCtx();
        UtFrameUtils.createDorisCluster(runningDir);
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt("create database db1;", ctx);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        String createTableSQL = "create table " + DB_NAME
                + ".test  (k1 int, k2 varchar ) "
                + "distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(createTableSQL, ctx);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    @AfterClass
    public static void tearDown() {
        File file = new File(runningDir);
        file.delete();
    }

    @Test
    public void testSelectStmtOutFileClause() throws Exception {
        String query1 = "select * from db1.test into outfile \"file:///" + runningDir + "/result_\";";
        QueryStmt analyzedQueryStmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(query1, ctx);
        Assert.assertTrue(analyzedQueryStmt.hasOutFileClause());
        OutFileClause outFileClause = analyzedQueryStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assert.assertTrue(isOutFileClauseAnalyzed);
        Assert.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_CSV_PLAIN);
    }

    @Test
    public void testSetOperationStmtOutFileClause() throws Exception {
        String query1 = "select * from db1.test union select * from db1.test into outfile \"file:///" + runningDir + "/result_\";";
        QueryStmt analyzedSetOperationStmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(query1, ctx);
        Assert.assertTrue(analyzedSetOperationStmt.hasOutFileClause());
        OutFileClause outFileClause = analyzedSetOperationStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assert.assertTrue(isOutFileClauseAnalyzed);
        Assert.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_CSV_PLAIN);
    }

    @Test
    public void testParquetFormat() throws Exception {
        String query1 = "select * from db1.test union select * from db1.test into outfile \"file:///" + runningDir + "/result_\" FORMAT AS PARQUET;";
        QueryStmt analyzedSetOperationStmt = (QueryStmt) UtFrameUtils.parseAndAnalyzeStmt(query1, ctx);
        Assert.assertTrue(analyzedSetOperationStmt.hasOutFileClause());
        OutFileClause outFileClause = analyzedSetOperationStmt.getOutFileClause();
        boolean isOutFileClauseAnalyzed = Deencapsulation.getField(outFileClause, "isAnalyzed");
        Assert.assertTrue(isOutFileClauseAnalyzed);
        Assert.assertEquals(outFileClause.getFileFormatType(), TFileFormatType.FORMAT_PARQUET);
    }
}
