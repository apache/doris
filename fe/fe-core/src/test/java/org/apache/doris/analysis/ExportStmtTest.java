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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.utframe.UtFrameUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ExportStmtTest {
    private static String runningDir = "fe/mocked/ExportStmtTest/" + UUID.randomUUID().toString() + "/";

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
        createTable("CREATE TABLE test.table2 (\n" +
                "  `dt` int(11) COMMENT \"\",\n" +
                "  `id` int(11) COMMENT \"\",\n" +
                "  `value` varchar(8) COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`dt`, `id`)\n" +
                "PARTITION BY RANGE(`dt`)\n" +
                "(PARTITION p1 VALUES LESS THAN (\"10\"))\n" +
                "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "  \"replication_num\" = \"1\"\n" +
                ");");
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

    @Test
    public void testNormal() throws Exception {
        // generate schema from table
        String queryStr = "export table test.table2 to 'file:///root/doris/' properties ('format'='parquet')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
            ExportStmt exportStmt = (ExportStmt) stmt.get(0);
            Assert.assertTrue(exportStmt.getFileFormat() == TFileFormatType.FORMAT_PARQUET);
            List<List<String>> schema = exportStmt.getSchema();
            Assert.assertTrue(schema.size() == 3);
            Assert.assertTrue(schema.get(0).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(0).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(0).get(2).equalsIgnoreCase("dt"));
            Assert.assertTrue(schema.get(1).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(1).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(1).get(2).equalsIgnoreCase("id"));
            Assert.assertTrue(schema.get(2).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(2).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(2).get(2).equalsIgnoreCase("value"));
        } catch (Exception e) {
            Assert.fail();
        }

        // generate schema from columns
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', 'columns'='id,value')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
            ExportStmt exportStmt = (ExportStmt) stmt.get(0);
            Assert.assertTrue(exportStmt.getFileFormat() == TFileFormatType.FORMAT_PARQUET);
            List<List<String>> schema = exportStmt.getSchema();
            Assert.assertTrue(schema.size() == 2);
            Assert.assertTrue(schema.get(0).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(0).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(0).get(2).equalsIgnoreCase("id"));
            Assert.assertTrue(schema.get(1).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(1).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(1).get(2).equalsIgnoreCase("value"));
        } catch (Exception e) {
            Assert.fail();
        }

        // define schema and parquet properties
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='required,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2', 'columns'='id,value')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
            ExportStmt exportStmt = (ExportStmt) stmt.get(0);
            Assert.assertTrue(exportStmt.getFileFormat() == TFileFormatType.FORMAT_PARQUET);
            List<List<String>> schema = exportStmt.getSchema();
            Assert.assertTrue(schema.size() == 2);
            Assert.assertTrue(schema.get(0).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(0).get(1).equalsIgnoreCase("int32"));
            Assert.assertTrue(schema.get(0).get(2).equalsIgnoreCase("id"));
            Assert.assertTrue(schema.get(1).get(0).equalsIgnoreCase("required"));
            Assert.assertTrue(schema.get(1).get(1).equalsIgnoreCase("byte_array"));
            Assert.assertTrue(schema.get(1).get(2).equalsIgnoreCase("value"));
            Map<String, String> fileProperties = exportStmt.getFileProperties();
            Assert.assertTrue(fileProperties.size() == 1);
            Assert.assertTrue(fileProperties.get("version").equalsIgnoreCase("v2"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        }
        // define format as csv
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='csv', " +
                "'schema'='required,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
            ExportStmt exportStmt = (ExportStmt) stmt.get(0);
            Assert.assertTrue(exportStmt.getFileFormat() == TFileFormatType.FORMAT_CSV_PLAIN);
        } catch (Exception e) {
            Assert.fail();
        }
        // default format is csv
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('schema'='required,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
            ExportStmt exportStmt = (ExportStmt) stmt.get(0);
            Assert.assertTrue(exportStmt.getFileFormat() == TFileFormatType.FORMAT_CSV_PLAIN);
        } catch (Exception e) {
            Assert.fail();
        }
        // schema is empty
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'=''," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("schema is required for parquet file"));
        }
        // schema definition error
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='optional,int32;'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("ust only contains repetition type/data type/column name"));
        }
        // schema repetition type error
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='optional,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("currently only support required type"));
        }

        // schema data type not support
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='required, largeint, id; required, byte_array, value'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains(" data type is not supported"));
        }

        // columns not equal to schema size
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='required,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2', 'columns'='dt,id,value')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Parquet schema number does not equal to projection field number"));
        }

        // schema size not equal to table columns size
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='required,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Parquet schema number does not equal to projection field number"));
        }

        // schema size not equal to table columns size
        queryStr = "export table test.table2 to 'file:///root/doris/' " +
                "properties ('format'='parquet', " +
                "'schema'='required,int32, dt;required,int32, id; required, byte_array, value'," +
                "'parquet.version'='v2')";
        try {
            List<StatementBase> stmt = UtFrameUtils.parseAndAnalyzeStmts(queryStr, connectContext);
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
