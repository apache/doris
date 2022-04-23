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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowExecutor;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

public class CreateTableAsSelectStmtTest {
    
    private static String runningDir = "fe/mocked/CreateTableAsSelectStmtTest/" + UUID.randomUUID() + "/";
    private static ConnectContext connectContext;
    
    @AfterClass
    public static void tearDown() throws Exception {
        UtFrameUtils.cleanDorisFeDir(runningDir);
    }
    
    @BeforeClass
    public static void setUp() throws Exception {
        UtFrameUtils.createDorisCluster(runningDir);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database test";
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseAndAnalyzeStmt(createDbStmtStr, connectContext);
        Catalog.getCurrentCatalog().createDb(createDbStmt);
        String varcharTable = "CREATE TABLE `test`.`varchar_table`\n" +
                "(\n" +
                "    `userId`   varchar(255) NOT NULL COMMENT '',\n" +
                "    `username` varchar(255) NOT NULL COMMENT ''\n" +
                ") ENGINE = OLAP unique KEY(`userId`)\n" +
                "COMMENT \"varchar_table\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")";
        String decimalTable = "CREATE TABLE `test`.`decimal_table`\n" +
                "(\n" +
                "    `userId`   varchar(255) NOT NULL COMMENT '',\n" +
                "    `amount_decimal` decimal(10, 2) NOT NULL COMMENT ''\n" +
                ") ENGINE = OLAP unique KEY(`userId`)\n" +
                "COMMENT \"decimal_table\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")";
        String joinTable = "CREATE TABLE `test`.`join_table`\n" +
                "(\n" +
                "    `userId`   varchar(255) NOT NULL COMMENT '',\n" +
                "    `status`   int NOT NULL COMMENT ''\n" +
                ") ENGINE = OLAP unique KEY(`userId`)\n" +
                "COMMENT \"join_table\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")";
        MetricRepo.init();
        createTable(varcharTable);
        createTable(decimalTable);
        createTable(joinTable);
    }
    
    private static void createTable(String sql) throws Exception {
        CreateTableStmt stmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(stmt);
    }
    
    private static void createTableAsSelect(String sql) throws Exception {
        CreateTableAsSelectStmt stmt = (CreateTableAsSelectStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTableAsSelect(stmt);
    }
    
    private static ShowResultSet showCreateTable(String tableName) throws Exception {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("test", tableName));
        Analyzer dummyRootAnalyzer = new Analyzer(Catalog.getCurrentCatalog(), connectContext);
        stmt.analyze(dummyRootAnalyzer);
        ShowExecutor executor = new ShowExecutor(connectContext, stmt);
        return executor.execute();
    }
    
    @Test
    public void testErrorType() {
        String selectFromDecimal = "create table `test`.`select_decimal_table` PROPERTIES(\"replication_num\" = \"1\") as select * from `test`.`decimal_table`";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Unsupported type",
                () -> UtFrameUtils.parseAndAnalyzeStmt(selectFromDecimal, connectContext));
    }
    
    @Test
    public void testErrorColumn() {
        String selectFromColumn = "create table `test`.`select_column_table`(test_error) PROPERTIES(\"replication_num\" = \"1\") as select * from `test`.`varchar_table`";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class, "Number of columns don't equal number of SELECT statement's select list",
                () -> UtFrameUtils.parseAndAnalyzeStmt(selectFromColumn, connectContext));
    }
    
    @Test
    public void testVarchar() throws Exception {
        String selectFromDecimal = "create table `test`.`select_varchar` PROPERTIES(\"replication_num\" = \"1\") as select * from `test`.`varchar_table`";
        createTableAsSelect(selectFromDecimal);
        ShowResultSet showResultSet = showCreateTable("select_varchar");
        Assert.assertEquals("CREATE TABLE `select_varchar` (\n" +
                "  `userId` varchar(255) NULL COMMENT \"\",\n" +
                "  `username` varchar(255) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`userId`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet.getResultRows().get(0).get(1));
    }
    
    @Test
    public void testFunction() throws Exception {
        String selectFromFunction1 = "create table `test`.`select_function_1` PROPERTIES(\"replication_num\" = \"1\") as select count(*) from `test`.`varchar_table`";
        createTableAsSelect(selectFromFunction1);
        ShowResultSet showResultSet1 = showCreateTable("select_function_1");
        Assert.assertEquals("CREATE TABLE `select_function_1` (\n" +
                "  `_col0` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`_col0`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`_col0`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet1.getResultRows().get(0).get(1));
        
        String selectFromFunction2 = "create table `test`.`select_function_2` PROPERTIES(\"replication_num\" = \"1\") as select sum(status), sum(status), sum(status), count(status), count(status) from `test`.`join_table`";
        createTableAsSelect(selectFromFunction2);
        ShowResultSet showResultSet2 = showCreateTable("select_function_2");
        Assert.assertEquals("CREATE TABLE `select_function_2` (\n" +
                "  `_col0` bigint(20) NULL COMMENT \"\",\n" +
                "  `_col1` bigint(20) NULL COMMENT \"\",\n" +
                "  `_col2` bigint(20) NULL COMMENT \"\",\n" +
                "  `_col3` bigint(20) NULL COMMENT \"\",\n" +
                "  `_col4` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`_col0`, `_col1`, `_col2`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`_col0`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet2.getResultRows().get(0).get(1));
    }
    
    @Test
    public void testAlias() throws Exception {
        String selectAlias1 = "create table `test`.`select_alias_1` PROPERTIES(\"replication_num\" = \"1\") as select count(*) as amount from `test`.`varchar_table`";
        createTableAsSelect(selectAlias1);
        ShowResultSet showResultSet1 = showCreateTable("select_alias_1");
        Assert.assertEquals("CREATE TABLE `select_alias_1` (\n" +
                "  `amount` bigint(20) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`amount`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`amount`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet1.getResultRows().get(0).get(1));
        String selectAlias2 = "create table `test`.`select_alias_2` PROPERTIES(\"replication_num\" = \"1\") as select userId as alias_name, username from `test`.`varchar_table`";
        createTableAsSelect(selectAlias2);
        ShowResultSet showResultSet2 = showCreateTable("select_alias_2");
        Assert.assertEquals("CREATE TABLE `select_alias_2` (\n" +
                "  `alias_name` varchar(255) NULL COMMENT \"\",\n" +
                "  `username` varchar(255) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`alias_name`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`alias_name`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet2.getResultRows().get(0).get(1));
    }
    
    @Test
    public void testJoin() throws Exception {
        String selectFromJoin = "create table `test`.`select_join` PROPERTIES(\"replication_num\" = \"1\") " +
                "as select vt.userId, vt.username, jt.status from `test`.`varchar_table` vt join `test`.`join_table` jt on vt.userId=jt.userId";
        createTableAsSelect(selectFromJoin);
        ShowResultSet showResultSet = showCreateTable("select_join");
        Assert.assertEquals("CREATE TABLE `select_join` (\n" +
                "  `userId` varchar(255) NULL COMMENT \"\",\n" +
                "  `username` varchar(255) NULL COMMENT \"\",\n" +
                "  `status` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`userId`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet.getResultRows().get(0).get(1));
    }
    
    @Test
    public void testName() throws Exception {
        String selectFromName = "create table `test`.`select_name`(user, testname, userstatus) PROPERTIES(\"replication_num\" = \"1\") " +
                "as select vt.userId, vt.username, jt.status from `test`.`varchar_table` vt join `test`.`join_table` jt on vt.userId=jt.userId";
        createTableAsSelect(selectFromName);
        ShowResultSet showResultSet = showCreateTable("select_name");
        Assert.assertEquals("CREATE TABLE `select_name` (\n" +
                "  `user` varchar(255) NULL COMMENT \"\",\n" +
                "  `testname` varchar(255) NULL COMMENT \"\",\n" +
                "  `userstatus` int(11) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`user`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`user`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet.getResultRows().get(0).get(1));
    }
    
    @Test
    public void testUnion() throws Exception {
        String selectFromName = "create table `test`.`select_union` PROPERTIES(\"replication_num\" = \"1\") " +
                "as select userId  from `test`.`varchar_table` union select userId from `test`.`join_table`";
        createTableAsSelect(selectFromName);
        ShowResultSet showResultSet = showCreateTable("select_union");
        Assert.assertEquals("CREATE TABLE `select_union` (\n" +
                "  `userId` varchar(255) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`userId`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet.getResultRows().get(0).get(1));
    }
    
    @Test
    public void testCte() throws Exception {
        String selectFromName = "create table `test`.`select_cte` PROPERTIES(\"replication_num\" = \"1\") " +
                "as with cte_name1 as (select userId from `test`.`varchar_table`) select * from cte_name1";
        createTableAsSelect(selectFromName);
        ShowResultSet showResultSet = showCreateTable("select_cte");
        Assert.assertEquals("CREATE TABLE `select_cte` (\n" +
                "  `userId` varchar(255) NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`userId`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" +
                "PROPERTIES (\n" +
                "\"replication_allocation\" = \"tag.location.default: 1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\"\n" +
                ")", showResultSet.getResultRows().get(0).get(1));
    }
}
