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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * test for CreateTableAsSelectStmt.
 **/
public class CreateTableAsSelectStmtTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        String varcharTable = "CREATE TABLE `test`.`varchar_table`\n" + "(\n"
                + "    `userId`   varchar(255) NOT NULL,\n" + "    `username` varchar(255) NOT NULL\n"
                + ") ENGINE = OLAP unique KEY(`userId`)\n" + "COMMENT 'varchar_table'\n"
                + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n" + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n\"disable_auto_compaction\" = \"false\"\n" + ")";
        createTable(varcharTable);
        String decimalTable = "CREATE TABLE `test`.`decimal_table`\n" + "(\n"
                + "    `userId`   varchar(255) NOT NULL,\n" + "    `amount_decimal` decimal(10, 2) NOT NULL\n"
                + ") ENGINE = OLAP unique KEY(`userId`)\n" + "COMMENT 'decimal_table'\n"
                + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n" + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\")";
        createTable(decimalTable);
        String joinTable = "CREATE TABLE `test`.`join_table` (`userId`   varchar(255) NOT NULL,"
                + " `status`   int NOT NULL)" + " ENGINE = OLAP unique KEY(`userId`)\n" + "COMMENT 'join_table'\n"
                + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n" + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n" + "\"in_memory\" = \"false\",\n"
                + "\"storage_format\" = \"V2\"\n)";
        createTable(joinTable);
        String defaultTimestampTable =
                "CREATE TABLE `test`.`default_timestamp_table` (`userId`   varchar(255) NOT NULL,"
                        + " `date`  datetime NULL DEFAULT CURRENT_TIMESTAMP)" + " ENGINE = OLAP unique KEY(`userId`)\n"
                        + "COMMENT 'default_timestamp_table'\n" + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n" + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"in_memory\" = \"false\",\n" + "\"storage_format\" = \"V2\"\n)";
        createTable(defaultTimestampTable);
    }

    @Test
    public void testDecimal() throws Exception {
        String selectFromDecimal = "create table `test`.`select_decimal_table` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select * from `test`.`decimal_table`";
        createTableAsSelect(selectFromDecimal);
        Assertions.assertEquals("CREATE TABLE `select_decimal_table` (\n"
                        + "  `userId` varchar(255) NOT NULL,\n"
                        + "  `amount_decimal` "
                        + "DECIMAL" + "(10, 2) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showCreateTableByName("select_decimal_table").getResultRows().get(0).get(1));
        String selectFromDecimal1 =
                "create table `test`.`select_decimal_table_1` PROPERTIES(\"replication_num\" = \"1\") "
                        + "as select sum(amount_decimal) from `test`.`decimal_table`";
        createTableAsSelect(selectFromDecimal1);
        if (Config.enable_decimal_conversion) {
            Assertions.assertEquals(
                    "CREATE TABLE `select_decimal_table_1` (\n"
                            + "  `__sum_0` DECIMAL(38, 2) NULL\n"
                            + ") ENGINE=OLAP\n"
                            + "DUPLICATE KEY(`__sum_0`)\n"
                            + "COMMENT 'OLAP'\n"
                            + "DISTRIBUTED BY HASH(`__sum_0`) BUCKETS 10\n"
                            + "PROPERTIES (\n"
                            + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                            + "\"is_being_synced\" = \"false\",\n"
                            + "\"storage_format\" = \"V2\",\n"
                            + "\"light_schema_change\" = \"true\",\n"
                            + "\"disable_auto_compaction\" = \"false\",\n"
                            + "\"enable_single_replica_compaction\" = \"false\"\n"
                            + ");",
                    showCreateTableByName("select_decimal_table_1").getResultRows().get(0).get(1));
        } else {
            Assertions.assertEquals(
                    "CREATE TABLE `select_decimal_table_1` (\n"
                            + "  `__sum_0` decimal(27, 9) NULL\n"
                            + ") ENGINE=OLAP\n"
                            + "DUPLICATE KEY(`__sum_0`)\n"
                            + "COMMENT 'OLAP'\n"
                            + "DISTRIBUTED BY HASH(`__sum_0`) BUCKETS 10\n"
                            + "PROPERTIES (\n"
                            + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                            + "\"is_being_synced\" = \"false\",\n"
                            + "\"storage_format\" = \"V2\",\n"
                            + "\"light_schema_change\" = \"true\",\n"
                            + "\"disable_auto_compaction\" = \"false\",\n"
                            + "\"enable_single_replica_compaction\" = \"false\"\n"
                            + ");",
                    showCreateTableByName("select_decimal_table_1").getResultRows().get(0).get(1));
        }
    }

    @Test
    public void testErrorColumn() {
        String selectFromColumn =
                "create table `test`.`select_column_table`(test_error) PROPERTIES(\"replication_num\" = \"1\") "
                        + "as select * from `test`.`varchar_table`";
        ExceptionChecker.expectThrowsWithMsg(AnalysisException.class,
                "Number of columns don't equal number of SELECT statement's select list",
                () -> parseAndAnalyzeStmt(selectFromColumn, connectContext));
    }

    @Test
    public void testVarchar() throws Exception {
        String selectFromVarchar = "create table `test`.`select_varchar` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select * from `test`.`varchar_table`";
        createTableAsSelect(selectFromVarchar);
        ShowResultSet showResultSet = showCreateTableByName("select_varchar");
        Assertions.assertEquals("CREATE TABLE `select_varchar` (\n"
                        + "  `userId` varchar(255) NOT NULL,\n"
                        + "  `username` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testFunction() throws Exception {
        String selectFromFunction1 = "create table `test`.`select_function_1` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select count(*) from `test`.`varchar_table`";
        createTableAsSelect(selectFromFunction1);
        ShowResultSet showResultSet1 = showCreateTableByName("select_function_1");
        Assertions.assertEquals(
                "CREATE TABLE `select_function_1` (\n"
                        + "  `__count_0` bigint(20) NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`__count_0`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`__count_0`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet1.getResultRows().get(0).get(1));

        String selectFromFunction2 = "create table `test`.`select_function_2` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select sum(status), sum(status), sum(status), count(status), count(status) "
                + "from `test`.`join_table`";
        createTableAsSelect(selectFromFunction2);
        ShowResultSet showResultSet2 = showCreateTableByName("select_function_2");
        Assertions.assertEquals(
                "CREATE TABLE `select_function_2` (\n"
                        + "  `__sum_0` bigint(20) NULL,\n"
                        + "  `__sum_1` bigint(20) NULL,\n"
                        + "  `__sum_2` bigint(20) NULL,\n"
                        + "  `__count_3` bigint(20) NULL,\n"
                        + "  `__count_4` bigint(20) NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`__sum_0`, `__sum_1`, `__sum_2`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`__sum_0`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet2.getResultRows().get(0).get(1));
    }

    @Test
    public void testAlias() throws Exception {
        String selectAlias1 = "create table `test`.`select_alias_1` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select count(*) as amount from `test`.`varchar_table`";
        createTableAsSelect(selectAlias1);
        ShowResultSet showResultSet1 = showCreateTableByName("select_alias_1");
        Assertions.assertEquals("CREATE TABLE `select_alias_1` (\n"
                + "  `amount` bigint(20) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`amount`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`amount`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"is_being_synced\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"light_schema_change\" = \"true\",\n"
                + "\"disable_auto_compaction\" = \"false\",\n"
                + "\"enable_single_replica_compaction\" = \"false\"\n"
                + ");", showResultSet1.getResultRows().get(0).get(1));
        String selectAlias2 = "create table `test`.`select_alias_2` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select userId as alias_name, username from `test`.`varchar_table`";
        createTableAsSelect(selectAlias2);
        ShowResultSet showResultSet2 = showCreateTableByName("select_alias_2");
        Assertions.assertEquals("CREATE TABLE `select_alias_2` (\n"
                        + "  `alias_name` varchar(255) NOT NULL,\n"
                        + "  `username` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`alias_name`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`alias_name`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet2.getResultRows().get(0).get(1));
    }

    @Test
    public void testJoin() throws Exception {
        String selectFromJoin = "create table `test`.`select_join` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select vt.userId, vt.username, jt.status from `test`.`varchar_table` vt "
                + "join `test`.`join_table` jt on vt.userId=jt.userId";
        createTableAsSelect(selectFromJoin);
        ShowResultSet showResultSet = showCreateTableByName("select_join");
        Assertions.assertEquals("CREATE TABLE `select_join` (\n"
                        + "  `userId` varchar(255) NOT NULL,\n"
                        + "  `username` varchar(255) NOT NULL,\n"
                        + "  `status` int(11) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
        String selectFromJoin1 = "create table `test`.`select_join1` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select vt.userId as userId1, jt.userId as userId2, vt.username, jt.status "
                + "from `test`.`varchar_table` vt " + "join `test`.`join_table` jt on vt.userId=jt.userId";
        createTableAsSelect(selectFromJoin1);
        ShowResultSet showResultSet1 = showCreateTableByName("select_join1");
        Assertions.assertEquals("CREATE TABLE `select_join1` (\n"
                        + "  `userId1` varchar(255) NOT NULL,\n"
                        + "  `userId2` varchar(255) NOT NULL,\n"
                        + "  `username` varchar(255) NOT NULL,\n"
                        + "  `status` int(11) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId1`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId1`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet1.getResultRows().get(0).get(1));
    }

    @Test
    public void testName() throws Exception {
        String selectFromName =
                "create table `test`.`select_name`(user, testname, userstatus) PROPERTIES(\"replication_num\" = \"1\") "
                        + "as select vt.userId, vt.username, jt.status from `test`.`varchar_table` vt "
                        + "join `test`.`join_table` jt on vt.userId=jt.userId";
        createTableAsSelect(selectFromName);
        ShowResultSet showResultSet = showCreateTableByName("select_name");
        Assertions.assertEquals("CREATE TABLE `select_name` (\n"
                        + "  `user` varchar(255) NOT NULL,\n"
                        + "  `testname` varchar(255) NOT NULL,\n"
                        + "  `userstatus` int(11) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`user`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`user`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testUnion() throws Exception {
        String selectFromName = "create table `test`.`select_union` PROPERTIES(\"replication_num\" = \"1\") "
                + "as select userId  from `test`.`varchar_table` union select userId from `test`.`join_table`";
        createTableAsSelect(selectFromName);
        ShowResultSet showResultSet = showCreateTableByName("select_union");
        Assertions.assertEquals(
                "CREATE TABLE `select_union` (\n"
                        + "  `userId` varchar(255) NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");", showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testCte() throws Exception {
        String selectFromCte = "create table `test`.`select_cte` PROPERTIES(\"replication_num\" = \"1\") "
                + "as with cte_name1 as (select userId from `test`.`varchar_table`) select * from cte_name1";
        createTableAsSelect(selectFromCte);
        ShowResultSet showResultSet = showCreateTableByName("select_cte");
        Assertions.assertEquals(
                "CREATE TABLE `select_cte` (\n"
                        + "  `userId` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
        String selectFromCteAndUnion = "create table `test`.`select_cte_union` PROPERTIES(\"replication_num\" = \"1\")"
                + "as with source_data as (select 1 as id union all select 2 as id) select * from source_data;";
        createTableAsSelect(selectFromCteAndUnion);
        ShowResultSet showResultSet1 = showCreateTableByName("select_cte_union");
        Assertions.assertEquals("CREATE TABLE `select_cte_union` (\n"
                + "  `id` tinyint(4) NULL\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`id`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`id`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                + "\"is_being_synced\" = \"false\",\n"
                + "\"storage_format\" = \"V2\",\n"
                + "\"light_schema_change\" = \"true\",\n"
                + "\"disable_auto_compaction\" = \"false\",\n"
                + "\"enable_single_replica_compaction\" = \"false\"\n"
                + ");", showResultSet1.getResultRows().get(0).get(1));
    }

    @Test
    public void testPartition() throws Exception {
        String selectFromPartition = "create table `test`.`selectPartition` PARTITION BY LIST "
                + "(userId)(PARTITION p1 values in (\"CA\",\"GB\",\"US\",\"ZH\")) "
                + "PROPERTIES (\"replication_num\" = \"1\") as " + "select * from `test`.`varchar_table`;";
        createTableAsSelect(selectFromPartition);
        ShowResultSet showResultSet = showCreateTableByName("selectPartition");
        Assertions.assertEquals("CREATE TABLE `selectPartition` (\n"
                        + "  `userId` varchar(255) NOT NULL,\n"
                        + "  `username` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "PARTITION BY LIST(`userId`)\n"
                        + "(PARTITION p1 VALUES IN (\"CA\",\"GB\",\"US\",\"ZH\"))\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testDefaultTimestamp() throws Exception {
        String createSql = "create table `test`.`test_default_timestamp` PROPERTIES (\"replication_num\" = \"1\")"
                + " as select * from `test`.`default_timestamp_table`";
        createTableAsSelect(createSql);
        ShowResultSet showResultSet = showCreateTableByName("test_default_timestamp");
        Assertions.assertEquals("CREATE TABLE `test_default_timestamp` (\n"
                        + "  `userId` varchar(255) NOT NULL,\n"
                        + "  `date` datetime"
                        + " NULL DEFAULT CURRENT_TIMESTAMP\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testAggValue() throws Exception {
        String createSql = "create table `test`.`test_agg_value` PROPERTIES (\"replication_num\" = \"1\")"
                + " as select username from `test`.`varchar_table`";
        createTableAsSelect(createSql);
        ShowResultSet showResultSet = showCreateTableByName("test_agg_value");
        Assertions.assertEquals(
                "CREATE TABLE `test_agg_value` (\n"
                        + "  `username` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`username`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`username`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testUseKeyType() throws Exception {
        String createSql = "create table `test`.`test_use_key_type` UNIQUE KEY(`userId`) PROPERTIES (\"replication_num\" = \"1\")"
                + " as select * from `test`.`varchar_table`";
        createTableAsSelect(createSql);
        ShowResultSet showResultSet = showCreateTableByName("test_use_key_type");
        Assertions.assertEquals(
                "CREATE TABLE `test_use_key_type` (\n"
                        + "  `userId` varchar(255) NOT NULL,\n"
                        + "  `username` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "UNIQUE KEY(`userId`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`userId`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showResultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testQuerySchema() throws Exception {
        connectContext.setDatabase("default_cluster:test");
        String create1 = "create table test.qs1 (k1 int, k2 int) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1')";
        String create2 = "create table test.qs2 (k1 int, k2 int) distributed by hash(k1) "
                + "buckets 1 properties('replication_num' = '1')";
        createTables(create1, create2);

        String view1 = "create view test.v1 as select qs1.k1 from qs1 join qs2 on qs1.k1 = qs2.k1";
        String view2 = "create view test.v2 as with cte(s1) as (select * from v1) select * from cte";

        createView(view1);
        createView(view2);

        String sql1 = "select * from v1";

        org.apache.doris.analysis.SqlParser parser = new org.apache.doris.analysis.SqlParser(
                new org.apache.doris.analysis.SqlScanner(new StringReader(sql1)));
        QueryStmt stmt = (QueryStmt) SqlParserUtils.getStmt(parser, 0);
        Map<Long, TableIf> tableMap = Maps.newHashMap();
        Set<String> parentViewNameSet = Sets.newHashSet();
        Analyzer analyzer = new Analyzer(Env.getCurrentEnv(), ConnectContext.get());
        stmt.getTables(analyzer, true, tableMap, parentViewNameSet);

        List<String> createStmts = Lists.newArrayList();
        for (TableIf tbl : tableMap.values()) {
            List<String> createTableStmts = Lists.newArrayList();
            Env.getDdlStmt(tbl, createTableStmts, null, null, false, true, -1L);
            createStmts.add(createTableStmts.get(0));
            if (tbl.getName().equals("qs1")) {
                Assert.assertEquals("CREATE TABLE `qs1` (\n"
                                + "  `k1` int(11) NULL,\n"
                                + "  `k2` int(11) NULL\n"
                                + ") ENGINE=OLAP\n"
                                + "DUPLICATE KEY(`k1`, `k2`)\n"
                                + "COMMENT 'OLAP'\n"
                                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                                + "PROPERTIES (\n"
                                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                                + "\"is_being_synced\" = \"false\",\n"
                                + "\"storage_format\" = \"V2\",\n"
                                + "\"light_schema_change\" = \"true\",\n"
                                + "\"disable_auto_compaction\" = \"false\",\n"
                                + "\"enable_single_replica_compaction\" = \"false\"\n"
                                + ");",
                        createTableStmts.get(0));
            } else {
                Assert.assertEquals("CREATE TABLE `qs2` (\n"
                                + "  `k1` int(11) NULL,\n"
                                + "  `k2` int(11) NULL\n"
                                + ") ENGINE=OLAP\n"
                                + "DUPLICATE KEY(`k1`, `k2`)\n"
                                + "COMMENT 'OLAP'\n"
                                + "DISTRIBUTED BY HASH(`k1`) BUCKETS 1\n"
                                + "PROPERTIES (\n"
                                + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                                + "\"is_being_synced\" = \"false\",\n"
                                + "\"storage_format\" = \"V2\",\n"
                                + "\"light_schema_change\" = \"true\",\n"
                                + "\"disable_auto_compaction\" = \"false\",\n"
                                + "\"enable_single_replica_compaction\" = \"false\"\n"
                                + ");",
                        createTableStmts.get(0));
            }
        }
        Assert.assertEquals(2, createStmts.size());
    }

    @Test
    public void testVarcharLength() throws Exception {
        String createSql =
                "create table `test`.`varchar_len1` PROPERTIES (\"replication_num\" = \"1\")"
                        + " as select 'abc', concat('xx', userId), userId from `test`.`varchar_table`";
        createTableAsSelect(createSql);
        ShowResultSet showResultSet = showCreateTableByName("varchar_len1");
        String showStr = showResultSet.getResultRows().get(0).get(1);
        Assertions.assertEquals(
                "CREATE TABLE `varchar_len1` (\n"
                        + "  `__literal_0` varchar(*) NULL,\n"
                        + "  `__concat_1` varchar(*) NULL,\n"
                        + "  `userId` varchar(255) NOT NULL\n"
                        + ") ENGINE=OLAP\n"
                        + "DUPLICATE KEY(`__literal_0`)\n"
                        + "COMMENT 'OLAP'\n"
                        + "DISTRIBUTED BY HASH(`__literal_0`) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "\"replication_allocation\" = \"tag.location.default: 1\",\n"
                        + "\"is_being_synced\" = \"false\",\n"
                        + "\"storage_format\" = \"V2\",\n"
                        + "\"light_schema_change\" = \"true\",\n"
                        + "\"disable_auto_compaction\" = \"false\",\n"
                        + "\"enable_single_replica_compaction\" = \"false\"\n"
                        + ");",
                showStr);
    }
}
