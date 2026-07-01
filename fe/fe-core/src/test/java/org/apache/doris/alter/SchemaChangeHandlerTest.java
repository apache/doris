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

package org.apache.doris.alter;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

public class SchemaChangeHandlerTest extends TestWithFeService {
    private static final Logger LOG = LogManager.getLogger(SchemaChangeHandlerTest.class);
    private int jobSize = 0;

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 10;
        // create database db1
        createDatabase("test");

        // create tables
        String createAggTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_agg (\n" + "user_id LARGEINT NOT NULL,\n"
                + "date DATE NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                + "last_visit_date DATETIME REPLACE DEFAULT '1970-01-01 00:00:00',\n" + "cost BIGINT SUM DEFAULT '0',\n"
                + "max_dwell_time INT MAX DEFAULT '0',\n" + "min_dwell_time INT MIN DEFAULT '99999')\n"
                + "AGGREGATE KEY(user_id, date, city, age, sex)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createAggTblStmtStr);

        String createUniqTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_uniq (\n" + "user_id LARGEINT NOT NULL,\n"
                + "username VARCHAR(50) NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                + "phone LARGEINT,\n" + "address VARCHAR(500),\n" + "register_time DATETIME)\n"
                + "UNIQUE  KEY(user_id, username)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true',\n"
                + "'enable_unique_key_merge_on_write' = 'true');";
        createTable(createUniqTblStmtStr);

        String createDupTblStmtStr = "CREATE TABLE IF NOT EXISTS test.sc_dup (\n" + "timestamp DATETIME,\n"
                + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createDupTblStmtStr);

        String createAggTblStmtStrForStruct = "CREATE TABLE IF NOT EXISTS test.sc_agg_s (\n"
                + "user_id LARGEINT NOT NULL,\n"
                + "date DATE NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                + "last_visit_date DATETIME REPLACE DEFAULT '1970-01-01 00:00:00',\n" + "cost BIGINT SUM DEFAULT '0',\n"
                + "max_dwell_time INT MAX DEFAULT '0',\n" + "min_dwell_time INT MIN DEFAULT '99999')\n"
                + "AGGREGATE KEY(user_id, date, city, age, sex)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createAggTblStmtStrForStruct);

        String createUniqTblStmtStrForStruct = "CREATE TABLE IF NOT EXISTS test.sc_uniq_s (\n"
                + "user_id LARGEINT NOT NULL,\n"
                + "username VARCHAR(50) NOT NULL,\n" + "city VARCHAR(20),\n" + "age SMALLINT,\n" + "sex TINYINT,\n"
                + "phone LARGEINT,\n" + "address VARCHAR(500),\n" + "register_time DATETIME)\n"
                + "UNIQUE  KEY(user_id, username)\n" + "DISTRIBUTED BY HASH(user_id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true',\n"
                + "'enable_unique_key_merge_on_write' = 'true');";
        createTable(createUniqTblStmtStrForStruct);

        String createDupTblStmtStrForStruct = "CREATE TABLE IF NOT EXISTS test.sc_dup_s (\n" + "timestamp DATETIME,\n"
                + "type INT,\n" + "error_code INT,\n" + "error_msg VARCHAR(1024),\n" + "op_id BIGINT,\n"
                + "op_time DATETIME)\n" + "DUPLICATE  KEY(timestamp, type)\n" + "DISTRIBUTED BY HASH(type) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createDupTblStmtStrForStruct);

        String createSeqMapTblStmt = "CREATE TABLE IF NOT EXISTS test.sc_seq_map "
                + "(k1 bigint, k2 date, c varchar(20), d varchar(20), s1 bigint)ENGINE = OLAP \n"
                + "UNIQUE KEY(k1, k2) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES('replication_num' = '1', 'light_schema_change' = 'true', "
                + "'sequence_mapping.s1' = 'c,d', 'enable_unique_key_merge_on_write' = 'false')";
        createTable(createSeqMapTblStmt);
    }

    private void waitAlterJobDone(Map<Long, AlterJobV2> alterJobs) throws Exception {
        for (AlterJobV2 alterJobV2 : alterJobs.values()) {
            while (!alterJobV2.getJobState().isFinalState()) {
                LOG.info("alter job {} is running. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
                Thread.sleep(1000);
            }
            LOG.info("alter job {} is done. state: {}", alterJobV2.getJobId(), alterJobV2.getJobState());
            Assert.assertEquals(AlterJobV2.JobState.FINISHED, alterJobV2.getJobState());

            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(alterJobV2.getDbId());
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(alterJobV2.getTableId(), Table.TableType.OLAP);
            while (tbl.getState() != OlapTable.OlapTableState.NORMAL) {
                Thread.sleep(1000);
            }
        }
    }

    private void executeAlterAndVerify(String alterStmt, OlapTable tbl, String expectedStruct, int expectSchemaVersion,
            String columnName) throws Exception {
        alterTable(alterStmt, connectContext);
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());
        jobSize++;

        tbl.readLock();
        try {
            Column column = tbl.getColumn(columnName);
            Assertions.assertTrue(column.getType().toSql().toLowerCase().contains(expectedStruct.toLowerCase()),
                    "Actual struct: " + column.getType().toSql());
            // then check schema version increase
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            int schemaVersion = indexMeta.getSchemaVersion();
            LOG.info("schema version: {}", schemaVersion);
            Assertions.assertEquals(expectSchemaVersion, schemaVersion);
        } finally {
            tbl.readUnlock();
        }
    }

    private void expectException(String alterStmt, String expectedErrorMsg) {
        try {
            alterTable(alterStmt, connectContext);
            waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());
            Assertions.fail("Expected exception: " + expectedErrorMsg);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            Assertions.assertTrue(e.getMessage().contains(expectedErrorMsg),
                    "Actual error: " + e.getMessage() + "\nExpected: " + expectedErrorMsg);
        }
    }

    @Test
    public void testWithRowBinlogSchemaChangeNoHistoricalValue() throws Exception {
        String tableName = "binlog_no_hist";
        String create = "CREATE TABLE IF NOT EXISTS test." + tableName + " (\n"
                + "k1 INT NOT NULL,\n"
                + "v1 INT\n"
                + ")\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1','light_schema_change'='true',"
                + "'enable_unique_key_merge_on_write'='true',"
                + "'binlog.enable'='true','binlog.format'='ROW','binlog.need_historical_value'='false');";
        createTable(create);

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);

        List<String> cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName)
                .collect(Collectors.toList());
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v1")));

        // single add column
        alterTable("ALTER TABLE test." + tableName + " ADD COLUMN v2 INT AFTER v1", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(2, cols.indexOf("v2"));
        Assert.assertEquals(3, cols.indexOf(Column.BINLOG_LSN_COL));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v2")));

        // multiple add column clauses in one ALTER
        alterTable("ALTER TABLE test." + tableName
                + " ADD COLUMN v3 INT AFTER v2, ADD COLUMN v4 INT AFTER v3", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(3, cols.indexOf("v3"));
        Assert.assertEquals(4, cols.indexOf("v4"));
        Assert.assertEquals(5, cols.indexOf(Column.BINLOG_LSN_COL));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v3")));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v4")));

        // AddColumnsOp: ADD COLUMN (colDef1, colDef2)
        alterTable("ALTER TABLE test." + tableName + " ADD COLUMN (v5 INT, v6 INT)", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(5, cols.indexOf("v5"));
        Assert.assertEquals(6, cols.indexOf("v6"));
        Assert.assertEquals(7, cols.indexOf(Column.BINLOG_LSN_COL));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v5")));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v6")));

        // drop column
        alterTable("ALTER TABLE test." + tableName + " DROP COLUMN v6", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertFalse(cols.contains("v6"));
        Assert.assertEquals(6, cols.indexOf(Column.BINLOG_LSN_COL));
    }

    @Test
    public void testWithRowBinlogSchemaChangeWithHistoricalValue() throws Exception {
        String tableName = "binlog_hist";
        String create = "CREATE TABLE IF NOT EXISTS test." + tableName + " (\n"
                + "k1 INT NOT NULL,\n"
                + "v1 INT\n"
                + ")\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1','light_schema_change'='true',"
                + "'enable_unique_key_merge_on_write'='true',"
                + "'binlog.enable'='true','binlog.format'='ROW','binlog.need_historical_value'='true');";
        createTable(create);

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);

        List<String> cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName)
                .collect(Collectors.toList());
        Assert.assertTrue(cols.contains(Column.generateBeforeColName("v1")));

        // single add column
        alterTable("ALTER TABLE test." + tableName + " ADD COLUMN v2 INT AFTER v1", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(2, cols.indexOf("v2"));
        Assert.assertTrue(cols.contains(Column.generateBeforeColName("v2")));
        Assert.assertEquals(cols.indexOf(Column.generateBeforeColName("v1")) + 1,
                cols.indexOf(Column.generateBeforeColName("v2")));

        // multiple add column clauses in one ALTER
        alterTable("ALTER TABLE test." + tableName
                + " ADD COLUMN v3 INT AFTER v2, ADD COLUMN v4 INT AFTER v3", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(3, cols.indexOf("v3"));
        Assert.assertEquals(4, cols.indexOf("v4"));
        Assert.assertTrue(cols.contains(Column.generateBeforeColName("v3")));
        Assert.assertTrue(cols.contains(Column.generateBeforeColName("v4")));
        Assert.assertEquals(cols.indexOf(Column.generateBeforeColName("v2")) + 1,
                cols.indexOf(Column.generateBeforeColName("v3")));
        Assert.assertEquals(cols.indexOf(Column.generateBeforeColName("v3")) + 1,
                cols.indexOf(Column.generateBeforeColName("v4")));

        // AddColumnsOp: ADD COLUMN (colDef1, colDef2)
        alterTable("ALTER TABLE test." + tableName + " ADD COLUMN (v5 INT, v6 INT)", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertEquals(5, cols.indexOf("v5"));
        Assert.assertEquals(6, cols.indexOf("v6"));
        Assert.assertTrue(cols.contains(Column.generateBeforeColName("v5")));
        Assert.assertTrue(cols.contains(Column.generateBeforeColName("v6")));
        Assert.assertEquals(cols.indexOf(Column.generateBeforeColName("v4")) + 1,
                cols.indexOf(Column.generateBeforeColName("v5")));
        Assert.assertEquals(cols.indexOf(Column.generateBeforeColName("v5")) + 1,
                cols.indexOf(Column.generateBeforeColName("v6")));

        // drop column
        alterTable("ALTER TABLE test." + tableName + " DROP COLUMN v6", connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());
        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertFalse(cols.contains("v6"));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName("v6")));

        // enable hidden sequence column should not pollute row binlog schema
        alterTable("ALTER TABLE test." + tableName
                + " ENABLE FEATURE \"SEQUENCE_LOAD\" WITH PROPERTIES (\"function_column.sequence_type\" = \"int\")",
                connectContext);
        jobSize++;
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());

        Assert.assertTrue(tbl.getBaseSchema(true).stream().anyMatch(Column::isSequenceColumn));
        cols = tbl.getRowBinlogMeta().getSchema(true).stream().map(Column::getName).collect(Collectors.toList());
        Assert.assertFalse(cols.contains(Column.SEQUENCE_COL));
        Assert.assertFalse(cols.contains(Column.generateBeforeColName(Column.SEQUENCE_COL)));
    }

    @Test
    public void testWithRowBinlogOpNotSupported() throws Exception {
        // 1) MODIFY COLUMN not supported
        String tableName = "binlog_mod";
        String create = "CREATE TABLE IF NOT EXISTS test." + tableName + " (\n"
                + "k1 INT NOT NULL,\n"
                + "v1 INT\n"
                + ")\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1','light_schema_change'='true',"
                + "'enable_unique_key_merge_on_write'='true',"
                + "'binlog.enable'='true','binlog.format'='ROW','binlog.need_historical_value'='false');";
        createTable(create);
        expectException("ALTER TABLE test." + tableName + " MODIFY COLUMN v1 BIGINT", "Table With binlog<row>");

        // 2) VARIANT not supported
        String createVariant = "CREATE TABLE test.binlog_variant (k1 INT NOT NULL, v1 VARIANT) "
                + "UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','light_schema_change'='true',"
                + "'enable_unique_key_merge_on_write'='true',"
                + "'binlog.enable'='true','binlog.format'='ROW');";
        try {
            createTable(createVariant);
            Assertions.fail("Expected exception for VARIANT column");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("variant"));
        }

        String tableName2 = "binlog_add_variant";
        String create2 = "CREATE TABLE test." + tableName2 + " (k1 INT NOT NULL, v1 INT) "
                + "UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','light_schema_change'='true',"
                + "'enable_unique_key_merge_on_write'='true',"
                + "'binlog.enable'='true','binlog.format'='ROW');";
        createTable(create2);
        expectException("ALTER TABLE test." + tableName2 + " ADD COLUMN v2 VARIANT", "VARIANT");

        // 3) AUTO_INCREMENT not supported
        String createAutoinc = "CREATE TABLE test.binlog_autoinc (k1 BIGINT NOT NULL AUTO_INCREMENT, v1 INT) "
                + "UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','light_schema_change'='true',"
                + "'enable_unique_key_merge_on_write'='true',"
                + "'binlog.enable'='true','binlog.format'='ROW');";
        try {
            createTable(createAutoinc);
            Assertions.fail("Expected exception for AUTO_INCREMENT column");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().toLowerCase().contains("auto"));
        }
    }

    @Test
    public void testWithRowBinlogPartitionOps() throws Exception {
        String tableName = "row_binlog_part";
        String create = "CREATE TABLE IF NOT EXISTS test." + tableName + " (\n"
                + "k1 INT NOT NULL\n"
                + ")\n"
                + "DUPLICATE KEY(k1)\n"
                + "PARTITION BY RANGE(k1) (\n"
                + "PARTITION p1 VALUES LESS THAN (\"10\"),\n"
                + "PARTITION p2 VALUES LESS THAN (\"20\")\n"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES('replication_num'='1','binlog.enable'='true','binlog.format'='ROW');";
        createTable(create);

        alterTable("ALTER TABLE test." + tableName + " ADD PARTITION p3 VALUES LESS THAN (\"30\")",
                connectContext);
        alterTable("ALTER TABLE test." + tableName
                        + " ADD TEMPORARY PARTITION tp1 VALUES LESS THAN (\"10\")",
                connectContext);
        alterTable("ALTER TABLE test." + tableName
                        + " REPLACE PARTITION (p1) WITH TEMPORARY PARTITION (tp1)",
                connectContext);
        alterTable("ALTER TABLE test." + tableName + " DROP PARTITION p2", connectContext);
    }

    @Test
    public void testReplaceTableWithRowBinlog() throws Exception {
        String target = "CREATE TABLE test.row_binlog_replace_target (k1 INT) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1','binlog.enable'='true','binlog.format'='ROW');";
        String source = "CREATE TABLE test.row_binlog_replace_source (k1 INT) "
                + "DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES('replication_num'='1');";
        createTable(target);
        createTable(source);
        String alterStmt = "ALTER TABLE test.row_binlog_replace_target REPLACE WITH TABLE "
                + "row_binlog_replace_source PROPERTIES('swap' = 'true')";
        alterTable(alterStmt, connectContext);
        waitAlterJobDone(Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2());
    }

    // In this test we should cover this following cases:
    //  Positive Test Case
    //    3.1 add sub-column
    //    3.2 add sub-columns
    //    3.3 add sub-column + lengthen sub-varchar-column
    //  Negative Test Case
    //    3.4 add sub-column + re-order struct-column
    //    3.5 reduce sub-column
    //    3.6 reduce sub-columns
    //    3.7 add sub-column + shorten sub-varchar-column
    //    3.8 change struct to other type
    //    3.9 add sub-column + duplicate sub-column name
    //    3.10 add sub-column + change origin sub-column name
    //    3.11 add sub-column + change origin sub-column type
    //    3.12 add sub-column with json/variant
    // ------------------------- Positive Test Case -------------------------
    private void testAddSingleSubColumn(OlapTable tbl, String tableName, String defaultValue) throws Exception {
        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s STRUCT<col:VARCHAR(10), col1:INT> "
                + defaultValue;
        executeAlterAndVerify(alterStmt, tbl, "STRUCT<col:varchar(10),col1:int>", 3, "c_s");
    }

    private void testAddNestedStructSubColumn(OlapTable tbl, String tableName, String defaultValue) throws Exception {
        // origin c_s_s : struct<s1:struct<a:int>, s2:struct<a:array<struct<a:int>>>>
        // case1. add s1 sub-column : struct<s1:struct<a:int, b:double>, s2:struct<a:array<struct<a:int>>>>
        // case2. add s2 sub-column : struct<s1:struct<a:int,b:double>, s2:struct<a:array<struct<a:int>>, b:double>>
        // case3. add s2.a sub-column : struct<s1:struct<a:int,b:double>,s2:struct<a:array<struct<a:int,b:double>>,b:double>>
        // case4. add multiple sub-columns : struct<s1:struct<a:int,b:double,c:varchar(10)>,s2:struct<a:array<struct<a:int,b:double,c:varchar(10)>>,b:double,c:varchar(10)>,c:varchar(10)>
        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s_s "
                + "struct<s1:struct<a:int,b:double>,s2:struct<a:array<struct<a:int>>>> " + defaultValue;
        executeAlterAndVerify(alterStmt, tbl,
                "struct<s1:struct<a:int,b:double>,s2:struct<a:array<struct<a:int>>>>", 4, "c_s_s");
        alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s_s "
                + "struct<s1:struct<a:int,b:double>, s2:struct<a:array<struct<a:int>>,b:double>> " + defaultValue;
        executeAlterAndVerify(alterStmt, tbl,
                "struct<s1:struct<a:int,b:double>,s2:struct<a:array<struct<a:int>>,b:double>>", 5, "c_s_s");
        alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s_s "
                + "struct<s1:struct<a:int,b:double>, s2:struct<a:array<struct<a:int, b:double>>,b:double>> "
                + defaultValue;
        executeAlterAndVerify(alterStmt, tbl,
                "struct<s1:struct<a:int,b:double>,s2:struct<a:array<struct<a:int,b:double>>,b:double>>", 6, "c_s_s");
        alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s_s "
                + "struct<s1:struct<a:int,b:double,c:varchar(10)>,s2:struct<a:array<struct<a:int,b:double,c:varchar(10)>>,b:double,c:varchar(10)>,c:varchar(10)> "
                + defaultValue;
        executeAlterAndVerify(alterStmt, tbl,
                "struct<s1:struct<a:int,b:double,c:varchar(10)>,s2:struct<a:array<struct<a:int,b:double,c:varchar(10)>>,b:double,c:varchar(10)>,c:varchar(10)>",
                7, "c_s_s");
    }

    private void testAddMultipleSubColumns(OlapTable tbl, String tableName, String defaultValue) throws Exception {
        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s STRUCT<col:VARCHAR(10), "
                + "col1:INT, col2:DECIMAL(10,2), col3:DATETIME> " + defaultValue;
        executeAlterAndVerify(alterStmt, tbl,
                "struct<col:varchar(10),col1:int,col2:decimalv3(10,2),col3:datetimev2(0)>", 8, "c_s");
    }

    private void testLengthenVarcharSubColumn(OlapTable tbl, String tableName, String defaultValue) throws Exception {
        String alterStmt = "ALTER TABLE test." + tableName
                + " MODIFY COLUMN c_s STRUCT<col:VARCHAR(30),col1:int,col2:decimal(10,2),col3:datetime,col4:string> "
                + defaultValue;
        executeAlterAndVerify(alterStmt, tbl,
                "struct<col:varchar(30),col1:int,col2:decimalv3(10,2),col3:datetimev2(0),col4:text>", 9, "c_s");
    }

    // ------------------------- Negative Test Case -------------------------
    private void testReduceSubColumns(String defaultValue, String tableName) {
        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s STRUCT<col:VARCHAR(10)> "
                + defaultValue;
        expectException(alterStmt, "Cannot reduce struct fields");
    }

    private void testShortenVarcharSubColumn(String defaultValue, String tableName) {
        String alterStmt = "ALTER TABLE test." + tableName
                + " MODIFY COLUMN c_s struct<col:varchar(10),col1:int,col2:decimalv3(10,2),col3:datetimev2(0),col4:string> "
                + defaultValue;
        expectException(alterStmt, "Shorten type length is prohibited");
    }

    private void testChangeStructToOtherType(String defaultValue, String tableName) {
        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s VARCHAR(100) " + defaultValue;
        expectException(alterStmt, "Can not change");
    }

    private void testDuplicateSubColumnName(String defaultValue, String tableName) {
        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN c_s STRUCT<col:VARCHAR(10), col:INT> "
                + defaultValue;
        expectException(alterStmt, "Duplicate field name");
    }

    private void testChangeExistingSubColumnName(String defaultValue, String tableName) {
        String alterStmt = "ALTER TABLE test." + tableName
                + " MODIFY COLUMN c_s struct<col6:varchar(30),"
                + "col1:int,col2:decimalv3(10,2),col3:datetimev2(0),col4:text> "
                + defaultValue;
        expectException(alterStmt, "Cannot rename");
    }

    private void testChangeExistingSubColumnType(String defaultValue, String tableName) {
        String alterStmt = "ALTER TABLE test." + tableName
                + " MODIFY COLUMN c_s struct<col:varchar(30),col1:varchar(10),col2:decimalv3(10,2),col3:datetimev2(0),col4:text> "
                + defaultValue;
        expectException(alterStmt, "Cannot change");
    }

    private void testAddUnsupportedSubColumnType(String defaultValue, String tableName) {
        String alterStmtJson = "ALTER TABLE test." + tableName
                + " MODIFY COLUMN c_s struct<col:varchar(30),col1:varchar(10),col2:decimalv3(10,2),col3:datetimev2(0),col4:text,col5:json> "
                + defaultValue;
        expectException(alterStmtJson, "STRUCT unsupported sub-type");
        String alterStmtVariant = "ALTER TABLE test." + tableName
                + " MODIFY COLUMN c_s struct<col:varchar(30),col1:varchar(10),col2:decimalv3(10,2),col3:datetimev2(0),col4:text,col5:variant> "
                + defaultValue;
        expectException(alterStmtVariant, "STRUCT unsupported sub-type");
    }

    @Test
    public void testModifyStructColumn() throws Exception {
        // loop for all tables to add struct column
        String[] tableNames = {"sc_agg_s", "sc_uniq_s", "sc_dup_s"};
        String[] defaultValues = {"REPLACE_IF_NOT_NULL", "NULL", "NULL"};
        for (int i = 0; i < tableNames.length; i++) {
            String tableName = tableNames[i];
            String defaultVal = defaultValues[i];
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
            OlapTable tbl = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
            // add struct column
            String addValColStmtStr = "alter table test." + tableName + " add column c_s struct<col:varchar(10)> "
                    + defaultVal;
            alterTable(addValColStmtStr, connectContext);
            // check alter job, do not create job
            Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
            jobSize++;
            waitAlterJobDone(alterJobs);
            // add struct column
            // support nested struct can also be support add sub-column
            addValColStmtStr = "alter table test." + tableName
                    + " add column c_s_s struct<s1:struct<a:int>, s2:struct<a:array<struct<a:int>>>> "
                    + defaultVal;
            alterTable(addValColStmtStr, connectContext);
            // check alter job, do not create job
            alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
            jobSize++;
            waitAlterJobDone(alterJobs);

            // positive test
            testAddSingleSubColumn(tbl, tableName, defaultVal);
            testAddNestedStructSubColumn(tbl, tableName, defaultVal);
            testAddMultipleSubColumns(tbl, tableName, defaultVal);
            testLengthenVarcharSubColumn(tbl, tableName, defaultVal);

            // negative test
            testReduceSubColumns(defaultVal, tableName);
            testShortenVarcharSubColumn(defaultVal, tableName);
            testChangeStructToOtherType(defaultVal, tableName);
            testDuplicateSubColumnName(defaultVal, tableName);
            testChangeExistingSubColumnName(defaultVal, tableName);
            testChangeExistingSubColumnType(defaultVal, tableName);
            testAddUnsupportedSubColumnType(defaultVal, tableName);
        }
    }

    @Test
    public void testAddVariantColumnWithLightSchemaChangeDisabled() throws Exception {
        String tableName = "sc_variant_no_lsc";
        dropTable("test." + tableName, false);
        String createTableStmt = "CREATE TABLE test." + tableName + " (\n"
                + "id INT,\n"
                + "name VARCHAR(10)\n"
                + ") DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'false');";
        createTable(createTableStmt);

        String alterStmt = "ALTER TABLE test." + tableName + " ADD COLUMN v VARIANT";
        expectException(alterStmt, "Variant type rely on light schema change");
    }

    @Test
    public void testModifyColumnToVariantWithLightSchemaChangeDisabled() throws Exception {
        String tableName = "sc_variant_modify_no_lsc";
        dropTable("test." + tableName, false);
        String createTableStmt = "CREATE TABLE test." + tableName + " (\n"
                + "id INT,\n"
                + "v VARIANT\n"
                + ") DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true');";
        createTable(createTableStmt);

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
        tbl.writeLock();
        try {
            tbl.setEnableLightSchemaChange(false);
        } finally {
            tbl.writeUnlock();
        }

        String alterStmt = "ALTER TABLE test." + tableName + " MODIFY COLUMN v VARIANT";
        expectException(alterStmt, "Variant type rely on light schema change");
    }

    @Test
    public void testAggAddOrDropColumn() throws Exception {
        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_agg", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
        } finally {
            tbl.readUnlock();
        }

        // process agg add value column schema change
        String addValColStmtStr = "alter table test.sc_agg add column new_v1 int MAX default '0'";
        alterTable(addValColStmtStr, connectContext);
        jobSize++;
        // check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        Assertions.assertEquals(jobSize, alterJobs.size());

        tbl.readLock();
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
            // col_unique_id 0-9
            Assertions.assertEquals(9, indexMeta.getMaxColUniqueId());
        } finally {
            tbl.readUnlock();
        }

        // process agg add key column schema change
        String addKeyColStmtStr = "alter table test.sc_agg add column new_k1 int default '1'";
        alterTable(addKeyColStmtStr, connectContext);

        // check alter job
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(11, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process agg drop value column schema change
        String dropValColStmtStr = "alter table test.sc_agg drop column new_v1";
        alterTable(dropValColStmtStr, connectContext);
        jobSize++;
        // check alter job, do not create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        tbl.readLock();
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        try {
            // process agg drop key column with replace schema change, expect exception.
            String dropKeyColStmtStr = "alter table test.sc_agg drop column new_k1";
            alterTable(dropKeyColStmtStr, connectContext);
            Assert.fail();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }

        LOG.info("getIndexIdToSchema 1: {}", tbl.getIndexIdToSchema(true));

        String addRollUpStmtStr = "alter table test.sc_agg add rollup agg_rollup(user_id, max_dwell_time);";
        alterTable(addRollUpStmtStr, connectContext);
        // 2. check alter job
        Map<Long, AlterJobV2> materializedViewAlterJobs = Env.getCurrentEnv().getMaterializedViewHandler()
                .getAlterJobsV2();
        waitAlterJobDone(materializedViewAlterJobs);
        Assertions.assertEquals(1, materializedViewAlterJobs.size());

        LOG.info("getIndexIdToSchema 2: {}", tbl.getIndexIdToSchema(true));

        // process agg drop value column with rollup schema change
        String dropRollUpValColStmtStr = "alter table test.sc_agg drop column max_dwell_time";
        try {
            alterTable(dropRollUpValColStmtStr, connectContext);
            org.junit.jupiter.api.Assertions.fail();
        } catch (Exception e) {
            LOG.info("{}", e);
        }
        // check alter job, need create job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(materializedViewAlterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process agg add mul value column schema change
        String addMultiValColStmtStr
                = "alter table test.sc_agg add column new_v2 int MAX default '0', add column new_v3 int MAX default '1';";
        alterTable(addMultiValColStmtStr, connectContext);
        jobSize++;
        // check alter job, do not create job
        Assertions.assertEquals(jobSize, alterJobs.size());

        tbl.readLock();
        try {
            Assertions.assertEquals(12, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
            Assertions.assertEquals(12, indexMeta.getMaxColUniqueId());
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testUniqAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_uniq", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
        } finally {
            tbl.readUnlock();
        }

        // process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_uniq add column new_v1 int default '0'";
        alterTable(addValColStmtStr, connectContext);
        jobSize++;
        // check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        tbl.readLock();
        try {
            Assertions.assertEquals(9, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_uniq drop column new_v1";
        alterTable(dropValColStmtStr, connectContext);
        jobSize++;
        // check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        tbl.readLock();
        try {
            Assertions.assertEquals(8, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testDupAddOrDropColumn() throws Exception {

        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_dup", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
        } finally {
            tbl.readUnlock();
        }

        // process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_dup add column new_v1 int default '0'";
        alterTable(addValColStmtStr, connectContext);
        jobSize++;
        // check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        tbl.readLock();
        try {
            Assertions.assertEquals(7, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_dup drop column new_v1";
        alterTable(dropValColStmtStr, connectContext);
        jobSize++;
        // check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        tbl.readLock();
        try {
            Assertions.assertEquals(6, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testAddValueColumnOnAggMV() {
        OlapTable olapTable = Mockito.mock(OlapTable.class);
        Column newColumn = Mockito.mock(Column.class);
        ColumnPosition columnPosition = Mockito.mock(ColumnPosition.class);
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();

        Mockito.when(olapTable.getKeysType()).thenReturn(KeysType.DUP_KEYS);
        Mockito.when(newColumn.getAggregationType()).thenReturn(null);
        MaterializedIndexMeta mockMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.when(olapTable.getIndexMetaByIndexId(2)).thenReturn(mockMeta);
        Mockito.when(mockMeta.getKeysType()).thenReturn(KeysType.AGG_KEYS);
        Mockito.when(newColumn.isKey()).thenReturn(false);

        try {
            Deencapsulation.invoke(schemaChangeHandler, "addColumnInternal", olapTable, newColumn, columnPosition,
                    Long.valueOf(2), Long.valueOf(1), Maps.newHashMap(), Sets.newHashSet(), false, Maps.newHashMap());
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

    @Test
    public void testRowBinlogSchemaChangeKeepsHiddenKeyColumn() throws Exception {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        List<Column> rowBinlogSchema = Lists.newArrayList();
        Column key = new Column("k1", PrimitiveType.INT);
        key.setIsKey(true);
        rowBinlogSchema.add(Column.generateRowBinlogKeyColumn(key));
        rowBinlogSchema.add(new Column(Column.BINLOG_LSN_COL, PrimitiveType.LARGEINT));
        rowBinlogSchema.add(new Column(Column.BINLOG_OPERATION_COL, PrimitiveType.BIGINT));
        rowBinlogSchema.add(new Column(Column.BINLOG_TIMESTAMP_COL, PrimitiveType.BIGINT));

        Column hiddenKey = new Column("__DORIS_TEST_HIDDEN_KEY__", PrimitiveType.BIGINT);
        hiddenKey.setIsKey(true);
        hiddenKey.setIsVisible(false);
        AtomicInteger uniqueId = new AtomicInteger(10);
        IntSupplier uniqueIdSupplier = uniqueId::getAndIncrement;
        Method addColumnRowBinlog = SchemaChangeHandler.class.getDeclaredMethod(
                "addColumnRowBinlog", List.class, Column.class, ColumnPosition.class,
                java.util.Set.class, boolean.class, IntSupplier.class);
        addColumnRowBinlog.setAccessible(true);
        addColumnRowBinlog.invoke(schemaChangeHandler, rowBinlogSchema, hiddenKey, null,
                Sets.newHashSet(hiddenKey.getName()), false, uniqueIdSupplier);

        List<String> columnNames = rowBinlogSchema.stream().map(Column::getName).collect(Collectors.toList());
        Assertions.assertEquals(1, columnNames.indexOf("__DORIS_TEST_HIDDEN_KEY__"));
        Assertions.assertEquals(2, columnNames.indexOf(Column.BINLOG_LSN_COL));

        Column hiddenValue = new Column("__DORIS_TEST_HIDDEN_VALUE__", PrimitiveType.INT);
        hiddenValue.setIsKey(false);
        hiddenValue.setIsVisible(false);
        addColumnRowBinlog.invoke(schemaChangeHandler, rowBinlogSchema, hiddenValue, null,
                Sets.newHashSet(hiddenValue.getName()), false, uniqueIdSupplier);
        columnNames = rowBinlogSchema.stream().map(Column::getName).collect(Collectors.toList());
        Assertions.assertFalse(columnNames.contains("__DORIS_TEST_HIDDEN_VALUE__"));
    }

    @Test
    public void testAggAddOrDropInvertedIndex() throws Exception {
        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_agg", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(0, tbl.getIndexes().size());
        } finally {
            tbl.readUnlock();
        }

        // process agg add inverted index schema change
        String addInvertedIndexStmtStr =
                "alter table test.sc_agg add index idx_city(city) using inverted properties(\"parser\"=\"english\")";
        alterTable(addInvertedIndexStmtStr, connectContext);
        jobSize++;
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(1, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process agg drop inverted index schema change
        String dropInvertedIndexStmtStr = "alter table test.sc_agg drop index idx_city";
        alterTable(dropInvertedIndexStmtStr, connectContext);
        jobSize++;
        // check alter job
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(0, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testUniqAddOrDropInvertedIndex() throws Exception {

        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_uniq", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(0, tbl.getIndexes().size());
        } finally {
            tbl.readUnlock();
        }

        // process uniq add inverted index schema change
        String addInvertedIndexStmtStr =
                "alter table test.sc_uniq add index idx_city(city) using inverted properties(\"parser\"=\"english\")";
        alterTable(addInvertedIndexStmtStr, connectContext);
        jobSize++;
        // check alter job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(1, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process uniq drop inverted indexn schema change
        String dropInvertedIndexStmtStr = "alter table test.sc_uniq drop index idx_city";
        alterTable(dropInvertedIndexStmtStr, connectContext);
        jobSize++;
        // check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(0, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testDupAddOrDropInvertedIndex() throws Exception {

        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_dup", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(0, tbl.getIndexes().size());
        } finally {
            tbl.readUnlock();
        }

        // process dup add inverted index schema change
        String addInvertedIndexStmtStr =
                "alter table test.sc_dup add index idx_error_msg(error_msg) using inverted properties(\"parser\"=\"standard\")";
        alterTable(addInvertedIndexStmtStr, connectContext);
        jobSize++;
        // check dup job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(1, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }

        // process dup drop inverted index schema change
        String dropInvertedIndexStmtStr = "alter table test.sc_dup drop index idx_error_msg";
        alterTable(dropInvertedIndexStmtStr, connectContext);
        jobSize++;
        // check alter job
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(0, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testAddDuplicateInvertedIndexException() throws Exception {

        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_dup", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(0, tbl.getIndexes().size());
        } finally {
            tbl.readUnlock();
        }

        String addInvertedIndexStmtStr = "alter table test.sc_dup add index idx_error_msg(error_msg), "
                + "add index idx_error_msg1(error_msg)";
        try {
            alterTable(addInvertedIndexStmtStr, connectContext);
        } catch (Exception e) {
            // Verify the error message contains relevant info
            Assertions.assertTrue(e.getMessage().contains("INVERTED index for column (error_msg) "
                    + "with analyzer default analyzer already exists"));
        }
        addInvertedIndexStmtStr = "alter table test.sc_dup add index idx_error_msg(error_msg), "
                + "add index idx_error_msg(error_msg)";
        try {
            alterTable(addInvertedIndexStmtStr, connectContext);
        } catch (Exception e) {
            // Verify the error message contains relevant info
            Assertions.assertTrue(e.getMessage().contains("index `idx_error_msg` already exist."));
        }
    }

    private void alterTable(String sql, ConnectContext connectContext) throws Exception {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan parsed = nereidsParser.parseSingle(sql);
        StmtExecutor stmtExecutor = new StmtExecutor(connectContext, sql);
        connectContext.setStatementContext(new StatementContext());
        if (parsed instanceof AlterTableCommand) {
            ((AlterTableCommand) parsed).run(connectContext, stmtExecutor);
        }
    }

    @Test
    public void testDupAddOrDropNgramBfIndex() throws Exception {
        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_dup", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(0, tbl.getIndexes().size());
        } finally {
            tbl.readUnlock();
        }
        String addNgramBfIndexStmtStr = "ALTER TABLE test.sc_dup "
                + "ADD INDEX idx_error_msg(error_msg) USING NGRAM_BF "
                + "PROPERTIES(\"gram_size\"=\"2\", \"bf_size\"=\"256\")";
        alterTable(addNgramBfIndexStmtStr, connectContext);

        jobSize++;
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        LOG.info("alterJobs:{}", alterJobs);
        Assertions.assertEquals(jobSize, alterJobs.size());

        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(1, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);

            Assertions.assertEquals("idx_error_msg", tbl.getIndexes().get(0).getIndexName());
            Assertions.assertEquals(IndexType.NGRAM_BF, tbl.getIndexes().get(0).getIndexType());
            Map<String, String> props = tbl.getIndexes().get(0).getProperties();
            Assertions.assertEquals("2", props.get("gram_size"));
            Assertions.assertEquals("256", props.get("bf_size"));
            Index index = tbl.getIndexes().get(0);
            LOG.warn("index:{}", index.toString());
            Assertions.assertEquals(IndexType.NGRAM_BF, index.getIndexType());
            Assertions.assertTrue(index.toString().contains("USING NGRAM_BF"));
        } finally {
            tbl.readUnlock();
        }

        String dropNgramBfIndexStmtStr = "ALTER TABLE test.sc_dup DROP INDEX idx_error_msg";
        alterTable(dropNgramBfIndexStmtStr, connectContext);
        jobSize++;
        Assertions.assertEquals(jobSize, alterJobs.size());
        waitAlterJobDone(alterJobs);

        tbl.readLock();
        try {
            Assertions.assertEquals(0, tbl.getIndexes().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
        } finally {
            tbl.readUnlock();
        }
    }

    @Test
    public void testAddOrDropSequenceMap() throws Exception {
        LOG.info("dbName: {}", Env.getCurrentInternalCatalog().getDbNames());

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("sc_seq_map", Table.TableType.OLAP);
        tbl.readLock();
        try {
            Assertions.assertNotNull(tbl);
            Assertions.assertEquals("Doris", tbl.getEngine());
            Assertions.assertEquals(0, tbl.getIndexes().size());
        } finally {
            tbl.readUnlock();
        }

        // test should contain PROPERTIES_SEQUENCE_MAPPING properties
        String addColumn = "ALTER TABLE test.sc_seq_map add column s2 bigint";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test value column cannot overlap
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, s3 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'f', 'sequence_mapping.s3' = 'f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test sequence column not exists
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s3' = 'f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test uses key column as sequence column
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.k1' = 'f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test value column not exists
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'h')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test mapping column cannot be sequence column of mapping
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 's1,f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test value column already exists in other sequence groups
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'c,f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test value column cannot be key column
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'k1,f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test add column should be used as sequence column or value column
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar, e varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test sequence column data type
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 varchar, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'f')";
        try {
            alterTable(addColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test add ok
        addColumn = "ALTER TABLE test.sc_seq_map add column (s2 bigint, f varchar) "
                + "PROPERTIES ('sequence_mapping.s2' = 'f')";
        try {
            alterTable(addColumn, connectContext);
            jobSize++;
        } catch (Exception e) {
            Assertions.fail("DdlException should not thrown");
        }

        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);

        // test cannot drop column of none-empty sequence mapping key column
        String dropColumn = "ALTER TABLE test.sc_seq_map drop column s1";
        try {
            alterTable(dropColumn, connectContext);
            Assertions.fail("Expected DdlException was not thrown");
        } catch (Exception e) {
            Assertions.assertNotNull(e.getMessage());
        }

        // test drop ok
        dropColumn = "ALTER TABLE test.sc_seq_map drop column c";
        try {
            alterTable(dropColumn, connectContext);
            jobSize++;
        } catch (Exception e) {
            Assertions.fail("DdlException should not thrown");
        }
        alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        waitAlterJobDone(alterJobs);
    }

}
