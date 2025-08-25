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

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.IndexDef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

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
    public void testAddValueColumnOnAggMV(@Injectable OlapTable olapTable, @Injectable Column newColumn,
            @Injectable ColumnPosition columnPosition) {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        new Expectations() {
            {
                olapTable.getKeysType();
                result = KeysType.DUP_KEYS;
                newColumn.getAggregationType();
                result = null;
                olapTable.getIndexMetaByIndexId(2).getKeysType();
                result = KeysType.AGG_KEYS;
                newColumn.isKey();
                result = false;
            }
        };

        try {
            Deencapsulation.invoke(schemaChangeHandler, "addColumnInternal", olapTable, newColumn, columnPosition,
                    Long.valueOf(2), Long.valueOf(1), Maps.newHashMap(), Sets.newHashSet(), false, Maps.newHashMap());
            Assert.fail();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

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
                    + "with non-analyzed type already exists"));
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
            Assertions.assertEquals(IndexDef.IndexType.NGRAM_BF, tbl.getIndexes().get(0).getIndexType());
            Map<String, String> props = tbl.getIndexes().get(0).getProperties();
            Assertions.assertEquals("2", props.get("gram_size"));
            Assertions.assertEquals("256", props.get("bf_size"));
            Index index = tbl.getIndexes().get(0);
            LOG.warn("index:{}", index.toString());
            Assertions.assertEquals(IndexDef.IndexType.NGRAM_BF, index.getIndexType());
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

}
