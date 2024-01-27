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

import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
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
        //create database db1
        createDatabase("test");

        //create tables
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

        //process agg add value column schema change
        String addValColStmtStr = "alter table test.sc_agg add column new_v1 int MAX default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
        Map<Long, AlterJobV2> alterJobs = Env.getCurrentEnv().getSchemaChangeHandler().getAlterJobsV2();
        Assertions.assertEquals(jobSize, alterJobs.size());

        tbl.readLock();
        try {
            Assertions.assertEquals(10, tbl.getBaseSchema().size());
            String baseIndexName = tbl.getIndexNameById(tbl.getBaseIndexId());
            Assertions.assertEquals(baseIndexName, tbl.getName());
            MaterializedIndexMeta indexMeta = tbl.getIndexMetaByIndexId(tbl.getBaseIndexId());
            Assertions.assertNotNull(indexMeta);
            //col_unique_id 0-9
            Assertions.assertEquals(9, indexMeta.getMaxColUniqueId());
        } finally {
            tbl.readUnlock();
        }

        //process agg add  key column schema change
        String addKeyColStmtStr = "alter table test.sc_agg add column new_k1 int default '1'";
        AlterTableStmt addKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addKeyColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addKeyColStmt);

        //check alter job
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

        //process agg drop value column schema change
        String dropValColStmtStr = "alter table test.sc_agg drop column new_v1";
        AlterTableStmt dropValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(dropValColStmt);
        jobSize++;
        //check alter job, do not create job
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
            //process agg drop key column with replace schema change, expect exception.
            String dropKeyColStmtStr = "alter table test.sc_agg drop column new_k1";
            AlterTableStmt dropKeyColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropKeyColStmtStr);
            Env.getCurrentEnv().getAlterInstance().processAlterTable(dropKeyColStmt);
            Assert.fail();
        } catch (Exception e) {
            LOG.info(e.getMessage());
        }

        LOG.info("getIndexIdToSchema 1: {}", tbl.getIndexIdToSchema(true));

        String addRollUpStmtStr = "alter table test.sc_agg add rollup agg_rollup(user_id, max_dwell_time);";
        AlterTableStmt addRollUpStmt = (AlterTableStmt) parseAndAnalyzeStmt(addRollUpStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addRollUpStmt);
        // 2. check alter job
        Map<Long, AlterJobV2> materializedViewAlterJobs = Env.getCurrentEnv().getMaterializedViewHandler()
                .getAlterJobsV2();
        waitAlterJobDone(materializedViewAlterJobs);
        Assertions.assertEquals(1, materializedViewAlterJobs.size());

        LOG.info("getIndexIdToSchema 2: {}", tbl.getIndexIdToSchema(true));

        //process agg drop value column with rollup schema change
        String dropRollUpValColStmtStr = "alter table test.sc_agg drop column max_dwell_time";
        AlterTableStmt dropRollUpValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropRollUpValColStmtStr);
        try {
            Env.getCurrentEnv().getAlterInstance().processAlterTable(dropRollUpValColStmt);
            Assertions.assertTrue(false);
        } catch (Exception e) {
            LOG.info("{}", e);
        }
        //check alter job, need create job
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

        //process agg add mul value column schema change
        String addMultiValColStmtStr
                = "alter table test.sc_agg add column new_v2 int MAX default '0', add column new_v3 int MAX default '1';";
        AlterTableStmt addMultiValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addMultiValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addMultiValColStmt);
        jobSize++;
        //check alter job, do not create job
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

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_uniq add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
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

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_uniq drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
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

        //process uniq add value column schema change
        String addValColStmtStr = "alter table test.sc_dup add column new_v1 int default '0'";
        AlterTableStmt addValColStmt = (AlterTableStmt) parseAndAnalyzeStmt(addValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addValColStmt);
        jobSize++;
        //check alter job, do not create job
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

        //process uniq drop val column schema change
        String dropValColStmtStr = "alter table test.sc_dup drop column new_v1";
        AlterTableStmt dropValColStm = (AlterTableStmt) parseAndAnalyzeStmt(dropValColStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(dropValColStm);
        jobSize++;
        //check alter job
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
                    new Long(2), new Long(1), Maps.newHashMap(), Sets.newHashSet(), false, Maps.newHashMap());
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

        //process agg add inverted index schema change
        String addInvertedIndexStmtStr = "alter table test.sc_agg add index idx_city(city) using inverted properties(\"parser\"=\"english\")";
        AlterTableStmt addInvertedIndexStmt = (AlterTableStmt) parseAndAnalyzeStmt(addInvertedIndexStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addInvertedIndexStmt);
        jobSize++;
        //check alter job
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

        //process agg drop inverted index schema change
        String dropInvertedIndexStmtStr = "alter table test.sc_agg drop index idx_city";
        AlterTableStmt dropInvertedIndexStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropInvertedIndexStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(dropInvertedIndexStmt);
        jobSize++;
        //check alter job
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

        //process uniq add inverted index schema change
        String addInvertedIndexStmtStr = "alter table test.sc_uniq add index idx_city(city) using inverted properties(\"parser\"=\"english\")";
        AlterTableStmt addInvertedIndexStmt = (AlterTableStmt) parseAndAnalyzeStmt(addInvertedIndexStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addInvertedIndexStmt);
        jobSize++;
        //check alter job
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

        //process uniq drop inverted indexn schema change
        String dropInvertedIndexStmtStr = "alter table test.sc_uniq drop index idx_city";
        AlterTableStmt dropInvertedIndexStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropInvertedIndexStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(dropInvertedIndexStmt);
        jobSize++;
        //check alter job
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

        //process dup add inverted index schema change
        String addInvertedIndexStmtStr = "alter table test.sc_dup add index idx_error_msg(error_msg) using inverted properties(\"parser\"=\"standard\")";
        AlterTableStmt addInvertedIndexStmt = (AlterTableStmt) parseAndAnalyzeStmt(addInvertedIndexStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(addInvertedIndexStmt);
        jobSize++;
        //check dup job
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

        //process dup drop inverted index schema change
        String dropInvertedIndexStmtStr = "alter table test.sc_dup drop index idx_error_msg";
        AlterTableStmt dropInvertedIndexStmt = (AlterTableStmt) parseAndAnalyzeStmt(dropInvertedIndexStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterTable(dropInvertedIndexStmt);
        jobSize++;
        //check alter job
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
