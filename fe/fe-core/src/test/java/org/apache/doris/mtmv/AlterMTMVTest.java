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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.mtmv.MTMVRefreshEnum.RefreshMethod;
import org.apache.doris.mtmv.ivm.IvmInfo;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;


public class AlterMTMVTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_table_stream = true;
    }

    @Test
    public void testAlterMTMV() throws Exception {
        createDatabaseAndUse("test");

        createTable("CREATE TABLE `stu` (`sid` int(32) NULL, `sname` varchar(32) NULL)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`sid`)\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1')");

        createMvByNereids("CREATE MATERIALIZED VIEW mv_a BUILD DEFERRED REFRESH COMPLETE ON COMMIT\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1') "
                + "AS select * from stu limit 1");

        alterMv("ALTER MATERIALIZED VIEW mv_a RENAME mv_b");
        alterMv("ALTER MATERIALIZED VIEW test.mv_b RENAME test.mv_c");

        MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
        Table table = Env.getCurrentInternalCatalog().getDb("test").get().getTableOrMetaException("stu");
        Set<BaseTableInfo> allMTMVs = relationManager.getMtmvsByBaseTable(new BaseTableInfo(table));
        boolean hasMvA = false;
        boolean hasMvB = false;
        boolean hasMvC = false;
        for (BaseTableInfo mtmv : allMTMVs) {
            if ("mv_a".equals(mtmv.getTableName())) {
                hasMvA = true;
            }
            if ("mv_b".equals(mtmv.getTableName())) {
                hasMvB = true;
            }
            if ("mv_c".equals(mtmv.getTableName())) {
                hasMvC = true;
            }
        }
        Assertions.assertFalse(hasMvA);
        Assertions.assertFalse(hasMvB);
        Assertions.assertTrue(hasMvC);


        createTable("CREATE TABLE `stu1` (`sid` int(32) NULL, `sname` varchar(32) NULL)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`sid`)\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1')");

        DdlException exception = Assertions.assertThrows(DdlException.class, () ->
                alterTableSync("ALTER TABLE stu1 REPLACE WITH TABLE mv_c PROPERTIES('swap' = 'true')"));
        Assertions.assertEquals("errCode = 2, detailMessage = replace table[mv_c] cannot be a materialized view",
                exception.getMessage());

        createDatabaseAndUse("Test");
        createTable("CREATE TABLE `case_stu` (`sid` int(32) NULL, `sname` varchar(32) NULL)\n"
                + "ENGINE=OLAP\n"
                + "DUPLICATE KEY(`sid`)\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1')");
        createMvByNereids("CREATE MATERIALIZED VIEW mv_case BUILD DEFERRED REFRESH COMPLETE ON COMMIT\n"
                + "DISTRIBUTED BY HASH(`sid`) BUCKETS 1\n"
                + "PROPERTIES ('replication_allocation' = 'tag.location.default: 1') "
                + "AS select * from case_stu limit 1");

        AnalysisException renameException = Assertions.assertThrows(AnalysisException.class, () ->
                alterMv("ALTER MATERIALIZED VIEW Test.mv_case RENAME test.mv_case_new"));
        Assertions.assertEquals("Can not rename materialized view to another database or catalog",
                renameException.getMessage());
    }

    // --- P0-3: Block ALTER to/from INCREMENTAL refresh method ---

    @Test
    public void testAlterFromCompleteToIncrementalRejected() throws Exception {
        createDatabaseAndUse("alter_test");
        createTable("CREATE TABLE alter_test.alt_base (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_complete_mv\n"
                + " BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base");
        Exception ex = Assertions.assertThrows(Exception.class,
                () -> alterMv("ALTER MATERIALIZED VIEW alt_complete_mv\n"
                        + " REFRESH INCREMENTAL ON MANUAL"));
        Assertions.assertTrue(ex.getMessage().contains("Cannot ALTER refresh method to INCREMENTAL"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testAlterFromIncrementalToCompleteRejected() throws Exception {
        createDatabaseAndUse("alter_test2");
        createTable("CREATE TABLE alter_test2.alt_base2 (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_incr_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base2");
        Exception ex = Assertions.assertThrows(Exception.class,
                () -> alterMv("ALTER MATERIALIZED VIEW alt_incr_mv\n"
                        + " REFRESH COMPLETE ON MANUAL"));
        Assertions.assertTrue(ex.getMessage().contains("Cannot ALTER the refresh method of an INCREMENTAL"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testAlterFromIncrementalToAutoRejected() throws Exception {
        createDatabaseAndUse("alter_test3");
        createTable("CREATE TABLE alter_test3.alt_base3 (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_incr_mv3\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base3");
        Exception ex = Assertions.assertThrows(Exception.class,
                () -> alterMv("ALTER MATERIALIZED VIEW alt_incr_mv3\n"
                        + " REFRESH AUTO ON MANUAL"));
        Assertions.assertTrue(ex.getMessage().contains("Cannot ALTER the refresh method of an INCREMENTAL"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testAlterFromCompleteToAutoAllowed() throws Exception {
        createDatabaseAndUse("alter_test4");
        createTable("CREATE TABLE alter_test4.alt_base4 (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_complete_mv4\n"
                + " BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base4");
        // Should not throw
        alterMv("ALTER MATERIALIZED VIEW alt_complete_mv4\n"
                + " REFRESH AUTO ON MANUAL");
    }

    @Test
    public void testAlterFromAutoToCompleteAllowed() throws Exception {
        createDatabaseAndUse("alter_test_auto_complete");
        createTable("CREATE TABLE alter_test_auto_complete.alt_base (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_auto_mv_complete\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base");
        alterMv("ALTER MATERIALIZED VIEW alt_auto_mv_complete\n"
                + " REFRESH COMPLETE ON MANUAL");
    }

    @Test
    public void testAlterFromAutoToIncrementalRejected() throws Exception {
        createDatabaseAndUse("alter_test5");
        createTable("CREATE TABLE alter_test5.alt_base5 (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_auto_mv5\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base5");
        Exception ex = Assertions.assertThrows(Exception.class,
                () -> alterMv("ALTER MATERIALIZED VIEW alt_auto_mv5\n"
                        + " REFRESH INCREMENTAL ON MANUAL"));
        Assertions.assertTrue(ex.getMessage().contains("Cannot ALTER refresh method to INCREMENTAL"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testAlterAutoFallbackPersistsFallbackFlag() throws Exception {
        createDatabaseAndUse("alter_test_auto_fallback");
        createTable("CREATE TABLE alter_test_auto_fallback.alt_base (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_auto_fallback_mv\n"
                + " BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base");

        alterMv("ALTER MATERIALIZED VIEW alt_auto_fallback_mv REFRESH AUTO FALLBACK ON MANUAL");

        MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog()
                .getDb("alter_test_auto_fallback").get()
                .getTableOrMetaException("alt_auto_fallback_mv");
        Assertions.assertEquals(RefreshMethod.AUTO, mtmv.getRefreshInfo().getRefreshMethod());
        Assertions.assertTrue(mtmv.getRefreshInfo().allowFallback());
    }

    @Test
    public void testAlterNonPartitionMvToPartitionsFallbackRejected() throws Exception {
        createDatabaseAndUse("alter_test_partitions_fallback");
        createTable("CREATE TABLE alter_test_partitions_fallback.alt_base (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')");
        createMvByNereids("CREATE MATERIALIZED VIEW alt_partitions_fallback_mv\n"
                + " BUILD DEFERRED REFRESH AUTO ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM alt_base");

        Exception ex = Assertions.assertThrows(Exception.class,
                () -> alterMv("ALTER MATERIALIZED VIEW alt_partitions_fallback_mv "
                        + "REFRESH PARTITIONS FALLBACK ON MANUAL"));
        Assertions.assertTrue(ex.getMessage().contains("Cannot ALTER refresh method to PARTITIONS"),
                "unexpected message: " + ex.getMessage());
    }

    @Test
    public void testAlterIvmInfoPersistence() throws Exception {
        createDatabaseAndUse("alter_ivm_test");
        createTable("CREATE TABLE alter_ivm_test.ivm_base (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_alter_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM ivm_base");

        MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog()
                .getDb("alter_ivm_test").get()
                .getTableOrMetaException("ivm_alter_mv");

        // Incremental MTMV persists plan signature at create time.
        IvmInfo initialInfo = mtmv.getIvmInfo();
        Assertions.assertNotNull(initialInfo.getPlanSignature());

        // Build a modified IvmInfo with planSignature
        IvmInfo newInfo = new IvmInfo();
        newInfo.setPlanSignature("sig-1");

        // Persist via alterMTMVIvmInfo
        TableNameInfo tableName = new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName());
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, newInfo);

        // Verify the MTMV's IvmInfo was updated
        IvmInfo updatedInfo = mtmv.getIvmInfo();
        Assertions.assertEquals("sig-1", updatedInfo.getPlanSignature());

        // Reset it back and verify
        IvmInfo resetInfo = new IvmInfo();
        resetInfo.setPlanSignature("sig-2");
        Env.getCurrentEnv().alterMTMVIvmInfo(tableName, resetInfo);

        IvmInfo finalInfo = mtmv.getIvmInfo();
        Assertions.assertEquals("sig-2", finalInfo.getPlanSignature());
    }

    @Test
    public void testCreateIncrementalMtmvAutoCreatesStream() throws Exception {
        createDatabaseAndUse("stream_test");
        createTable("CREATE TABLE stream_test.stream_base (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',"
                + " 'binlog.enable' = 'true', 'binlog.need_historical_value' = 'true',"
                + " 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1')\n"
                + " AS SELECT k1, v1 FROM stream_base");

        // Verify the auto-created stream exists
        MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog()
                .getDb("stream_test").get()
                .getTableOrMetaException("stream_mv");
        String streamName = IvmUtil.streamName(mtmv.getId(), "stream_base");
        org.apache.doris.catalog.TableIf streamTable = Env.getCurrentInternalCatalog()
                .getDb("stream_test").get()
                .getTableOrMetaException(streamName);
        Assertions.assertNotNull(streamTable, "Stream should be auto-created for IVM base table");
        Assertions.assertTrue(streamTable instanceof org.apache.doris.catalog.stream.OlapTableStream,
                "Should be an OlapTableStream");
    }

    @Test
    public void testCreateIncrementalMtmvExcludeTriggerTableSkipsStream() throws Exception {
        createDatabaseAndUse("stream_excl_test");
        createTable("CREATE TABLE stream_excl_test.excl_base1 (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',"
                + " 'binlog.enable' = 'true', 'binlog.need_historical_value' = 'true',"
                + " 'binlog.format' = 'ROW')");
        createTable("CREATE TABLE stream_excl_test.excl_base2 (k1 int, v1 int)\n"
                + "UNIQUE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true',"
                + " 'binlog.enable' = 'true', 'binlog.need_historical_value' = 'true',"
                + " 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW excl_stream_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1',\n"
                + "   'excluded_trigger_tables' = 'excl_base1')\n"
                + " AS SELECT k1, v1 FROM excl_base1 UNION ALL SELECT k1, v1 FROM excl_base2");

        MTMV mtmv = (MTMV) Env.getCurrentInternalCatalog()
                .getDb("stream_excl_test").get()
                .getTableOrMetaException("excl_stream_mv");

        // excl_base1 is excluded → no stream should be created
        String excludedStreamName = IvmUtil.streamName(mtmv.getId(), "excl_base1");
        Assertions.assertThrows(Exception.class,
                () -> Env.getCurrentInternalCatalog()
                        .getDb("stream_excl_test").get()
                        .getTableOrMetaException(excludedStreamName),
                "Excluded table should NOT have a stream auto-created");

        // excl_base2 is NOT excluded → stream should exist
        String includedStreamName = IvmUtil.streamName(mtmv.getId(), "excl_base2");
        org.apache.doris.catalog.TableIf includedStream = Env.getCurrentInternalCatalog()
                .getDb("stream_excl_test").get()
                .getTableOrMetaException(includedStreamName);
        Assertions.assertNotNull(includedStream, "Non-excluded table should have a stream auto-created");
    }

    @Test
    public void testAlterIvmExcludedTriggerTablesCanOnlyExpandScope() throws Exception {
        createDatabaseAndUse("alter_ivm_excluded_trigger_test");
        createTable("CREATE TABLE alter_ivm_excluded_trigger_test.ivm_base (k1 int, v1 int)\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true', 'binlog.format' = 'ROW')");
        createMvByNereids("CREATE MATERIALIZED VIEW ivm_excluded_mv\n"
                + " BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + " DISTRIBUTED BY RANDOM BUCKETS 2\n"
                + " PROPERTIES ('replication_num' = '1',"
                + " 'excluded_trigger_tables' = 'alter_ivm_excluded_trigger_test.ivm_base')\n"
                + " AS SELECT k1, v1 FROM ivm_base");

        alterMv("ALTER MATERIALIZED VIEW ivm_excluded_mv\n"
                + " SET ('excluded_trigger_tables' = 'ivm_base')");

        Exception ex = Assertions.assertThrows(Exception.class,
                () -> alterMv("ALTER MATERIALIZED VIEW ivm_excluded_mv\n"
                        + " SET ('excluded_trigger_tables' = 'alter_ivm_excluded_trigger_test.ivm_base')"));
        Assertions.assertTrue(ex.getMessage().contains("Existing excluded trigger tables can only be expanded"),
                "unexpected message: " + ex.getMessage());
    }
}
