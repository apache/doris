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

import org.apache.doris.common.Config;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.sqltest.SqlTestBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests for SHOW CREATE MATERIALIZED VIEW DDL output.
 *
 * PR-4: For IVM (INCREMENTAL) materialized views, the physical model uses
 * UNIQUE_KEYS + Merge-On-Write + hidden row-id columns internally. However,
 * SHOW CREATE should output a logical DDL that can be re-executed by users,
 * hiding internal row-id details:
 *   - No hidden row-id key in KEY(...)
 *   - No hidden IVM columns anywhere in the DDL
 *   - Internal row-id HASH distribution is shown as RANDOM
 *   - No enable_unique_key_merge_on_write property
 *   - REFRESH INCREMENTAL must appear correctly
 */
public class ShowCreateMTMVTest extends SqlTestBase {
    @Override
    protected void runBeforeAll() throws Exception {
        super.runBeforeAll();
        Config.enable_table_stream = true;
        createTable("CREATE TABLE IF NOT EXISTS show_create_ivm_base (\n"
                + "    id bigint,\n"
                + "    score bigint\n"
                + ")\n"
                + "DUPLICATE KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES (\n"
                + "  \"replication_num\" = \"1\",\n"
                + "  \"binlog.enable\" = \"true\",\n"
                + "  \"binlog.format\" = \"ROW\"\n"
                + ")\n");
    }

    // TC-4-1: INCREMENTAL MV with only internal row-id key should not print UNIQUE KEY(...)
    @Test
    public void testShowCreateIncrementalMVNoUniqueKey() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_no_uk "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_no_uk");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertFalse(ddl.contains("UNIQUE KEY"),
                "IVM SHOW CREATE should not contain UNIQUE KEY, but got:\n" + ddl);
    }

    // TC-4-2: INCREMENTAL MV SHOW CREATE must not expose hidden row-id column
    @Test
    public void testShowCreateIncrementalMVNoRowIdColumn() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_no_rowid "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_no_rowid");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "IVM SHOW CREATE should not contain any __DORIS_IVM_ columns, but got:\n" + ddl);
        Assertions.assertTrue(ddl.contains("DISTRIBUTED BY RANDOM BUCKETS AUTO"),
                "IVM SHOW CREATE should show default row-id distribution as RANDOM AUTO, but got:\n" + ddl);
    }

    // TC-4-3: INCREMENTAL MV SHOW CREATE must contain REFRESH INCREMENTAL
    @Test
    public void testShowCreateIncrementalMVContainsRefreshIncremental() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_refresh "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_refresh");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertTrue(ddl.contains("REFRESH INCREMENTAL"),
                "IVM SHOW CREATE should contain 'REFRESH INCREMENTAL', but got:\n" + ddl);
    }

    // TC-4-4: Non-INCREMENTAL MV SHOW CREATE behavior must be unchanged
    @Test
    public void testShowCreateCompleteMVUnchanged() throws Exception {
        createMvByNereids("create materialized view mv_show_complete "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_complete");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertTrue(ddl.contains("REFRESH COMPLETE"),
                "COMPLETE MV SHOW CREATE should contain 'REFRESH COMPLETE', but got:\n" + ddl);
        // Non-IVM MV uses DUP_KEYS without key columns, so no KEY clause output
        // (isDuplicateWithoutKey() returns true when keysNum == 0)
        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "COMPLETE MV should not have any IVM columns:\n" + ddl);
    }

    // TC-4-5: SHOW CREATE output of IVM MV must be replayable (can be re-executed)
    @Test
    public void testShowCreateIncrementalMVIsReplayable() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_replay "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_replay");
        String ddl = Env.getMTMVDdl(mtmv);

        // The DDL should not contain physical details that would cause re-execution to fail.
        Assertions.assertFalse(ddl.contains("enable_unique_key_merge_on_write"),
                "Replayable DDL must not contain MOW property:\n" + ddl);
        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "Replayable DDL must not contain IVM hidden columns:\n" + ddl);
        Assertions.assertTrue(ddl.contains("DISTRIBUTED BY RANDOM BUCKETS AUTO"),
                "Replayable DDL should show row-id distribution as RANDOM AUTO:\n" + ddl);
        Assertions.assertTrue(ddl.contains("REFRESH INCREMENTAL"),
                "Replayable DDL must contain REFRESH INCREMENTAL:\n" + ddl);

        createMvByNereids(ddl.replace("mv_show_ivm_replay", "mv_show_ivm_replay_copy"));
    }

    @Test
    public void testShowCreateIncrementalMVRandomBuckets() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_random_buckets "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 8\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_random_buckets");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "IVM SHOW CREATE should not expose row-id distribution:\n" + ddl);
        Assertions.assertTrue(ddl.contains("DISTRIBUTED BY RANDOM BUCKETS 8"),
                "IVM SHOW CREATE should preserve user RANDOM bucket count:\n" + ddl);
    }

    @Test
    public void testShowCreateIncrementalMVExplicitKeyRandomBucketsReplayable() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_key_random_buckets "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "KEY(id)\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 8\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_key_random_buckets");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "IVM SHOW CREATE should not expose row-id distribution:\n" + ddl);
        Assertions.assertTrue(ddl.contains("KEY(`id`)"),
                "IVM SHOW CREATE should keep the user-visible key:\n" + ddl);
        Assertions.assertTrue(ddl.contains("DISTRIBUTED BY RANDOM BUCKETS 8"),
                "IVM SHOW CREATE should preserve user RANDOM bucket count:\n" + ddl);

        createMvByNereids(ddl.replace("mv_show_ivm_key_random_buckets",
                "mv_show_ivm_key_random_buckets_replay"));
    }

    // TC-4-6: Even with show_hidden_columns=true, IVM row-id should not be exposed in SHOW CREATE
    @Test
    public void testShowCreateIncrementalMVNoRowIdEvenWithShowHidden() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_hidden "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_hidden");

        // getMTMVDdl uses getBaseSchema() (visible only) and our filter,
        // so show_hidden_columns session variable should not affect the output
        boolean originalShowHidden = connectContext.getSessionVariable().showHiddenColumns();
        try {
            connectContext.getSessionVariable().setShowHiddenColumns(true);
            String ddl = Env.getMTMVDdl(mtmv);

            Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                    "Even with show_hidden_columns=true, IVM hidden columns should not be exposed:\n" + ddl);
            Assertions.assertFalse(ddl.contains("UNIQUE KEY"),
                    "Even with show_hidden_columns=true, UNIQUE KEY should be hidden:\n" + ddl);
        } finally {
            connectContext.getSessionVariable().setShowHiddenColumns(originalShowHidden);
        }
    }

    // TC-4-7: DUP_KEYS MV (non-IVM) preserves its normal KEY clause in SHOW CREATE
    @Test
    public void testShowCreateDupKeysMVNoKeyOutput() throws Exception {
        createMvByNereids("create materialized view mv_show_dup_keys "
                + "BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_dup_keys");
        String ddl = Env.getMTMVDdl(mtmv);

        // Non-IVM COMPLETE MV uses DUP_KEYS and may output DUPLICATE KEY(...)
        // The important thing is it should NOT have UNIQUE KEY or MOW properties
        Assertions.assertFalse(ddl.contains("UNIQUE KEY"),
                "DUP_KEYS MV should not contain UNIQUE KEY:\n" + ddl);
        Assertions.assertFalse(ddl.contains("enable_unique_key_merge_on_write"),
                "DUP_KEYS MV should not contain MOW property:\n" + ddl);
        // Should contain DUPLICATE KEY if it has key columns
        if (ddl.contains("DUPLICATE KEY")) {
            // Verify the key columns are from the query, not IVM internal columns
            Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                    "DUP_KEYS MV key clause should not reference IVM columns:\n" + ddl);
        }
    }

    // TC-4-8: SHOW CREATE roundtrip - re-create IVM MV from DDL and verify identical output
    @Test
    public void testShowCreateIvmRoundtrip() throws Exception {
        // Step 1: create an IVM MV with an explicit user distribution.
        createMvByNereids("create materialized view mv_show_ivm_roundtrip_1 "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "KEY(id)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 3\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.show_create_ivm_base;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv1 = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_roundtrip_1");
        String ddl1 = Env.getMTMVDdl(mtmv1);

        // Step 2: verify DDL preserves user-visible key/distribution without exposing row-id.
        Assertions.assertFalse(ddl1.contains("__DORIS_IVM_"),
                "DDL should not expose IVM hidden columns, but got:\n" + ddl1);
        Assertions.assertTrue(ddl1.contains("KEY(`id`)"),
                "DDL should contain visible key columns without row-id:\n" + ddl1);
        Assertions.assertTrue(ddl1.contains("DISTRIBUTED BY HASH(`id`) BUCKETS 3"),
                "DDL should preserve user HASH distribution:\n" + ddl1);

        // Step 3: use DDL to create a second MV (replace the name).
        String ddl2Sql = ddl1.replace("mv_show_ivm_roundtrip_1", "mv_show_ivm_roundtrip_2");
        createMvByNereids(ddl2Sql);

        MTMV mtmv2 = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_roundtrip_2");
        String ddl2 = Env.getMTMVDdl(mtmv2);

        // Step 4: verify structural part of SHOW CREATE is identical (everything before "AS select").
        // The query SQL may differ slightly (e.g. "select *" expands to explicit columns with aliases),
        // but the structure (columns, refresh, distribution, properties) must match exactly.
        String struct1 = ddl1.substring(0, ddl1.indexOf("\nAS "))
                .replace("mv_show_ivm_roundtrip_1", "MV_NAME");
        String struct2 = ddl2.substring(0, ddl2.indexOf("\nAS "))
                .replace("mv_show_ivm_roundtrip_2", "MV_NAME");
        Assertions.assertEquals(struct1, struct2,
                "Structural part of SHOW CREATE should match after roundtrip.\n"
                + "DDL1:\n" + ddl1 + "\nDDL2:\n" + ddl2);

        // Step 5: second roundtrip should be a true fixpoint (DDL from MV2 reproduces itself).
        String ddl3Sql = ddl2.replace("mv_show_ivm_roundtrip_2", "mv_show_ivm_roundtrip_3");
        createMvByNereids(ddl3Sql);
        MTMV mtmv3 = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_roundtrip_3");
        String ddl3 = Env.getMTMVDdl(mtmv3);
        String normalized2 = ddl2.replace("mv_show_ivm_roundtrip_2", "MV_NAME");
        String normalized3 = ddl3.replace("mv_show_ivm_roundtrip_3", "MV_NAME");
        Assertions.assertEquals(normalized2, normalized3,
                "Second roundtrip should be a perfect fixpoint.\n"
                + "DDL2:\n" + ddl2 + "\nDDL3:\n" + ddl3);

        // Step 6: verify schema columns match (excluding hidden IVM columns).
        List<String> cols1 = mtmv1.getBaseSchema().stream()
                .map(Column::getName)
                .filter(n -> !IvmUtil.isIvmHiddenColumn(n))
                .collect(Collectors.toList());
        List<String> cols2 = mtmv2.getBaseSchema().stream()
                .map(Column::getName)
                .filter(n -> !IvmUtil.isIvmHiddenColumn(n))
                .collect(Collectors.toList());
        Assertions.assertEquals(cols1, cols2,
                "Visible schema columns should match after roundtrip");

        // Step 7: all should have HASH distribution internally.
        Assertions.assertEquals(DistributionInfo.DistributionInfoType.HASH,
                mtmv1.getDefaultDistributionInfo().getType(),
                "MV1 internal distribution should be HASH");
        Assertions.assertEquals(DistributionInfo.DistributionInfoType.HASH,
                mtmv2.getDefaultDistributionInfo().getType(),
                "MV2 internal distribution should be HASH");
        Assertions.assertEquals(DistributionInfo.DistributionInfoType.HASH,
                mtmv3.getDefaultDistributionInfo().getType(),
                "MV3 internal distribution should be HASH");
    }
}
