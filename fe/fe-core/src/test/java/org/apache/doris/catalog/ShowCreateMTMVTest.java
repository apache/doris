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

import org.apache.doris.nereids.sqltest.SqlTestBase;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for SHOW CREATE MATERIALIZED VIEW DDL output.
 *
 * PR-4: For IVM (INCREMENTAL) materialized views, the physical model uses
 * UNIQUE_KEYS + Merge-On-Write + hidden row-id columns internally. However,
 * SHOW CREATE should output a logical DDL that can be re-executed by users,
 * hiding all internal physical details:
 *   - No UNIQUE KEY(...) clause
 *   - No hidden IVM columns (e.g., __DORIS_IVM_ROW_ID_COL__)
 *   - No enable_unique_key_merge_on_write property
 *   - REFRESH INCREMENTAL must appear correctly
 */
public class ShowCreateMTMVTest extends SqlTestBase {

    // TC-4-1: INCREMENTAL MV SHOW CREATE must not contain UNIQUE KEY(...)
    @Test
    public void testShowCreateIncrementalMVNoUniqueKey() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_no_uk "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.T4;");

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
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_no_rowid");
        String ddl = Env.getMTMVDdl(mtmv);

        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "IVM SHOW CREATE should not contain any __DORIS_IVM_ columns, but got:\n" + ddl);
    }

    // TC-4-3: INCREMENTAL MV SHOW CREATE must contain REFRESH INCREMENTAL
    @Test
    public void testShowCreateIncrementalMVContainsRefreshIncremental() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_refresh "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.T4;");

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
                + "as select * from test.T4;");

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
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_replay");
        String ddl = Env.getMTMVDdl(mtmv);

        // The DDL should not contain physical details that would cause re-execution to fail
        Assertions.assertFalse(ddl.contains("UNIQUE KEY"),
                "Replayable DDL must not contain UNIQUE KEY:\n" + ddl);
        Assertions.assertFalse(ddl.contains("enable_unique_key_merge_on_write"),
                "Replayable DDL must not contain MOW property:\n" + ddl);
        Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                "Replayable DDL must not contain IVM hidden columns:\n" + ddl);
        Assertions.assertTrue(ddl.contains("REFRESH INCREMENTAL"),
                "Replayable DDL must contain REFRESH INCREMENTAL:\n" + ddl);
    }

    // TC-4-6: Even with show_hidden_columns=true, IVM row-id should not be exposed in SHOW CREATE
    @Test
    public void testShowCreateIncrementalMVNoRowIdEvenWithShowHidden() throws Exception {
        createMvByNereids("create materialized view mv_show_ivm_hidden "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "as select * from test.T4;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("mv_show_ivm_hidden");

        // getMTMVDdl uses getBaseSchema() (visible only) and our filter,
        // so show_hidden_columns session variable should not affect the output
        boolean originalShowHidden = connectContext.getSessionVariable().showHiddenColumns();
        try {
            connectContext.getSessionVariable().setShowHiddenColumns(true);
            String ddl = Env.getMTMVDdl(mtmv);

            Assertions.assertFalse(ddl.contains("__DORIS_IVM_"),
                    "Even with show_hidden_columns=true, IVM columns should be hidden:\n" + ddl);
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
                + "as select * from test.T4;");

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
}
