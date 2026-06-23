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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.util.UnitTestUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class DropMaterializedViewTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(UnitTestUtil.DB_NAME);
        createTable(String.format("CREATE TABLE %s.%s(k1 int, k2 bigint) DUPLICATE KEY(k1) DISTRIBUTED BY "
                + "HASH(k2) BUCKETS 1 PROPERTIES('replication_num' = '1');",
                UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME));
        createMvByNereids(String.format("CREATE MATERIALIZED VIEW %s.%s BUILD IMMEDIATE REFRESH AUTO ON MANUAL "
                + "DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES ('replication_num' = '1') AS SELECT k1, sum(k2) as k3 from %s.%s"
                + " GROUP BY k1;",
                UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.TABLE_NAME));
    }

    private static void dropTable(String db, String tbl, boolean isMaterializedView) throws Exception {
        Env.getCurrentEnv().dropTable(
                InternalCatalog.INTERNAL_CATALOG_NAME,
                db,
                tbl,
                false,
                isMaterializedView,
                false,
                false,
                false,
                false);
    }

    @Test
    public void testDropMv() throws Exception {
        ExceptionChecker.expectThrowsWithMsg(DdlException.class,
                String.format("'%s.%s' is not TABLE. Use 'DROP MATERIALIZED VIEW %s.%s'",
                    UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME, UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME),
                () -> dropTable(UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME, false));
        ExceptionChecker.expectThrowsNoException(() -> dropMvByNereids(String.format("DROP MATERIALIZED VIEW %s.%s",
                UnitTestUtil.DB_NAME, UnitTestUtil.MV_NAME)));
    }

    @Test
    public void testDropIvmMtmvRemovesStreams() throws Exception {
        Config.enable_table_stream = true;
        String db = UnitTestUtil.DB_NAME;
        String baseTable = "ivm_drop_test_base";

        createTable(String.format("CREATE TABLE %s.%s (k1 int, v1 int) "
                + "UNIQUE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES ('replication_num' = '1', 'enable_unique_key_merge_on_write' = 'true', "
                + "'binlog.enable' = 'true', 'binlog.format' = 'ROW', "
                + "'binlog.need_historical_value' = 'true');", db, baseTable));

        String mvName = "ivm_drop_test_mv";
        createMvByNereids(String.format("CREATE MATERIALIZED VIEW %s.%s "
                + "BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL "
                + "DISTRIBUTED BY RANDOM BUCKETS 2 PROPERTIES ('replication_num' = '1') "
                + "AS SELECT k1, v1 FROM %s.%s;", db, mvName, db, baseTable));
        Thread.sleep(1000);

        Database database = Env.getCurrentInternalCatalog().getDbOrDdlException(db);
        MTMV mtmv = (MTMV) database.getTableOrDdlException(mvName);
        Assertions.assertTrue(mtmv.isIvm());
        String streamName = IvmUtil.streamName(mtmv.getId(), baseTable);
        Assertions.assertNotNull(database.getTableNullable(streamName),
                "Stream should be created for IVM MTMV");

        // Drop MV should also remove associated streams
        dropMvByNereids(String.format("DROP MATERIALIZED VIEW %s.%s", db, mvName));
        Assertions.assertNull(database.getTableNullable(streamName),
                "Stream should be removed after MTMV drop");
    }
}
