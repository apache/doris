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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MTMVSchemaChangeTest extends TestWithFeService {
    // t1 => v1 => mv1
    @Test
    public void testDropBaseView() throws Exception {
        createDatabaseAndUse("db1");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createView("create view v1 as select * from t1");
        createMvByNereids("create materialized view mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from v1 ;");
        Database db1 = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("db1");
        MTMV mtmv = (MTMV) db1.getTableOrAnalysisException("mv1");
        MTMVRefreshSnapshot mtmvRefreshSnapshot = new MTMVRefreshSnapshot();
        mtmv.setRefreshSnapshot(mtmvRefreshSnapshot);
        Assertions.assertTrue(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 0);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.INIT));
        dropView("drop view v1");
        Assertions.assertFalse(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 1);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.SCHEMA_CHANGE));
    }

    @Test
    public void testDropBaseTable() throws Exception {
        createDatabaseAndUse("db2");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id varchar(10),\n"
                        + "    score String\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\"\n"
                        + ")\n"
        );
        createView("create view v1 as select * from t1");
        createMvByNereids("create materialized view mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from v1 ;");
        Database db2 = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("db2");
        MTMV mtmv = (MTMV) db2.getTableOrAnalysisException("mv1");
        MTMVRefreshSnapshot mtmvRefreshSnapshot = new MTMVRefreshSnapshot();
        mtmv.setRefreshSnapshot(mtmvRefreshSnapshot);
        Assertions.assertTrue(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 0);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.INIT));
        dropTable("t1", true);
        Assertions.assertTrue(mtmv.getRefreshSnapshot().equals(mtmvRefreshSnapshot));
        Assertions.assertTrue(mtmv.getSchemaChangeVersion() == 1);
        Assertions.assertTrue(mtmv.getStatus().getState().equals(MTMVState.SCHEMA_CHANGE));
    }
}
