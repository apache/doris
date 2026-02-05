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
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MTMVRelationTest extends TestWithFeService {
    // t1 => v1 => v2
    // t2 => mv1
    // mv1 join v2 => mv2
    @Test
    public void testMTMVRelation() throws Exception {
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
                        + ")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
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
        createView("create view v2 as select * from v1");
        createMvByNereids("create materialized view mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select * from t2;");
        createMvByNereids("create materialized view mv2 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select mv1.* from mv1, v2 ;");
        Database db1 = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("db1");
        MTMV mtmv = (MTMV) db1.getTableOrAnalysisException("mv2");
        MTMVRelation relation = mtmv.getRelation();
        BaseTableInfo t1 = new BaseTableInfo(db1.getTableOrAnalysisException("t1"));
        BaseTableInfo t2 = new BaseTableInfo(db1.getTableOrAnalysisException("t2"));
        BaseTableInfo v1 = new BaseTableInfo(db1.getTableOrAnalysisException("v1"));
        BaseTableInfo v2 = new BaseTableInfo(db1.getTableOrAnalysisException("v2"));
        BaseTableInfo mv1 = new BaseTableInfo(db1.getTableOrAnalysisException("mv1"));
        BaseTableInfo mv2 = new BaseTableInfo(db1.getTableOrAnalysisException("mv2"));
        // test forward index
        Assertions.assertEquals(Sets.newHashSet(t1, t2, mv1), relation.getBaseTables());
        Assertions.assertEquals(Sets.newHashSet(v1, v2), relation.getBaseViews());
        Assertions.assertEquals(Sets.newHashSet(mv1), relation.getBaseTablesOneLevel());
        Assertions.assertEquals(Sets.newHashSet(v2), relation.getBaseViewsOneLevel());
        Assertions.assertEquals(Sets.newHashSet(mv1, t1), relation.getBaseTablesOneLevelAndFromView());

        // test inverted index
        MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
        Assertions.assertEquals(Sets.newHashSet(mv2), relationManager.getMtmvsByBaseTable(t1));
        Assertions.assertEquals(Sets.newHashSet(mv2, mv1), relationManager.getMtmvsByBaseTable(t2));
        Assertions.assertEquals(Sets.newHashSet(mv2), relationManager.getMtmvsByBaseTable(mv1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(v1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(v2));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(mv2));

        Assertions.assertEquals(Sets.newHashSet(mv2), relationManager.getMtmvsByBaseTableOneLevelAndFromView(t1));
        Assertions.assertEquals(Sets.newHashSet(mv1), relationManager.getMtmvsByBaseTableOneLevelAndFromView(t2));
        Assertions.assertEquals(Sets.newHashSet(mv2), relationManager.getMtmvsByBaseTableOneLevelAndFromView(mv1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTableOneLevelAndFromView(v1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTableOneLevelAndFromView(v2));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTableOneLevelAndFromView(mv2));

        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseView(t1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseView(t2));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseView(mv1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseView(mv2));
        Assertions.assertEquals(Sets.newHashSet(mv2), relationManager.getMtmvsByBaseView(v1));
        Assertions.assertEquals(Sets.newHashSet(mv2), relationManager.getMtmvsByBaseView(v2));

        dropMvByNereids("drop materialized view mv2");
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(t1));
        Assertions.assertEquals(Sets.newHashSet(mv1), relationManager.getMtmvsByBaseTable(t2));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(mv1));

        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTableOneLevelAndFromView(t1));
        Assertions.assertEquals(Sets.newHashSet(mv1), relationManager.getMtmvsByBaseTableOneLevelAndFromView(t2));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTableOneLevelAndFromView(mv1));

        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseView(v1));
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseView(v2));
    }
}
