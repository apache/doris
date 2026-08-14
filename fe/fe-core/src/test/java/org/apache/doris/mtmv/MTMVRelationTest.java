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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVState;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

public class MTMVRelationTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        Config.enable_table_stream = true;
        Config.enable_feature_binlog = true;
    }

    private MTMVRelation createOldStreamRelation(BaseTableInfo streamInfo) {
        MTMVRelation relation = new MTMVRelation(Sets.newHashSet(streamInfo), Sets.newHashSet(streamInfo), null,
                Sets.newHashSet(), Sets.newHashSet());
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(relation), MTMVRelation.class);
    }

    private void addMatchingBaseTableSnapshot(MTMV mtmv, MTMVRelatedTableIf baseTable) throws Exception {
        MTMVRefreshPartitionSnapshot snapshot = new MTMVRefreshPartitionSnapshot();
        snapshot.addTableSnapshot(new BaseTableInfo(baseTable), baseTable.getTableSnapshot(Optional.empty()));
        mtmv.getRefreshSnapshot().getPartitionSnapshots().put(
                mtmv.getPartitionNames().iterator().next(), snapshot);
    }

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

    @Test
    public void testMTMVRelationIncludesStreamBaseDependency() throws Exception {
        createDatabaseAndUse("stream_mtmv_db");
        createTables(
                "CREATE TABLE stream_base (k1 int, k2 int)\n"
                        + "UNIQUE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true',\n"
                        + "'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true')",
                "CREATE STREAM stream_source ON TABLE stream_base\n"
                        + "PROPERTIES ('show_initial_rows' = 'true')");
        createMvByNereids("CREATE MATERIALIZED VIEW stream_mv BUILD DEFERRED\n"
                + "REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT k1, k2 FROM stream_source");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("stream_mtmv_db");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("stream_mv");
        TableIf stream = db.getTableOrAnalysisException("stream_source");
        TableIf baseTable = db.getTableOrAnalysisException("stream_base");
        BaseTableInfo streamInfo = new BaseTableInfo(stream);
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        BaseTableInfo mtmvInfo = new BaseTableInfo(mtmv);

        Assertions.assertEquals(Sets.newHashSet(streamInfo, baseTableInfo), mtmv.getRelation().getBaseTables());
        Assertions.assertEquals(Sets.newHashSet(streamInfo), mtmv.getRelation().getBaseTablesOneLevel());
        Assertions.assertEquals(Sets.newHashSet(streamInfo, baseTableInfo),
                mtmv.getRelation().getBaseTablesOneLevelAndFromView());
        Assertions.assertEquals(Sets.newHashSet(mtmvInfo), Env.getCurrentEnv().getMtmvService().getRelationManager()
                .getMtmvsByBaseTable(baseTableInfo));

        Pair<Set<TableIf>, Set<TableIf>> queryTables = MTMVPlanUtil.getBaseTableFromQuery(
                mtmv.getQuerySql(), connectContext);
        Assertions.assertEquals(Sets.newHashSet(stream, baseTable), queryTables.first);
        Assertions.assertEquals(Sets.newHashSet(stream), queryTables.second);
    }

    @Test
    public void testCompatibleAddsPersistedStreamBaseDependency() throws Exception {
        createDatabaseAndUse("stream_mtmv_compatible_db");
        createTables(
                "CREATE TABLE stream_base (k1 int, k2 int)\n"
                        + "UNIQUE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true',\n"
                        + "'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true')",
                "CREATE STREAM stream_source ON TABLE stream_base\n"
                        + "PROPERTIES ('show_initial_rows' = 'true')");
        createMvByNereids("CREATE MATERIALIZED VIEW stream_mv BUILD DEFERRED\n"
                + "REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT k1, k2 FROM stream_source");

        Database db = Env.getCurrentEnv().getInternalCatalog()
                .getDbOrAnalysisException("stream_mtmv_compatible_db");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("stream_mv");
        TableIf stream = db.getTableOrAnalysisException("stream_source");
        TableIf baseTable = db.getTableOrAnalysisException("stream_base");
        BaseTableInfo streamInfo = new BaseTableInfo(stream);
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        BaseTableInfo mtmvInfo = new BaseTableInfo(mtmv);

        // Model an image written before stream bases were persisted as MTMV dependencies.
        mtmv.setRelation(createOldStreamRelation(streamInfo));
        MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
        relationManager.refreshMTMVCache(mtmv.getRelation(), mtmvInfo);
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(baseTableInfo));
        Assertions.assertTrue(MTMVPartitionUtil.isMTMVSync(mtmv));

        mtmv.compatible(Env.getCurrentEnv().getCatalogMgr());
        mtmv.compatible(Env.getCurrentEnv().getCatalogMgr());

        Assertions.assertEquals(Sets.newHashSet(streamInfo, baseTableInfo), mtmv.getRelation().getBaseTables());
        Assertions.assertEquals(Sets.newHashSet(streamInfo), mtmv.getRelation().getBaseTablesOneLevel());
        Assertions.assertEquals(Sets.newHashSet(streamInfo, baseTableInfo),
                mtmv.getRelation().getBaseTablesOneLevelAndFromView());
        Assertions.assertEquals(Sets.newHashSet(mtmvInfo), relationManager.getMtmvsByBaseTable(baseTableInfo));
        Assertions.assertFalse(MTMVPartitionUtil.isMTMVSync(mtmv));
    }

    @Test
    public void testCompatibleFailsWhilePersistedStreamIsMissing() throws Exception {
        createDatabaseAndUse("stream_mtmv_missing_db");
        createTables(
                "CREATE TABLE stream_base (k1 int, k2 int)\n"
                        + "UNIQUE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true',\n"
                        + "'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true')",
                "CREATE STREAM stream_source ON TABLE stream_base\n"
                        + "PROPERTIES ('show_initial_rows' = 'true')");
        createMvByNereids("CREATE MATERIALIZED VIEW stream_mv BUILD DEFERRED\n"
                + "REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT k1, k2 FROM stream_source");

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException("stream_mtmv_missing_db");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("stream_mv");
        TableIf stream = db.getTableOrAnalysisException("stream_source");
        BaseTableInfo streamInfo = new BaseTableInfo(stream);
        BaseTableInfo baseTableInfo = new BaseTableInfo(db.getTableOrAnalysisException("stream_base"));
        BaseTableInfo mtmvInfo = new BaseTableInfo(mtmv);
        mtmv.setRelation(createOldStreamRelation(streamInfo));
        MTMVRelationManager relationManager = Env.getCurrentEnv().getMtmvService().getRelationManager();
        relationManager.refreshMTMVCache(mtmv.getRelation(), mtmvInfo);

        mtmv.setStatus(new MTMVStatus(MTMVState.NORMAL, null));
        Env.getCurrentEnv().dropStream(InternalCatalog.INTERNAL_CATALOG_NAME,
                "stream_mtmv_missing_db", "stream_source", false, false);
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());

        mtmv.setStatus(new MTMVStatus(MTMVState.NORMAL, null));
        mtmv.compatible(Env.getCurrentEnv().getCatalogMgr());

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        Assertions.assertEquals(Sets.newHashSet(streamInfo), mtmv.getRelation().getBaseTables());
        Assertions.assertEquals(Sets.newHashSet(), relationManager.getMtmvsByBaseTable(baseTableInfo));

        recoverTable("RECOVER TABLE stream_mtmv_missing_db.stream_source");

        Assertions.assertSame(stream, db.getTableOrAnalysisException("stream_source"));
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
    }

    @Test
    public void testCompatibleRejectsSameNameViewWithRecycledStream() throws Exception {
        assertSameNameViewReplacementRejected("stream_mtmv_recycled_view_db", false);
    }

    @Test
    public void testCompatibleRejectsSameNameViewWithoutRecycledStream() throws Exception {
        assertSameNameViewReplacementRejected("stream_mtmv_erased_view_db", true);
    }

    private void assertSameNameViewReplacementRejected(String dbName, boolean eraseRecycledStream) throws Exception {
        createDatabaseAndUse(dbName);
        createTables(
                "CREATE TABLE stream_base (k1 int, k2 int)\n"
                        + "UNIQUE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES ('replication_num' = '1', 'binlog.enable' = 'true',\n"
                        + "'binlog.format' = 'ROW', 'binlog.need_historical_value' = 'true')",
                "CREATE TABLE replacement_base (k1 int, k2 int)\n"
                        + "DUPLICATE KEY(k1)\n"
                        + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                        + "PROPERTIES ('replication_num' = '1')",
                "CREATE STREAM stream_source ON TABLE stream_base\n"
                        + "PROPERTIES ('show_initial_rows' = 'true')");
        createMvByNereids("CREATE MATERIALIZED VIEW stream_mv BUILD DEFERRED\n"
                + "REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1')\n"
                + "AS SELECT k1, k2 FROM stream_source");

        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("stream_mv");
        TableIf stream = db.getTableOrAnalysisException("stream_source");
        BaseTableInfo streamInfo = new BaseTableInfo(stream);
        MTMVRelatedTableIf baseTable = (MTMVRelatedTableIf) db.getTableOrAnalysisException("stream_base");
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);

        mtmv.setStatus(new MTMVStatus(MTMVState.NORMAL, null));
        Env.getCurrentEnv().dropStream(InternalCatalog.INTERNAL_CATALOG_NAME, dbName,
                "stream_source", false, false);
        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        if (eraseRecycledStream) {
            Env.getCurrentRecycleBin().eraseTableInstantly(stream.getId());
            Assertions.assertNull(Env.getCurrentRecycleBin().getRecycledTableNullable(db.getId(), stream.getId()));
        } else {
            Assertions.assertSame(stream,
                    Env.getCurrentRecycleBin().getRecycledTableNullable(db.getId(), stream.getId()));
        }

        createView("CREATE VIEW stream_source AS SELECT k1, k2 FROM replacement_base");

        // Model a loaded MTMV whose old base snapshot still matches after the stream is replaced by a view.
        addMatchingBaseTableSnapshot(mtmv, baseTable);
        mtmv.setStatus(new MTMVStatus(MTMVState.NORMAL, null));
        Assertions.assertTrue(MTMVPartitionUtil.isMTMVSync(mtmv));

        mtmv.compatible(Env.getCurrentEnv().getCatalogMgr());

        Assertions.assertEquals(MTMVState.SCHEMA_CHANGE, mtmv.getStatus().getState());
        Assertions.assertFalse(mtmv.getStatus().canBeCandidate());
        Assertions.assertEquals(Sets.newHashSet(streamInfo, baseTableInfo), mtmv.getRelation().getBaseTables());
    }
}
