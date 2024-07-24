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

package org.apache.doris.cluster;

import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.clone.RebalancerTestUtil;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DecommissionBackendTest extends TestWithFeService {
    @Override
    protected int backendNum() {
        return 4;
    }

    @Override
    protected void beforeCluster() {
        FeConstants.runningUnitTest = true;
    }

    @BeforeAll
    public void beforeClass() {
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        Config.tablet_checker_interval_ms = 100;
        Config.tablet_schedule_interval_ms = 100;
        Config.tablet_repair_delay_factor_second = 1;
        Config.allow_replica_on_same_host = true;
        Config.disable_balance = true;
        Config.schedule_batch_size = 1000;
        Config.schedule_slot_num_per_hdd_path = 1000;
        Config.heartbeat_interval_second = 5;
    }

    @Test
    public void testDecommissionBackend() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db1
        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        RebalancerTestUtil.updateReplicaPathHash();

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();

        // 5. execute decommission
        Backend srcBackend = null;
        for (Backend backend : idToBackendRef.values()) {
            if (!Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backend.getId()).isEmpty()) {
                srcBackend = backend;
                break;
            }
        }

        Assertions.assertNotNull(srcBackend);
        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        Assertions.assertTrue(srcBackend.isDecommissioned());
        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
            && Env.getCurrentSystemInfo().getAllBackendsByAllCluster().containsKey(srcBackend.getId())) {
            Thread.sleep(1000);
        }

        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());

        // For now, we have pre-built internal table: analysis_job and column_statistics
        Assertions.assertEquals(tabletNum,
                Env.getCurrentInvertedIndex().getTabletMetaMap().size());

        // 6. add backend
        addNewBackend();
        Assertions.assertEquals(backendNum(), Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());
    }

    @Test
    public void testDecommissionBackendById() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();
        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db1
        createDatabase("db2");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db2.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        RebalancerTestUtil.updateReplicaPathHash();

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();
        Assertions.assertTrue(tabletNum > 0);

        // 5. execute decommission
        Backend srcBackend = null;
        for (Backend backend : idToBackendRef.values()) {
            if (!Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backend.getId()).isEmpty()) {
                srcBackend = backend;
                break;
            }
        }

        Assertions.assertNotNull(srcBackend);

        // decommission backend by id
        String decommissionByIdStmtStr = "alter system decommission backend \"" + srcBackend.getId() + "\"";
        AlterSystemStmt decommissionByIdStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionByIdStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionByIdStmt);

        Assertions.assertTrue(srcBackend.isDecommissioned());
        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
                && Env.getCurrentSystemInfo().getAllBackendsByAllCluster().containsKey(srcBackend.getId())) {
            Thread.sleep(1000);
        }

        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());

        // add backend
        addNewBackend();
        Assertions.assertEquals(backendNum(), Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());

    }

    @Test
    public void testDecommissionBackendWithDropTable() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        SystemInfoService infoService = Env.getCurrentSystemInfo();

        ImmutableMap<Long, Backend> idToBackendRef = infoService.getAllBackendsByAllCluster();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db3
        createDatabase("db3");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        long availableBeNum = infoService.getAllBackendIds(true).stream()
                .filter(beId -> infoService.checkBackendScheduleAvailable(beId)).count();

        // 3. create table tbl1 tbl2
        createTable("create table db3.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '"
                + availableBeNum + "');");
        createTable("create table db3.tbl2(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        RebalancerTestUtil.updateReplicaPathHash();

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();
        Assertions.assertTrue(tabletNum > 0);

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("db2");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("tbl1");
        Assertions.assertNotNull(tbl);
        long backendId = tbl.getPartitions().iterator().next()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).iterator().next()
                .getTablets().iterator().next()
                .getReplicas().iterator().next()
                .getBackendId();

        Backend srcBackend = infoService.getBackend(backendId);
        Assertions.assertNotNull(srcBackend);

        // 5. drop table tbl1
        dropTable("db3.tbl1", false);

        // 6. execute decommission
        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);
        Assertions.assertTrue(srcBackend.isDecommissioned());

        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
            && Env.getCurrentSystemInfo().getAllBackendsByAllCluster().containsKey(srcBackend.getId())) {
            Thread.sleep(1000);
        }

        // BE has been dropped successfully
        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());

        // tbl1 has been dropped successfully
        final String sql = "show create table db3.tbl1;";
        Assertions.assertThrows(AnalysisException.class, () -> showCreateTable(sql));

        // TabletInvertedIndex still holds these tablets of srcBackend, but they are all in recycled status
        List<Long> tabletList = Env.getCurrentInvertedIndex().getTabletIdsByBackendId(srcBackend.getId());
        Assertions.assertFalse(tabletList.isEmpty());
        Assertions.assertTrue(Env.getCurrentRecycleBin().allTabletsInRecycledStatus(tabletList));

        // recover tbl1, because tbl1 has more than one replica, so it still can be recovered
        Assertions.assertDoesNotThrow(() -> recoverTable("db3.tbl1"));
        Assertions.assertDoesNotThrow(() -> showCreateTable(sql));
        dropTable("db3.tbl1", false);

        addNewBackend();
        Assertions.assertEquals(backendNum(), Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());
    }

    @Test
    public void testDecommissionBackendWithMTMV() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getAllBackendsByAllCluster();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db1
        createDatabase("db4");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table
        createTable("CREATE TABLE db4.table1 (\n"
                + " `c1` varchar(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` int(20) not NULL,\n"
                + " `k4` bitmap BITMAP_UNION,\n"
                + " `k5` bitmap BITMAP_UNION\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + ";");

        createTable("CREATE TABLE db4.table2 (\n"
                + " `c1` bigint(20) NULL,\n"
                + " `c2` bigint(20) NULL,\n"
                + " `c3` bigint(20) not NULL,\n"
                + " `k4` bitmap BITMAP_UNION,\n"
                + " `k5` bitmap BITMAP_UNION\n"
                + ") ENGINE=OLAP\n"
                + "AGGREGATE KEY(`c1`, `c2`, `c3`)\n"
                + "COMMENT 'OLAP'\n"
                + "DISTRIBUTED BY HASH(`c2`) BUCKETS 1\n"
                + ";");

        createMvByNereids("create materialized view db4.mv1 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "as "
                + "select t1.c1, t2.c2, t2.k4 "
                + "from db4.table1 t1 "
                + "inner join db4.table2 t2 on t1.c1= t2.c2;");

        createMvByNereids("create materialized view db4.mv2 BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "DISTRIBUTED BY HASH(c1) BUCKETS 20 \n"
                + "PROPERTIES ( 'colocate_with' = 'foo', 'replication_num' = '3' ) "
                + "as "
                + "select t1.c1 as c1, t2.c3, t2.k5 "
                + "from db4.table1 t1 "
                + "inner join db4.table2 t2 on t1.c1= t2.c3;");

        RebalancerTestUtil.updateReplicaPathHash();

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("db4");
        OlapTable tbl = (OlapTable) db.getTableOrMetaException("mv1");
        Assertions.assertNotNull(tbl);

        Partition partition = tbl.getPartitions().iterator().next();
        Tablet tablet = partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)
                .iterator().next().getTablets().iterator().next();
        Assertions.assertNotNull(tablet);
        Backend srcBackend = Env.getCurrentSystemInfo().getBackend(tablet.getReplicas().get(0).getBackendId());
        Assertions.assertNotNull(srcBackend);

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();

        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:"
                + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        Assertions.assertTrue(srcBackend.isDecommissioned());
        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
            && Env.getCurrentSystemInfo().getAllBackendsByAllCluster().containsKey(srcBackend.getId())) {
            Thread.sleep(1000);
        }

        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());

        // For now, we have pre-built internal table: analysis_job and column_statistics
        Assertions.assertEquals(tabletNum,
                Env.getCurrentInvertedIndex().getTabletMetaMap().size());

        for (Replica replica : tablet.getReplicas()) {
            Assertions.assertTrue(replica.getBackendId() != srcBackend.getId());
        }

        // 6. add backend
        addNewBackend();
        Assertions.assertEquals(backendNum(), Env.getCurrentSystemInfo().getAllBackendsByAllCluster().size());
    }
}
