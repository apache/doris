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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DecommissionBackendTest extends TestWithFeService {
    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void beforeCluster() {
        FeConstants.runningUnitTest = true;
        needCleanDir = false;
    }

    @BeforeAll
    public void beforeClass() {
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        Config.tablet_checker_interval_ms = 1000;
        Config.tablet_repair_delay_factor_second = 1;
        Config.allow_replica_on_same_host = true;
    }

    @Test
    public void testDecommissionBackend() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getIdToBackend();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db1
        createDatabase("db1");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1
        createTable("create table db1.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();

        // 5. execute decommission
        Backend srcBackend = null;
        for (Backend backend : idToBackendRef.values()) {
            if (Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backend.getId()).size() > 0) {
                srcBackend = backend;
                break;
            }
        }

        Assertions.assertTrue(srcBackend != null);
        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);

        Assertions.assertEquals(true, srcBackend.isDecommissioned());
        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
            && Env.getCurrentSystemInfo().getIdToBackend().containsKey(srcBackend.getId())) {
            Thread.sleep(1000);
        }

        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getIdToBackend().size());

        // For now, we have pre-built internal table: analysis_job and column_statistics
        Assertions.assertEquals(tabletNum,
                Env.getCurrentInvertedIndex().getTabletMetaMap().size());

        // 6. add backend
        String addBackendStmtStr = "alter system add backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt addBackendStmt = (AlterSystemStmt) parseAndAnalyzeStmt(addBackendStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(addBackendStmt);
        Assertions.assertEquals(backendNum(), Env.getCurrentSystemInfo().getIdToBackend().size());

    }

    @Test
    public void testDecommissionBackendWithDropTable() throws Exception {
        // 1. create connect context
        connectContext = createDefaultCtx();

        ImmutableMap<Long, Backend> idToBackendRef = Env.getCurrentSystemInfo().getIdToBackend();
        Assertions.assertEquals(backendNum(), idToBackendRef.size());

        // 2. create database db2
        createDatabase("db2");
        System.out.println(Env.getCurrentInternalCatalog().getDbNames());

        // 3. create table tbl1 tbl2
        createTable("create table db2.tbl1(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '2');");
        createTable("create table db2.tbl2(k1 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');");

        // 4. query tablet num
        int tabletNum = Env.getCurrentInvertedIndex().getTabletMetaMap().size();
        Assertions.assertTrue(tabletNum > 0);

        Backend srcBackend = null;
        for (Backend backend : idToBackendRef.values()) {
            if (Env.getCurrentInvertedIndex().getTabletIdsByBackendId(backend.getId()).size() > 0) {
                srcBackend = backend;
                break;
            }
        }
        Assertions.assertTrue(srcBackend != null);

        // 5. drop table tbl1
        dropTable("db2.tbl1", false);

        // 6. execute decommission
        String decommissionStmtStr = "alter system decommission backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt decommissionStmt = (AlterSystemStmt) parseAndAnalyzeStmt(decommissionStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(decommissionStmt);
        Assertions.assertEquals(true, srcBackend.isDecommissioned());

        long startTimestamp = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTimestamp < 90000
            && Env.getCurrentSystemInfo().getIdToBackend().containsKey(srcBackend.getId())) {
            Thread.sleep(1000);
        }

        // BE has been dropped successfully
        Assertions.assertEquals(backendNum() - 1, Env.getCurrentSystemInfo().getIdToBackend().size());

        // tbl1 has been dropped successfully
        final String sql = "show create table db2.tbl1;";
        Assertions.assertThrows(AnalysisException.class, () -> showCreateTable(sql));

        // TabletInvertedIndex still holds these tablets of srcBackend, but they are all in recycled status
        List<Long> tabletList = Env.getCurrentInvertedIndex().getTabletIdsByBackendId(srcBackend.getId());
        Assertions.assertTrue(tabletList.size() > 0);
        Assertions.assertTrue(Env.getCurrentRecycleBin().allTabletsInRecycledStatus(tabletList));

        // recover tbl1, because tbl1 has more than one replica, so it still can be recovered
        Assertions.assertDoesNotThrow(() -> recoverTable("db2.tbl1"));
        Assertions.assertDoesNotThrow(() -> showCreateTable(sql));

        String addBackendStmtStr = "alter system add backend \"127.0.0.1:" + srcBackend.getHeartbeatPort() + "\"";
        AlterSystemStmt addBackendStmt = (AlterSystemStmt) parseAndAnalyzeStmt(addBackendStmtStr);
        Env.getCurrentEnv().getAlterInstance().processAlterCluster(addBackendStmt);
        Assertions.assertEquals(backendNum(), Env.getCurrentSystemInfo().getIdToBackend().size());
    }

}
