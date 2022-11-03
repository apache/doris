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
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.system.Backend;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DecommissionBackendTest extends TestWithFeService {

    @Override
    protected int backendNum() {
        return 3;
    }

    @Override
    protected void beforeCreatingConnectContext() throws Exception {
        FeConstants.default_scheduler_interval_millisecond = 1000;
        FeConstants.tablet_checker_interval_ms = 1000;
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
        Assertions.assertEquals(tabletNum, Env.getCurrentInvertedIndex().getTabletMetaMap().size());

    }

}

