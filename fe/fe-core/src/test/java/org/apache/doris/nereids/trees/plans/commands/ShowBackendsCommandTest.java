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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.tablefunction.BackendsTableValuedFunction;

import com.google.common.collect.ImmutableList;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowBackendsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String infoDB = InfoSchemaDb.DATABASE_NAME;

    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private CatalogMgr catalogMgr;

    private void runBefore() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                catalogMgr.getCatalog(anyString);
                minTimes = 0;
                result = catalog;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                accessControllerManager.checkDbPriv(ctx, internalCtl, infoDB, PrivPredicate.SELECT);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testNormal() throws Exception {
        runBefore();
        ShowBackendsCommand command = new ShowBackendsCommand();
        ShowResultSet showResultSet = command.doRun(ctx, null);
        List<Column> columnList = showResultSet.getMetaData().getColumns();
        ImmutableList<String> backendsTitleNames = BackendsTableValuedFunction.getBackendsTitleNames();
        Assertions.assertTrue(!columnList.isEmpty() && columnList.size() == backendsTitleNames.size());

        for (int i = 0; i < backendsTitleNames.size(); i++) {
            Assertions.assertEquals(backendsTitleNames.get(i), columnList.get(i).getName(),
                    "Column at index " + i + " should be " + backendsTitleNames.get(i));
        }

        Assertions.assertEquals("BackendId", columnList.get(0).getName());
        Assertions.assertEquals("Host", columnList.get(1).getName());
        Assertions.assertEquals("HeartbeatPort", columnList.get(2).getName());
        Assertions.assertEquals("BePort", columnList.get(3).getName());
        Assertions.assertEquals("HttpPort", columnList.get(4).getName());
        Assertions.assertEquals("BrpcPort", columnList.get(5).getName());
        Assertions.assertEquals("ArrowFlightSqlPort", columnList.get(6).getName());
        Assertions.assertEquals("LastStartTime", columnList.get(7).getName());
        Assertions.assertEquals("LastHeartbeat", columnList.get(8).getName());
        Assertions.assertEquals("Alive", columnList.get(9).getName());
        Assertions.assertEquals("SystemDecommissioned", columnList.get(10).getName());
        Assertions.assertEquals("TabletNum", columnList.get(11).getName());
        Assertions.assertEquals("DataUsedCapacity", columnList.get(12).getName());
        Assertions.assertEquals("TrashUsedCapacity", columnList.get(13).getName());
        Assertions.assertEquals("AvailCapacity", columnList.get(14).getName());
        Assertions.assertEquals("TotalCapacity", columnList.get(15).getName());
        Assertions.assertEquals("UsedPct", columnList.get(16).getName());
        Assertions.assertEquals("MaxDiskUsedPct", columnList.get(17).getName());
        Assertions.assertEquals("RemoteUsedCapacity", columnList.get(18).getName());
        Assertions.assertEquals("Tag", columnList.get(19).getName());
        Assertions.assertEquals("ErrMsg", columnList.get(20).getName());
        Assertions.assertEquals("Version", columnList.get(21).getName());
        Assertions.assertEquals("Status", columnList.get(22).getName());
        Assertions.assertEquals("HeartbeatFailureCounter", columnList.get(23).getName());
        Assertions.assertEquals("CpuCores", columnList.get(24).getName());
        Assertions.assertEquals("Memory", columnList.get(25).getName());
        Assertions.assertEquals("LiveSince", columnList.get(26).getName());
        Assertions.assertEquals("RunningTasks", columnList.get(27).getName());
        Assertions.assertEquals("NodeRole", columnList.get(28).getName());
    }

    @Test
    public void testNoPrivilege() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                env.getCatalogMgr();
                minTimes = 0;
                result = catalogMgr;

                catalogMgr.getCatalog(anyString);
                minTimes = 0;
                result = catalog;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                accessControllerManager.checkDbPriv(ctx, internalCtl, infoDB, PrivPredicate.SELECT);
                minTimes = 0;
                result = false;
            }
        };
        ShowBackendsCommand command = new ShowBackendsCommand();
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }
}
