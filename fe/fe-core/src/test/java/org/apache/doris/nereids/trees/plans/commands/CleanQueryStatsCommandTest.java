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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class CleanQueryStatsCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    public void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testAllNormal() throws IOException {
        runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        CleanQueryStatsCommand command = new CleanQueryStatsCommand();
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testDB() throws Exception {
        runBefore();
        connectContext.setDatabase("test_db");
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkDbPriv(Mockito.nullable(ConnectContext.class), Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        CleanQueryStatsCommand command = new CleanQueryStatsCommand("test_db");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    public void testTbl() throws Exception {
        runBefore();
        createDatabase("test_db");
        createTable("create table test_db.test_tbl\n" + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n" + "partition by range(k2)\n" + "(partition p1 values less than(\"10\"))\n"
                + "distributed by hash(k2) buckets 1\n" + "properties('replication_num' = '1'); ");
        TableNameInfo tableNameInfo = new TableNameInfo("test_db", "test_tbl");
        connectContext.setDatabase("test_db");
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(Mockito.nullable(ConnectContext.class), Mockito.any(TableNameInfo.class), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);
        CleanQueryStatsCommand command = new CleanQueryStatsCommand(tableNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}
