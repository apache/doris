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

import org.apache.doris.backup.CatalogMocker;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ShowQueryStatsCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static boolean dbInitialized = false;

    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws Exception {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
        if (!dbInitialized) {
            createDatabaseWithSql("CREATE DATABASE IF NOT EXISTS " + CatalogMocker.TEST_DB_NAME);
            createTable("CREATE TABLE IF NOT EXISTS " + CatalogMocker.TEST_DB_NAME + "." + CatalogMocker.TEST_TBL_NAME
                    + " (k1 INT, k2 INT) DISTRIBUTED BY HASH(k1) BUCKETS 1"
                    + " PROPERTIES ('replication_num' = '1')");
            dbInitialized = true;
        }
    }

    @Test
    public void testValidateWithPrivilege() throws Exception {
        runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.any(TableNameInfo.class),
                Mockito.eq(PrivPredicate.SHOW));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

        // normal
        ShowQueryStatsCommand command = new ShowQueryStatsCommand(tableNameInfo.getDb(),
                tableNameInfo, false, false);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }

    @Test
    void testValidateNoPrivilege() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkGlobalPriv(
                Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN));
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.SHOW));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        ShowQueryStatsCommand command = new ShowQueryStatsCommand(tableNameInfo.getDb(),
                tableNameInfo, false, false);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext));
    }
}
