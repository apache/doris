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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DropCachedStatsCommandTest extends TestWithFeService {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String catalogNotExist = "catalog_not_exist";
    private static final String dbNotExist = "db_not_exist";
    private static final String tblNotExist = "tbl_not_exist";
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(CatalogMocker.TEST_DB_NAME);
        createTable("create table " + CatalogMocker.TEST_DB_NAME + "." + CatalogMocker.TEST_TBL_NAME + "\n"
                + "(k1 int, k2 int)\n"
                + "duplicate key(k1)\n"
                + "distributed by hash(k2) buckets 1\n"
                + "properties('replication_num' = '1');");
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        connectContext.setSkipAuth(true);
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(true).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.eq(PrivPredicate.DROP));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        //test normal
        connectContext.getSessionVariable().enableStats = true;
        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        DropCachedStatsCommand command = new DropCachedStatsCommand(tableNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //test table is empty
        TableNameInfo tableNameInfo2 =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, "");
        DropCachedStatsCommand command2 = new DropCachedStatsCommand(tableNameInfo2);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, () -> command2.validate(connectContext),
                "Table name is null");

        //test unkown catalog
        List<String> parts = new ArrayList<>();
        parts.add(catalogNotExist);
        parts.add(dbNotExist);
        parts.add(tblNotExist);
        TableNameInfo tableNameInfo3 = new TableNameInfo(parts);
        DropCachedStatsCommand command3 = new DropCachedStatsCommand(tableNameInfo3);
        Assertions.assertThrows(AnalysisException.class, () -> command3.validate(connectContext),
                "Unknown catalog 'catalog_not_exist'");

        //test unkown db
        TableNameInfo tableNameInfo4 = new TableNameInfo(dbNotExist, CatalogMocker.TEST_TBL_NAME);
        DropCachedStatsCommand command4 = new DropCachedStatsCommand(tableNameInfo4);
        Assertions.assertThrows(AnalysisException.class, () -> command4.validate(connectContext),
                "Unknown database 'db_not_exist'");

        //test unkown tbl
        TableNameInfo tableNameInfo5 = new TableNameInfo(CatalogMocker.TEST_DB_NAME, tblNotExist);
        DropCachedStatsCommand command5 = new DropCachedStatsCommand(tableNameInfo5);
        Assertions.assertThrows(AnalysisException.class, () -> command5.validate(connectContext),
                "Unknown table 'tbl_not_exist'");

        //test enable stats is false
        connectContext.getSessionVariable().enableStats = false;
        Assertions.assertThrows(UserException.class, () -> command.validate(connectContext),
                "Analyze function is forbidden, you should add `enable_stats=true` in your FE conf file");
    }

    @Test
    void testValidateNoPrivilege() throws Exception {
        runBefore();
        AccessControllerManager spyAcm = Mockito.spy(accessControllerManager);
        Mockito.doReturn(false).when(spyAcm).checkTblPriv(
                Mockito.nullable(ConnectContext.class), Mockito.anyString(),
                Mockito.anyString(), Mockito.anyString(), Mockito.any(PrivPredicate.class));
        Deencapsulation.setField(env, "accessManager", spyAcm);

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        DropCachedStatsCommand command = new DropCachedStatsCommand(tableNameInfo);
        connectContext.getSessionVariable().enableStats = true;
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "DROP command denied to user 'null'@'null' for table 'test_db.test_tbl2'");
    }
}
