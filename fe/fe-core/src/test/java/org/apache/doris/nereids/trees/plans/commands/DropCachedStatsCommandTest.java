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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class DropCachedStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String catalogNotExist = "catalog_not_exist";
    private static final String dbNotExist = "db_not_exist";
    private static final String tblNotExist = "tbl_not_exist";
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private ConnectContext connectContext;
    private Database db;

    private void runBefore() throws Exception {
        db = CatalogMocker.mockDb();
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getCatalogMgr().getCatalog(anyString);
                minTimes = 0;
                result = catalog;

                catalog.getDb(anyString);
                minTimes = 0;
                result = db;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                        PrivPredicate.DROP);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();

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
    void testValidateNoPrivilege() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkTblPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.DROP);
                minTimes = 0;
                result = false;
            }
        };

        TableNameInfo tableNameInfo =
                new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        DropCachedStatsCommand command = new DropCachedStatsCommand(tableNameInfo);
        connectContext.getSessionVariable().enableStats = true;
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(connectContext),
                "DROP command denied to user 'null'@'null' for table 'test_db.test_tbl2'");
    }
}
