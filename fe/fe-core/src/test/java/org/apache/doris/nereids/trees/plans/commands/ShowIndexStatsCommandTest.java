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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowIndexStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String tableNotExist = "table_not_exist";
    @Mocked
    private Env env;
    @Mocked
    private InternalCatalog catalog;
    @Mocked
    private AccessControllerManager accessManager;
    @Mocked
    private ConnectContext ctx;
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
                result = accessManager;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                ctx.isSkipAuth();
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv(ctx, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME,
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = true;

                accessManager.checkTblPriv(ctx, internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME,
                        PrivPredicate.SHOW);
                minTimes = 0;
                result = false;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        TableNameInfo tableNameInfo =
                new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);
        ShowIndexStatsCommand command = new ShowIndexStatsCommand(tableNameInfo, CatalogMocker.TEST_TBL_NAME);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    @Test
    public void testValidateFail() throws Exception {
        runBefore();
        TableNameInfo tableNameInfo =
                new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, tableNotExist);
        ShowIndexStatsCommand command = new ShowIndexStatsCommand(tableNameInfo, tableNotExist);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx),
                "Table: " + tableNotExist + " not exists");

        TableNameInfo tableNameInfo2 =
                new TableNameInfo(internalCtl, CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL2_NAME);
        ShowIndexStatsCommand command2 = new ShowIndexStatsCommand(tableNameInfo2, CatalogMocker.TEST_TBL2_NAME);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(ctx),
                "Permission denied command denied to user 'null'@'null' for table 'test_db: test_tbl2'");
    }
}
