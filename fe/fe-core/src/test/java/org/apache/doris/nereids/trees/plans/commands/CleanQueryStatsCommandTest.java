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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CleanQueryStatsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Injectable
    Database database;
    @Injectable
    OlapTable table;
    @Mocked
    AccessControllerManager accessManager;
    @Mocked
    Env env;
    @Mocked
    InternalCatalog catalog;
    @Mocked
    private ConnectContext connectContext;

    private String jobName = "testJob";
    private String dbName = "testDb";
    private String tblName = "testTbl";

    public void runBefore() {
        new Expectations() {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                catalog.getDbNullable("testDb");
                minTimes = 0;
                result = database;

                env.getAccessManager();
                minTimes = 0;
                result = accessManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                database.getTableNullable("testTbl");
                minTimes = 0;
                result = table;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

    @Test
    public void testAll() {
        runBefore();
        //normal
        new Expectations() {
            {
                accessManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        CleanQueryStatsCommand command = new CleanQueryStatsCommand();
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //NoPriviledge
        new Expectations() {
            {
                accessManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        CleanQueryStatsCommand command2 = new CleanQueryStatsCommand();
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext), "Access denied; you need (at least one of) the (CLEAN ALL QUERY STATS) privilege(s) for this operation");
    }

    @Test
    public void testDB() {
        runBefore();
        //normal
        new Expectations() {
            {
                accessManager.checkGlobalPriv(connectContext, PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
        CleanQueryStatsCommand command = new CleanQueryStatsCommand(dbName);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //NoPriviledge
        new Expectations() {
            {
                accessManager.checkDbPriv(connectContext, internalCtl, dbName, PrivPredicate.ALTER);
                minTimes = 0;
                result = false;
            }
        };
        CleanQueryStatsCommand command2 = new CleanQueryStatsCommand(dbName);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext), "Access denied; you need (at least one of) the (CLEAN DATABASE QUERY STATS FOR " + ClusterNamespace.getNameFromFullName(dbName) + ") privilege(s) for this operation");
    }

    @Test
    public void testTbl() throws Exception {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(dbName, tblName);

        //normal
        new Expectations() {
            {
                accessManager.checkGlobalPriv(connectContext, PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
        CleanQueryStatsCommand command = new CleanQueryStatsCommand(tableNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //NoPriviledge
        new Expectations() {
            {
                accessManager.checkTblPriv(connectContext, tableNameInfo, PrivPredicate.ALTER);
                minTimes = 0;
                result = false;
            }
        };
        CleanQueryStatsCommand command2 = new CleanQueryStatsCommand(tableNameInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext), "Access denied; you need (at least one of) the (CLEAN TABLE QUERY STATS FROM " + tableNameInfo + ") privilege(s) for this operation");
    }
}
