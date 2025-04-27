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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testAll() throws IOException {
        runBefore();
        //normal
        new Expectations() {
            {
                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
        CleanQueryStatsCommand command = new CleanQueryStatsCommand();
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //NoPriviledge
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = false;
            }
        };
        CleanQueryStatsCommand command2 = new CleanQueryStatsCommand();
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext), "Access denied; you need (at least one of) the (CLEAN ALL QUERY STATS) privilege(s) for this operation");
    }

    @Test
    public void testDB() throws IOException {
        runBefore();
        //normal
        new Expectations() {
            {
                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkDbPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
        CleanQueryStatsCommand command = new CleanQueryStatsCommand(CatalogMocker.TEST_DB_NAME);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //NoPriviledge
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkDbPriv(connectContext, internalCtl, CatalogMocker.TEST_DB_NAME, PrivPredicate.ALTER);
                minTimes = 0;
                result = false;
            }
        };
        CleanQueryStatsCommand command2 = new CleanQueryStatsCommand(CatalogMocker.TEST_DB_NAME);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext), "Access denied; you need (at least one of) the (CLEAN DATABASE QUERY STATS FOR " + ClusterNamespace.getNameFromFullName(CatalogMocker.TEST_DB_NAME) + ") privilege(s) for this operation");
    }

    @Test
    public void testTbl() throws IOException {
        runBefore();
        TableNameInfo tableNameInfo = new TableNameInfo(CatalogMocker.TEST_DB_NAME, CatalogMocker.TEST_TBL_NAME);

        //normal
        new Expectations() {
            {
                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkTblPriv(connectContext, tableNameInfo, PrivPredicate.ALTER);
                minTimes = 0;
                result = true;
            }
        };
        CleanQueryStatsCommand command = new CleanQueryStatsCommand(tableNameInfo);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));

        //NoPriviledge
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkTblPriv(connectContext, tableNameInfo, PrivPredicate.ALTER);
                minTimes = 0;
                result = false;
            }
        };
        CleanQueryStatsCommand command2 = new CleanQueryStatsCommand(tableNameInfo);
        Assertions.assertThrows(AnalysisException.class, () -> command2.validate(connectContext), "Access denied; you need (at least one of) the (CLEAN TABLE QUERY STATS FROM " + tableNameInfo + ") privilege(s) for this operation");
    }
}
