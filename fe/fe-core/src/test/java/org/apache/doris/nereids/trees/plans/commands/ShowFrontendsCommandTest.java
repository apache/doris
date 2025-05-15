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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowFrontendsCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    private static final String infoDB = InfoSchemaDb.DATABASE_NAME;

    @Mocked
    private Env env;
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private AccessControllerManager accessControllerManager;
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
        ShowFrontendsCommand command = new ShowFrontendsCommand(null);
        Assertions.assertDoesNotThrow(() -> command.doRun(ctx, null));
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
        ShowFrontendsCommand command = new ShowFrontendsCommand(null);
        Assertions.assertThrows(AnalysisException.class, () -> command.doRun(ctx, null));
    }
}
