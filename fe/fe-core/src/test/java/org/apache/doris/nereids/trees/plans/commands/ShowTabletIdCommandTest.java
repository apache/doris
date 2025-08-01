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
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowTabletIdCommandTest {
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager manager;
    @Mocked
    private ConnectContext ctx;

    void runBefore(String dbName, boolean hasGlobalPriv) {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                ctx.getDatabase();
                minTimes = 0;
                result = dbName;

                env.getAccessManager();
                minTimes = 0;
                result = manager;

                ConnectContext.get();
                minTimes = 0;
                result = ctx;

                manager.checkGlobalPriv(ctx, PrivPredicate.ADMIN);
                minTimes = 0;
                result = hasGlobalPriv;
            }
        };
    }

    @Test
    public void testValidate() throws Exception {
        runBefore(CatalogMocker.TEST_DB_NAME, true);
        ShowTabletIdCommand command = new ShowTabletIdCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertDoesNotThrow(() -> command.validate(ctx));
    }

    @Test
    void noGlobalPriv() {
        runBefore(CatalogMocker.TEST_DB_NAME, false);
        ShowTabletIdCommand command = new ShowTabletIdCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));
    }

    @Test
    void dbIsEmpty() {
        runBefore("", true);
        ShowTabletIdCommand command = new ShowTabletIdCommand(CatalogMocker.TEST_TBL_ID);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(ctx));
    }
}
