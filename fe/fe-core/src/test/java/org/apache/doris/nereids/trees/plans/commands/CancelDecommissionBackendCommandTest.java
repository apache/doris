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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class CancelDecommissionBackendCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private ConnectContext connectContext;
    @Mocked
    private AccessControllerManager accessControllerManager;

    private final String dbName = "test_db";

    private void runBefore() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                ConnectContext.get();
                minTimes = 0;
                result = connectContext;

                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() {
        runBefore();
        List<String> params = new ArrayList<>();
        params.add("192.168.1.120:30111");
        params.add("192.168.1.121:30111");
        params.add("192.168.1.122:30111");
        CancelDecommissionBackendCommand command = new CancelDecommissionBackendCommand(params);
        Assertions.assertDoesNotThrow(() -> command.validate());

        //test hostInfos and ids are empty
        List<String> params02 = new ArrayList<>();
        CancelDecommissionBackendCommand command02 = new CancelDecommissionBackendCommand(params02);
        Assertions.assertThrows(IllegalStateException.class, () -> command02.validate());
    }
}
