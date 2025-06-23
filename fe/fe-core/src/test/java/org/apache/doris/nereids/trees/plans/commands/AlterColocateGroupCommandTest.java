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
import org.apache.doris.nereids.trees.plans.commands.info.ColocateGroupName;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AlterColocateGroupCommandTest {
    private static final String internalCtl = InternalCatalog.INTERNAL_CATALOG_NAME;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;

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

                accessControllerManager.checkDbPriv(connectContext, internalCtl, "test_db", PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidate() {
        runBefore();
        ColocateGroupName groupName = new ColocateGroupName("test_db", "test_group");
        Map<String, String> properties = new HashMap<>();
        properties.put("k1", "v1");
        AlterColocateGroupCommand command = new AlterColocateGroupCommand(groupName, properties);
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}
