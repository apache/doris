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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AdminCopyTabletCommandTest {
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

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.ADMIN);
                minTimes = 0;
                result = true;
            }
        };
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        Map<String, String> properties = new HashMap<>();
        properties.put("version", "0");
        AdminCopyTabletCommand command = new AdminCopyTabletCommand(100, properties);
        Assertions.assertDoesNotThrow(() -> command.validate());

        Map<String, String> properties2 = new HashMap<>();
        properties2.put("backend_id", "10");
        AdminCopyTabletCommand command2 = new AdminCopyTabletCommand(100, properties2);
        Assertions.assertDoesNotThrow(() -> command2.validate());

        Map<String, String> properties3 = new HashMap<>();
        properties3.put("expiration_minutes", "10");
        AdminCopyTabletCommand command3 = new AdminCopyTabletCommand(100, properties3);
        Assertions.assertDoesNotThrow(() -> command3.validate());
    }

    @Test
    public void testNoPriviledge() {
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
        Map<String, String> properties = new HashMap<>();
        properties.put("version", "0");
        AdminCopyTabletCommand command = new AdminCopyTabletCommand(100, properties);
        Assertions.assertThrows(AnalysisException.class, () -> command.validate(),
                "Access denied; you need (at least one of) the (Admin_priv) privilege(s) for this operation");
    }
}
