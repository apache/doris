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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class DropRowPolicyCommandTest extends TestWithFeService {
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;
    private UserIdentity user;
    private TableNameInfo tableNameInfo;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
        user = new UserIdentity("jack", "127.0.0.1", true);
        tableNameInfo = new TableNameInfo("test_db", "test_tbl");
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        new Expectations() {
            {
                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;
            }
        };
        DropRowPolicyCommand command = new DropRowPolicyCommand(false, "test_policy", tableNameInfo, user, "role1");
        Assertions.assertDoesNotThrow(() -> command.validate(connectContext));
    }
}
