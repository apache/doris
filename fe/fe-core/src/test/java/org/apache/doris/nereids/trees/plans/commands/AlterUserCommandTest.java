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

import org.apache.doris.analysis.PassVar;
import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.AlterUserInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AlterUserCommandTest extends TestWithFeService {
    private ConnectContext connectContext;
    private Env env;
    private AccessControllerManager accessControllerManager;
    private UserDesc userDesc;

    private void runBefore() throws IOException {
        connectContext = createDefaultCtx();
        env = Env.getCurrentEnv();
        accessControllerManager = env.getAccessManager();
    }

    @Test
    public void testValidateNormal() throws Exception {
        runBefore();
        new Expectations() {
            {
                connectContext.isSkipAuth();
                minTimes = 0;
                result = true;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;
            }
        };
        // init
        UserIdentity userIdentity = new UserIdentity(Auth.ROOT_USER, "%");
        connectContext.setCurrentUserIdentity(userIdentity);
        PassVar passVar = new PassVar("", true);
        userDesc = new UserDesc(userIdentity, passVar);
        PasswordOptions passwordOptions = PasswordOptions.UNSET_OPTION;
        AlterUserInfo alterUserInfo = new AlterUserInfo(true, userDesc, passwordOptions, null);
        AlterUserCommand alterUserCommand = new AlterUserCommand(alterUserInfo);
        Assertions.assertDoesNotThrow(() -> alterUserCommand.validate());

        //test ops.size() > 1
        AlterUserInfo alterUserInfo02 = new AlterUserInfo(true, userDesc, passwordOptions, "alterUserInfo02");
        AlterUserCommand alterUserCommand02 = new AlterUserCommand(alterUserInfo02);
        Assertions.assertThrows(AnalysisException.class, () -> alterUserCommand02.validate(),
                "Only support doing one type of operation at one time,actual number of type is 2");

        //testUser to modify root user
        connectContext.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("testUser", "%"));
        Assertions.assertThrows(AnalysisException.class, () -> alterUserCommand.validate(), "Only root user can modify root user");

        //test PasswordOptions
        PasswordOptions passwordOptions02 = new PasswordOptions(PasswordOptions.UNSET, PasswordOptions.UNSET, PasswordOptions.UNSET, PasswordOptions.UNSET, PasswordOptions.UNSET, -1);
        AlterUserInfo alterUserInfo03 = new AlterUserInfo(true, userDesc, passwordOptions02, null);
        AlterUserCommand alterUserCommand03 = new AlterUserCommand(alterUserInfo03);
        Assertions.assertThrows(AnalysisException.class, () -> alterUserCommand03.validate(), "Not support lock account now");
    }
}
