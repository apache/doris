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

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AlterUserCommandTest {
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;
    @Mocked
    private ConnectContext connectContext;
    private UserDesc userDesc;
    private PasswordOptions passwordOptions;

    @Test
    public void testValidateNormal() throws Exception {
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

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.GRANT);
                minTimes = 0;
                result = true;
            }
        };
        // init
        UserIdentity userIdentity = new UserIdentity(Auth.ROOT_USER, "%");
        connectContext.setQualifiedUser("root");
        PassVar passVar = new PassVar("", true);
        userDesc = new UserDesc(userIdentity, passVar);
        passwordOptions = PasswordOptions.UNSET_OPTION;
        AlterUserInfo alterUserInfo = new AlterUserInfo(true, userDesc, passwordOptions, null);
        AlterUserCommand alterUserCommand = new AlterUserCommand(alterUserInfo);
        Assertions.assertDoesNotThrow(() -> alterUserCommand.validate());

        //test ops.size() > 1
        AlterUserInfo alterUserInfo02 = new AlterUserInfo(true, userDesc, passwordOptions, "alterUserInfo02");
        AlterUserCommand alterUserCommand02 = new AlterUserCommand(alterUserInfo02);
        Assertions.assertThrows(AnalysisException.class, () -> alterUserCommand02.validate(),
                "Only support doing one type of operation at one time,actual number of type is 2");

        //testUser to modify root user
        connectContext.setQualifiedUser("testUser");
        Assertions.assertThrows(AnalysisException.class, () -> alterUserCommand.validate(), "Only root user can modify root user");
    }

    @Test
    void testValidateNoPrivilege() {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkGlobalPriv(connectContext, PrivPredicate.GRANT);
                minTimes = 0;
                result = false;
            }
        };

        UserIdentity userIdentity = new UserIdentity(Auth.ROOT_USER, "%");
        connectContext.setQualifiedUser("root");
        PassVar passVar = new PassVar("", true);
        userDesc = new UserDesc(userIdentity, passVar);
        passwordOptions = PasswordOptions.UNSET_OPTION;
        AlterUserInfo alterUserInfo = new AlterUserInfo(true, userDesc, passwordOptions, null);
        AlterUserCommand alterUserCommand = new AlterUserCommand(alterUserInfo);
        Assertions.assertThrows(AnalysisException.class, () -> alterUserCommand.validate(),
                "Access denied; you need (at least one of) the (GRANT) privilege(s) for this operation");
    }
}
