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

import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CreateUserCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        ConnectContext ctx = new ConnectContext();
        ctx.setRemoteIP("192.168.1.1");
        UserIdentity currentUserIdentity = new UserIdentity("root", "192.168.1.1");
        currentUserIdentity.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUserIdentity);
        ctx.setThreadLocalInfo();
    }

    @Test
    public void testValidateNormal(@Mocked AccessControllerManager accessManager) {
        new Expectations() {
            {
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.GRANT);
                result = true;
            }
        };

        CreateUserCommand command = new CreateUserCommand(new CreateUserInfo(new UserDesc(new UserIdentity("user", "%"), "passwd", true)));
        CreateUserInfo info = command.getInfo();
        Assertions.assertDoesNotThrow(() -> info.validate());
        Assertions.assertEquals(new String(info.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");

        command = new CreateUserCommand(new CreateUserInfo(new UserDesc(new UserIdentity("user", "%"), "*59c70da2f3e3a5bdf46b68f5c8b8f25762bccef0", false)));
        CreateUserInfo info1 = command.getInfo();
        Assertions.assertDoesNotThrow(() -> info1.validate());
        Assertions.assertEquals("user", info1.getUserIdent().getQualifiedUser());
        Assertions.assertEquals(new String(info1.getPassword()), "*59C70DA2F3E3A5BDF46B68F5C8B8F25762BCCEF0");

        command = new CreateUserCommand(new CreateUserInfo(new UserDesc(new UserIdentity("user", "%"), "", false)));
        CreateUserInfo info2 = command.getInfo();
        Assertions.assertDoesNotThrow(() -> info2.validate());
        Assertions.assertEquals(new String(info2.getPassword()), "");
    }

    @Test
    public void testEmptyUser() {
        CreateUserCommand command = new CreateUserCommand(new CreateUserInfo(new UserDesc(new UserIdentity("", "%"), "passwd", true)));
        Assertions.assertThrows(AnalysisException.class,
                () -> command.getInfo().validate(),
                "Does not support anonymous user");
    }

    @Test
    public void testBadPass() {
        CreateUserCommand command = new CreateUserCommand(new CreateUserInfo(new UserDesc(new UserIdentity("user", "%"), "passwd", false)));
        Assertions.assertThrows(AnalysisException.class,
                () -> command.getInfo().validate(),
                "Password hash should be a 41-digit hexadecimal number");
    }
}
