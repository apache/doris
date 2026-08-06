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

package org.apache.doris.mysql.privilege;

import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.MysqlPassword;
import org.apache.doris.nereids.trees.plans.commands.CreateUserCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.nereids.trees.plans.commands.info.SetPassVarOp;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class SetPasswordTest {

    private Auth auth;
    private Env env = Mockito.mock(Env.class);
    private EditLog editLog = Mockito.mock(EditLog.class);
    private MockedStatic<Env> mockedEnvStatic;
    private MockedStatic<MysqlPassword> mockedMysqlPassword;

    @Before
    public void setUp() throws NoSuchMethodException, SecurityException, AnalysisException {
        auth = new Auth();
        mockedEnvStatic = Mockito.mockStatic(Env.class);
        mockedMysqlPassword = Mockito.mockStatic(MysqlPassword.class);

        mockedEnvStatic.when(Env::getCurrentEnv).thenReturn(env);
        Mockito.when(env.getAuth()).thenReturn(auth);
        Mockito.when(env.getEditLog()).thenReturn(editLog);
        mockedMysqlPassword.when(() -> MysqlPassword.checkPassword(Mockito.anyString())).thenReturn(new byte[10]);
    }

    @After
    public void tearDown() {
        mockedEnvStatic.close();
        mockedMysqlPassword.close();
    }

    @Test
    public void test() throws DdlException, AnalysisException {
        UserIdentity userIdentity = new UserIdentity("cmy", "%");
        userIdentity.setIsAnalyzed();
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(new UserDesc(userIdentity)));
        auth.createUser(createUserCommand.getInfo());

        ConnectContext ctx = new ConnectContext();
        // set password for 'cmy'@'%'
        UserIdentity currentUser1 = new UserIdentity("cmy", "%");
        currentUser1.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser1);
        ctx.setThreadLocalInfo();

        UserIdentity user1 = new UserIdentity("cmy", "%");
        user1.setIsAnalyzed();
        SetPassVarOp setPassVarOp = new SetPassVarOp(user1, null);
        try {
            setPassVarOp.validate(ctx);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // set password without for
        SetPassVarOp setPassVarOp2 = new SetPassVarOp(null, null);
        try {
            setPassVarOp2.validate(ctx);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // create user cmy2@'192.168.1.1'
        UserIdentity userIdentity2 = new UserIdentity("cmy2", "192.168.1.1");
        userIdentity2.setIsAnalyzed();
        CreateUserCommand createUserCommand1 = new CreateUserCommand(new CreateUserInfo(new UserDesc(userIdentity2)));
        auth.createUser(createUserCommand1.getInfo());

        UserIdentity currentUser2 = new UserIdentity("cmy2", "192.168.1.1");
        currentUser2.setIsAnalyzed();
        ctx.setCurrentUserIdentity(currentUser2);
        ctx.setThreadLocalInfo();

        // set password without for
        SetPassVarOp setPassVarOp3 = new SetPassVarOp(null, null);
        try {
            setPassVarOp3.validate(ctx);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }

        // set password for cmy2@'192.168.1.1'
        UserIdentity user2 = new UserIdentity("cmy2", "192.168.1.1");
        user2.setIsAnalyzed();
        SetPassVarOp setPassVarOp4 = new SetPassVarOp(user2, null);
        try {
            setPassVarOp4.validate(ctx);
        } catch (UserException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }
}
