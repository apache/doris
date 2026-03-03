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

import org.apache.doris.analysis.TablePattern;
import org.apache.doris.analysis.UserDesc;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.AccessPrivilege;
import org.apache.doris.catalog.AccessPrivilegeWithCols;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ShowGrantsCommandTest extends TestWithFeService {
    private Auth auth;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
    }

    public void createUser(String user, String host) throws Exception {
        auth = Env.getCurrentEnv().getAuth();
        TablePattern tablePattern1 = new TablePattern("test", "*");
        List<AccessPrivilegeWithCols> privileges1 = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
        UserIdentity user1 = new UserIdentity(user, host);
        UserDesc userDesc = new UserDesc(user1, "12345", true);

        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(userDesc));
        createUserCommand.getInfo().validate();
        auth.createUser(createUserCommand.getInfo());

        GrantTablePrivilegeCommand grantTablePrivilegeCommand = new GrantTablePrivilegeCommand(privileges1, tablePattern1, Optional.of(user1), Optional.empty());
        try {
            grantTablePrivilegeCommand.validate();
            auth.grantTablePrivilegeCommand(grantTablePrivilegeCommand);
        } catch (AnalysisException e) {
            e.printStackTrace();
        } catch (DdlException e1) {
            e1.printStackTrace();
        }
    }

    @Test
    void testDorun() throws Exception {
        createUser("aaa", "%");
        createUser("aaa", "192.168.%");
        createUser("zzz", "%");

        ShowGrantsCommand sg = new ShowGrantsCommand(null, true);
        ConnectContext ctx = ConnectContext.get();
        ShowResultSet sr = sg.doRun(ctx, null);

        List<List<String>> results = sr.getResultRows();
        Assertions.assertEquals("'aaa'@'%'", results.get(0).get(0));
        Assertions.assertEquals("'aaa'@'192.168.%'", results.get(1).get(0));
        int size = results.size();
        Assertions.assertEquals("'zzz'@'%'", results.get(size - 1).get(0));
    }

    @Test
    void testNonExistUser() {
        ConnectContext ctx = ConnectContext.get();
        UserIdentity nonExistUser = UserIdentity.createAnalyzedUserIdentWithIp("non_exist_user", "%");
        Assertions.assertThrows(AnalysisException.class, () -> {
            ShowGrantsCommand sg = new ShowGrantsCommand(nonExistUser, false);
            sg.doRun(ctx, null);
        });

        ctx.setIsTempUser(true);
        ctx.setCurrentUserIdentity(nonExistUser);
        Assertions.assertDoesNotThrow(() -> {
            ShowGrantsCommand sg = new ShowGrantsCommand(null, false);
            sg.doRun(ctx, null);
        });
    }

    @Test
    void testShowGrantsUseCurrentRolesForSelf() throws Exception {
        String user = "su_show_grants_user_" + System.currentTimeMillis();
        auth = Env.getCurrentEnv().getAuth();
        UserIdentity userIdentity = new UserIdentity(user, "%");
        UserDesc userDesc = new UserDesc(userIdentity, "12345", true);
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(userDesc));
        createUserCommand.getInfo().validate();
        auth.createUser(createUserCommand.getInfo());

        ConnectContext ctx = ConnectContext.get();
        UserIdentity currentUserIdentity = ctx.getCurrentUserIdentity();
        Set<String> currentRoles = ctx.getCurrentRoles() == null ? null : Sets.newHashSet(ctx.getCurrentRoles());
        try {
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(user, "%"));
            ctx.setCurrentRoles(Sets.newHashSet("admin"));

            ShowGrantsCommand sg = new ShowGrantsCommand(null, false);
            ShowResultSet sr = sg.doRun(ctx, null);
            List<List<String>> results = sr.getResultRows();
            Assertions.assertEquals(1, results.size());
            Assertions.assertEquals("admin", results.get(0).get(4));
            Assertions.assertNotEquals("NULL", results.get(0).get(5));
        } finally {
            ctx.setCurrentUserIdentity(currentUserIdentity);
            ctx.setCurrentRoles(currentRoles);
        }
    }

    @Test
    void testShowGrantsUseCurrentRolesForTempSelf() throws Exception {
        ConnectContext ctx = ConnectContext.get();
        UserIdentity currentUserIdentity = ctx.getCurrentUserIdentity();
        Set<String> currentRoles = ctx.getCurrentRoles() == null ? null : Sets.newHashSet(ctx.getCurrentRoles());
        boolean isTempUser = ctx.getIsTempUser();
        try {
            ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp("su_temp_user", "%"));
            ctx.setCurrentRoles(Sets.newHashSet("admin"));
            ctx.setIsTempUser(true);

            ShowGrantsCommand sg = new ShowGrantsCommand(null, false);
            ShowResultSet sr = sg.doRun(ctx, null);
            List<List<String>> results = sr.getResultRows();
            Assertions.assertEquals(1, results.size());
            Assertions.assertEquals("admin", results.get(0).get(4));
            Assertions.assertNotEquals("NULL", results.get(0).get(5));
        } finally {
            ctx.setCurrentUserIdentity(currentUserIdentity);
            ctx.setCurrentRoles(currentRoles);
            ctx.setIsTempUser(isTempUser);
        }
    }
}
