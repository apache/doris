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
import org.apache.doris.catalog.Function;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class ShowFunctionsCommandTest extends TestWithFeService {
    private Auth auth;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createDatabase("test_no_priv");
        connectContext.setDatabase("test");
        createFunction(
                "CREATE ALIAS FUNCTION test.test_for_create_function(bigint) WITH PARAMETER(id) AS CONCAT(LEFT(id,3),'****',RIGHT(id,4));");
    }

    @Test
    void testGetFunctions() throws AnalysisException {
        // test No database selected
        ShowFunctionsCommand sf = new ShowFunctionsCommand("", false, null);
        connectContext.setDatabase("");
        ShowFunctionsCommand finalSf = sf;
        Assertions.assertThrows(AnalysisException.class, () -> finalSf.getFunctions(connectContext));

        // test for not builtin functions
        sf = new ShowFunctionsCommand("test", false, null);
        List<Function> re2 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re2.size());
        Assertions.assertEquals("test_for_create_function", re2.get(0).functionName());

        // test for full not builtin functions
        sf = new ShowFunctionsCommand("test", true, null);
        List<Function> re4 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re4.size());
    }

    @Test
    void testGetResultRowSetByFunctions() throws AnalysisException {
        // test for not builtin functions
        ShowFunctionsCommand sf;
        connectContext.setDatabase("test");
        sf = new ShowFunctionsCommand("test", false, null);
        List<Function> func2 = sf.getFunctions(connectContext);
        List<List<String>> re2 = sf.getResultRowSetByFunctions(func2);
        Assertions.assertEquals(1, re2.get(0).size());
        Assertions.assertEquals("test_for_create_function", re2.get(0).get(0));

        // test for full not builtin functions
        connectContext.setDatabase("test");
        sf = new ShowFunctionsCommand("test", true, null);
        List<Function> func4 = sf.getFunctions(connectContext);
        List<List<String>> re4 = sf.getResultRowSetByFunctions(func4);
        Assertions.assertEquals(5, re4.get(0).size());
        Assertions.assertTrue(re4.get(0).get(0).startsWith("test_for_create_function"));
        Assertions.assertFalse(re4.get(0).get(1).isEmpty());
        Assertions.assertFalse(re4.get(0).get(2).isEmpty());
        Assertions.assertFalse(re4.get(0).get(4).isEmpty());

        // test for full not builtin functions with where condition
        String where = "test_for_create_function%";
        sf = new ShowFunctionsCommand("test", true, where);
        List<Function> func5 = sf.getFunctions(connectContext);
        List<List<String>> re5 = sf.getResultRowSetByFunctions(func5);
        Assertions.assertEquals(5, re5.get(0).size());
        Assertions.assertTrue(re5.get(0).get(0).startsWith("test_for_create_function"));
        Assertions.assertFalse(re5.get(0).get(1).isEmpty());
        Assertions.assertFalse(re5.get(0).get(2).isEmpty());
        Assertions.assertFalse(re5.get(0).get(4).isEmpty());
    }

    @Test
    void testLike() {
        connectContext.setDatabase("test");
        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", false, null);
        Assertions.assertTrue(sf.like("test_for_create_function", "test_for_create_function%"));
    }

    @Test
    void testAuth() throws Exception {
        auth = Env.getCurrentEnv().getAuth();
        TablePattern tablePattern1 = new TablePattern("test", "*");
        List<AccessPrivilegeWithCols> privileges1 = Lists
                .newArrayList(new AccessPrivilegeWithCols(AccessPrivilege.SELECT_PRIV));
        UserIdentity user1 = new UserIdentity("cmy", "%");
        UserDesc userDesc = new UserDesc(user1, "12345", true);

        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(userDesc));
        createUserCommand.getInfo().validate();
        auth.createUser(createUserCommand.getInfo());

        GrantTablePrivilegeCommand grantTablePrivilegeCommand = new GrantTablePrivilegeCommand(privileges1, tablePattern1, Optional.of(user1), Optional.empty());

        try {
            grantTablePrivilegeCommand.validate();
            auth.grantTablePrivilegeCommand(grantTablePrivilegeCommand);
        } catch (UserException e) {
            e.printStackTrace();
        }

        ConnectContext ctx = ConnectContext.get();
        ctx.setCurrentUserIdentity(user1);

        // user1 have select privilege of db test
        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", true, null);
        sf.handleShowFunctions(ctx, null);

        // but user1 have not select privilege of db test_no_priv
        ShowFunctionsCommand sfNoPriv = new ShowFunctionsCommand("test_no_priv", true, null);
        Assertions.assertThrows(AnalysisException.class, () -> sfNoPriv.handleShowFunctions(ctx, null));
    }
}
