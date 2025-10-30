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
        ShowFunctionsCommand sf = new ShowFunctionsCommand("", true, false, null);
        connectContext.setDatabase("");
        ShowFunctionsCommand finalSf = sf;
        Assertions.assertThrows(AnalysisException.class, () -> finalSf.getFunctions(connectContext));

        // test for builtin functions
        connectContext.setDatabase("test");  // reset database
        sf = new ShowFunctionsCommand("test", true, false, null);
        List<String> re1 = sf.getFunctions(connectContext);
        Assertions.assertTrue(re1.size() > 100);

        // test for not builtin functions
        sf = new ShowFunctionsCommand("test", false, false, null);
        List<String> re2 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re2.size());
        Assertions.assertEquals("test_for_create_function", re2.get(0));

        // test for full builtin functions
        sf = new ShowFunctionsCommand("test", true, true, null);
        List<String> re3 = sf.getFunctions(connectContext);
        Assertions.assertTrue(re3.size() > 100);

        // test for full not builtin functions
        sf = new ShowFunctionsCommand("test", false, true, null);
        List<String> re4 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re4.size());
    }

    @Test
    void testGetResultRowSetByFunctions() throws AnalysisException {
        // test for builtin functions
        connectContext.setDatabase("test");
        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", true, false, null);
        List<String> func1 = sf.getFunctions(connectContext);
        List<List<String>> re1 = sf.getResultRowSetByFunctions(func1);
        Assertions.assertTrue(re1.size() > 100);

        // test for not builtin functions
        sf = new ShowFunctionsCommand("test", false, false, null);
        List<String> func2 = sf.getFunctions(connectContext);
        List<List<String>> re2 = sf.getResultRowSetByFunctions(func2);
        Assertions.assertEquals(1, re2.get(0).size());
        Assertions.assertEquals("test_for_create_function", re2.get(0).get(0));

        // test for full builtin functions
        sf = new ShowFunctionsCommand("test", true, true, null);
        List<String> func3 = sf.getFunctions(connectContext);
        List<List<String>> re3 = sf.getResultRowSetByFunctions(func3);
        Assertions.assertTrue(re3.size() > 100);
        Assertions.assertEquals("", re3.get(0).get(1));
        Assertions.assertEquals("", re3.get(0).get(2));
        Assertions.assertEquals("", re3.get(0).get(3));
        Assertions.assertEquals("", re3.get(0).get(4));

        // test for full not builtin functions
        sf = new ShowFunctionsCommand("test", false, true, null);
        List<String> func4 = sf.getFunctions(connectContext);
        List<List<String>> re4 = sf.getResultRowSetByFunctions(func4);
        Assertions.assertEquals(5, re4.get(0).size());
        Assertions.assertEquals("test_for_create_function", re4.get(0).get(0));
        Assertions.assertEquals("", re4.get(0).get(1));
        Assertions.assertEquals("", re4.get(0).get(2));
        Assertions.assertEquals("", re4.get(0).get(3));
        Assertions.assertEquals("", re4.get(0).get(4));

        // test for full not builtin functions with where condition
        String where = "test_for_create_function%";
        sf = new ShowFunctionsCommand("test", false, true, where);
        List<String> func5 = sf.getFunctions(connectContext);
        List<List<String>> re5 = sf.getResultRowSetByFunctions(func5);
        Assertions.assertEquals(5, re5.get(0).size());
        Assertions.assertEquals("test_for_create_function", re5.get(0).get(0));
        Assertions.assertEquals("", re5.get(0).get(1));
        Assertions.assertEquals("", re5.get(0).get(2));
        Assertions.assertEquals("", re5.get(0).get(3));
        Assertions.assertEquals("", re5.get(0).get(4));
    }

    @Test
    void testLike() {
        connectContext.setDatabase("test");
        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", true, false, null);
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
        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", true, true, null);
        sf.handleShowFunctions(ctx, null);

        // but user1 have not select privilege of db test_no_priv
        ShowFunctionsCommand sfNoPriv = new ShowFunctionsCommand("test_no_priv", true, true, null);
        Assertions.assertThrows(AnalysisException.class, () -> sfNoPriv.handleShowFunctions(ctx, null));
    }
}
