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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.info.CreateUserInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * show builtin functions command test
 */
public class ShowBuiltinFunctionsCommandTest extends TestWithFeService {
    private static final int MIN_EXPECTED_BUILTIN_FUNCTION_COUNT = 774;
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
        // test for not full builtin functions
        connectContext.setDatabase("");
        ShowBuiltinFunctionsCommand sbf1 = new ShowBuiltinFunctionsCommand(false, null);
        List<String> re1 = sbf1.getFunctions(connectContext);
        Assertions.assertTrue(re1.size() >= MIN_EXPECTED_BUILTIN_FUNCTION_COUNT);

        // test for full builtin functions
        ShowBuiltinFunctionsCommand sbf2 = new ShowBuiltinFunctionsCommand(true, null);
        List<String> re3 = sbf2.getFunctions(connectContext);
        Assertions.assertTrue(re3.size() >= MIN_EXPECTED_BUILTIN_FUNCTION_COUNT);
    }

    @Test
    void testGetResultRowSetByFunctions() throws AnalysisException {
        // test for full builtin functions
        connectContext.setDatabase("");
        ShowBuiltinFunctionsCommand sbf2 = new ShowBuiltinFunctionsCommand(true, null);
        List<String> func3 = sbf2.getFunctions(connectContext);
        List<List<String>> re3 = sbf2.getResultRowSetByFunctions(func3);
        Assertions.assertTrue(re3.size() >= MIN_EXPECTED_BUILTIN_FUNCTION_COUNT);
        for (List<String> funcItem : re3) {
            Assertions.assertEquals("", funcItem.get(1));
            Assertions.assertEquals("", funcItem.get(2));
            Assertions.assertEquals("", funcItem.get(3));
            Assertions.assertEquals("", funcItem.get(4));
        }
    }

    @Test
    void testLike() {
        connectContext.setDatabase("");
        ShowBuiltinFunctionsCommand sbf = new ShowBuiltinFunctionsCommand(false, null);
        Assertions.assertTrue(sbf.like("year_of_week", "year_of_week%"));
    }

    @Test
    void testAuth() throws Exception {
        auth = Env.getCurrentEnv().getAuth();
        UserIdentity noPrivUser = new UserIdentity("cmy", "%");
        UserDesc userDesc = new UserDesc(noPrivUser, "12345", true);
        CreateUserCommand createUserCommand = new CreateUserCommand(new CreateUserInfo(userDesc));
        createUserCommand.getInfo().validate();
        auth.createUser(createUserCommand.getInfo());
        ConnectContext ctx = ConnectContext.get();
        ctx.setCurrentUserIdentity(noPrivUser);
        // user1 have no any privilege
        ShowBuiltinFunctionsCommand sbf = new ShowBuiltinFunctionsCommand(false, null);
        ShowResultSet showResultSet = sbf.handleShowBuiltinFunctions(ctx, null);
        List<List<String>> ret = showResultSet.getResultRows();
        Assertions.assertTrue(ret.size() >= MIN_EXPECTED_BUILTIN_FUNCTION_COUNT);
    }
}
