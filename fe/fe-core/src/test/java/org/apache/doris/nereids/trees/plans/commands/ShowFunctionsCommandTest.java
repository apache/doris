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
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowFunctionsCommandTest extends TestWithFeService {
    @Mocked
    private ConnectContext ctx;
    @Mocked
    private Env env;
    @Mocked
    private AccessControllerManager accessControllerManager;

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
        Assertions.assertTrue(re1.get(0).size() > 100);

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
        Assertions.assertTrue(re3.get(0).size() > 100);
        Assertions.assertEquals("", re3.get(1).get(0));
        Assertions.assertEquals("", re3.get(2).get(0));
        Assertions.assertEquals("", re3.get(3).get(0));
        Assertions.assertEquals("", re3.get(4).get(0));

        // test for full not builtin functions
        sf = new ShowFunctionsCommand("test", false, true, null);
        List<String> func4 = sf.getFunctions(connectContext);
        List<List<String>> re4 = sf.getResultRowSetByFunctions(func4);
        Assertions.assertEquals(1, re4.get(0).size());
        Assertions.assertEquals("test_for_create_function", re4.get(0).get(0));
        Assertions.assertEquals("", re4.get(1).get(0));
        Assertions.assertEquals("", re4.get(2).get(0));
        Assertions.assertEquals("", re4.get(3).get(0));
        Assertions.assertEquals("", re4.get(4).get(0));

        // test for full not builtin functions with where condition
        Expression where = new Like(new UnboundSlot(Lists.newArrayList("empty key")),
                new StringLiteral("test_for_create_function%"));
        sf = new ShowFunctionsCommand("test", false, true, where);
        List<String> func5 = sf.getFunctions(connectContext);
        List<List<String>> re5 = sf.getResultRowSetByFunctions(func5);
        Assertions.assertEquals(1, re5.get(0).size());
        Assertions.assertEquals("test_for_create_function", re5.get(0).get(0));
        Assertions.assertEquals("", re5.get(1).get(0));
        Assertions.assertEquals("", re5.get(2).get(0));
        Assertions.assertEquals("", re5.get(3).get(0));
        Assertions.assertEquals("", re5.get(4).get(0));
    }

    @Test
    void testLike() {
        connectContext.setDatabase("test");
        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", true, false, null);
        Assertions.assertTrue(sf.like("test_for_create_function", "test_for_create_function%"));
    }

    @Test
    void testAuth() throws Exception {
        new Expectations() {
            {
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;

                env.getAccessManager();
                minTimes = 0;
                result = accessControllerManager;

                accessControllerManager.checkDbPriv((ConnectContext) any, InternalCatalog.INTERNAL_CATALOG_NAME,
                        "test", PrivPredicate.SHOW);
                minTimes = 0;
                result = true;

                accessControllerManager.checkDbPriv((ConnectContext) any, InternalCatalog.INTERNAL_CATALOG_NAME,
                        "test_no_priv", PrivPredicate.SHOW);
                minTimes = 0;
                result = false;
            }
        };

        ShowFunctionsCommand sf = new ShowFunctionsCommand("test", true, true, null);
        sf.handleShowFunctions(ConnectContext.get(), null);

        ShowFunctionsCommand sfNoPriv = new ShowFunctionsCommand("test_no_priv", true, true, null);
        Assertions.assertThrows(AnalysisException.class, () -> sfNoPriv.handleShowFunctions(ConnectContext.get(), null));
    }
}
