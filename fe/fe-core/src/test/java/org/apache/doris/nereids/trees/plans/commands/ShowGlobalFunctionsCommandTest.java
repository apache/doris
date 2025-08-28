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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ShowGlobalFunctionsCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createFunction(
                "CREATE GLOBAL ALIAS FUNCTION test.test_for_create_function(bigint) "
                            + "WITH PARAMETER(id) AS CONCAT(LEFT(id,3),'****',RIGHT(id,4));");
    }

    @Test
    void testGetFunctions() throws AnalysisException {
        // test for not global
        ShowFunctionsCommand sf = new ShowFunctionsCommand(false, null, false);
        ShowFunctionsCommand finalSf = sf;
        Assertions.assertThrows(AnalysisException.class, () -> finalSf.getFunctions(connectContext));

        // test for verbose
        connectContext.setDatabase("test");
        sf = new ShowFunctionsCommand(true, null, true);
        List<String> re1 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re1.size());
        Assertions.assertEquals("test_for_create_function", re1.get(0));

        // test for no verbose
        sf = new ShowFunctionsCommand(false, null, true);
        List<String> re2 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re2.size());
        Assertions.assertEquals("test_for_create_function", re2.get(0));

        // test for like condition
        sf = new ShowFunctionsCommand(false, "test_for_create%", true);
        List<String> re3 = sf.getFunctions(connectContext);
        Assertions.assertEquals(1, re3.size());
        Assertions.assertEquals("test_for_create_function", re3.get(0));
    }

    @Test
    void testGetResultRowSetByFunctions() throws AnalysisException {
        connectContext.setDatabase("test");
        // test for verbose
        ShowFunctionsCommand sf = new ShowFunctionsCommand(true, null, true);
        List<String> func3 = sf.getFunctions(connectContext);
        List<List<String>> re3 = sf.getResultRowSetByFunctions(func3);
        Assertions.assertEquals(1, re3.size());
        Assertions.assertEquals("", re3.get(0).get(1));
        Assertions.assertEquals("", re3.get(0).get(2));
        Assertions.assertEquals("", re3.get(0).get(3));
        Assertions.assertEquals("", re3.get(0).get(4));

        // test for not verbose
        sf = new ShowFunctionsCommand(false, null, true);
        List<String> func4 = sf.getFunctions(connectContext);
        List<List<String>> re4 = sf.getResultRowSetByFunctions(func4);
        Assertions.assertEquals(1, re4.get(0).size());
        Assertions.assertEquals("test_for_create_function", re4.get(0).get(0));

        // test for like condition
        String where = "test_for_create_function%";
        sf = new ShowFunctionsCommand(true, where, true);
        List<String> func5 = sf.getFunctions(connectContext);
        List<List<String>> re5 = sf.getResultRowSetByFunctions(func5);
        Assertions.assertEquals(5, re5.get(0).size());
        Assertions.assertEquals("test_for_create_function", re5.get(0).get(0));
        Assertions.assertEquals("", re5.get(0).get(1));
        Assertions.assertEquals("", re5.get(0).get(2));
        Assertions.assertEquals("", re5.get(0).get(3));
        Assertions.assertEquals("", re5.get(0).get(4));
    }
}
