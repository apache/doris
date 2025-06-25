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

import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.SetType;
import org.apache.doris.catalog.Function;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ShowCreateFunctionCommandTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        connectContext.setDatabase(null);
        createFunction(
                "CREATE GLOBAL ALIAS FUNCTION test_for_create_global_function(bigint) "
                    + "WITH PARAMETER(id) AS CONCAT(LEFT(id,3),'****',RIGHT(id,4));");
        createDatabase("test");
        connectContext.setDatabase("test");
        createFunction(
                "CREATE ALIAS FUNCTION test.test_for_create_function(bigint) "
                    + "WITH PARAMETER(id) AS CONCAT(LEFT(id,3),'****',RIGHT(id,4));");
    }

    @Test
    void testGetFunction() throws Exception {
        // test for no database
        FunctionName fn = new FunctionName("test_for_create_function");
        ShowCreateFunctionCommand sc = new ShowCreateFunctionCommand("", SetType.DEFAULT, fn, null);
        connectContext.setDatabase("");
        ShowCreateFunctionCommand finalSc = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc.getFunction(connectContext));

        // test for test_for_create_function function
        BigIntType bt = BigIntType.INSTANCE;
        List<DataType> aList = new ArrayList<>();
        aList.add(bt);
        FunctionArgTypesInfo fati = new FunctionArgTypesInfo(aList, false);
        sc = new ShowCreateFunctionCommand("test", SetType.DEFAULT, fn, fati);
        sc.handleShowCreateFunction(connectContext, null);
        Function fc = sc.getFunction(connectContext);
        Assertions.assertTrue(fc.getFunctionName().getFunction().equalsIgnoreCase("test_for_create_function"));

        // test for global function
        fn = new FunctionName("test_for_create_global_function");
        sc = new ShowCreateFunctionCommand("", SetType.GLOBAL, fn, fati);
        connectContext.setDatabase("");
        sc.handleShowCreateFunction(connectContext, null);
        fc = sc.getFunction(connectContext);
        Assertions.assertTrue(fc.getFunctionName().getFunction().equalsIgnoreCase("test_for_create_global_function"));
    }

    @Test
    void testAnalyze() throws AnalysisException {
        FunctionName fn = new FunctionName("test_for_create_function");
        BigIntType bt = BigIntType.INSTANCE;
        List<DataType> aList = new ArrayList<>();
        aList.add(bt);
        FunctionArgTypesInfo fatf = new FunctionArgTypesInfo(aList, false);
        ShowCreateFunctionCommand sc = new ShowCreateFunctionCommand("test", SetType.DEFAULT, fn, fatf);
        sc.analyze(connectContext, SetType.DEFAULT);

        // wrong function name
        fn = new FunctionName("test-for@create_function");
        sc = new ShowCreateFunctionCommand("test", SetType.DEFAULT, fn, fatf);
        ShowCreateFunctionCommand finalSc = sc;
        Assertions.assertThrows(AnalysisException.class, () -> finalSc.analyze(connectContext, SetType.DEFAULT));
    }
}
