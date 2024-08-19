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

package org.apache.doris.analysis;

import org.apache.doris.common.FeConstants;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ShowCreateFunctionTest extends TestWithFeService {

    private String dbName = "testDb";

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createDatabase(dbName);
        useDatabase(dbName);
        createFunction(
                "CREATE ALIAS FUNCTION id_masking_create(bigint) WITH PARAMETER(id) AS CONCAT(LEFT(id,3),'****',RIGHT(id,4));");

        createFunction(
                "CREATE GLOBAL ALIAS FUNCTION id_masking_global_create(bigint) WITH PARAMETER(id) AS CONCAT(LEFT(id,3),'****',RIGHT(id,4));");
    }


    @Test
    public void testNormal() throws Exception {
        String sql = "SHOW CREATE FUNCTION id_masking_create(bigint)";
        ShowResultSet showResultSet = showCreateFunction(sql);
        String showSql = showResultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(showSql.contains("CREATE ALIAS FUNCTION id_masking_create(bigint) WITH PARAMETER(id)"));
    }

    @Test
    public void testShowCreateGlobalFunction() throws Exception {
        String sql = "SHOW CREATE GLOBAL FUNCTION id_masking_global_create(bigint)";
        ShowResultSet showResultSet = showCreateFunction(sql);
        String showSql = showResultSet.getResultRows().get(0).get(1);
        Assertions.assertTrue(
                showSql.contains("CREATE GLOBAL ALIAS FUNCTION id_masking_global_create(bigint) WITH PARAMETER(id)"));
    }

}
