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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public class PartitionPruneTestBase {
    protected static String runningDir;
    protected static ConnectContext connectContext;

    protected List<TestCase> cases = new ArrayList<>();

    protected void doTest() throws Exception {
        for (RangePartitionPruneTest.TestCase testCase : cases) {
            connectContext.getSessionVariable().partitionPruneAlgorithmVersion = 1;
            assertExplainContains(1, testCase.sql, testCase.v1Result);
            connectContext.getSessionVariable().partitionPruneAlgorithmVersion = 2;
            assertExplainContains(2, testCase.sql, testCase.v2Result);
        }
    }

    protected static void createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseAndAnalyzeStmt(sql, connectContext);
        Catalog.getCurrentCatalog().createTable(createTableStmt);
    }

    private void assertExplainContains(int version, String sql, String subString) throws Exception {
        Assert.assertTrue(String.format("version=%d, sql=%s, expectResult=%s",
            version, sql, subString),
            UtFrameUtils.getSQLPlanOrErrorMsg(connectContext, "explain " + sql)
                .contains(subString));
    }

    protected void addCase(String sql, String v1Result, String v2Result) {
        cases.add(new TestCase(sql, v1Result, v2Result));
    }

    protected static class TestCase {
        final String sql;
        final String v1Result;
        final String v2Result;

        public TestCase(String sql, String v1Result, String v2Result) {
            this.sql = sql;
            this.v1Result = v1Result;
            this.v2Result = v2Result;
        }
    }
}
