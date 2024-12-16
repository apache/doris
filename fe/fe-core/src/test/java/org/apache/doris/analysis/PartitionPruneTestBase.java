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

import org.apache.doris.utframe.TestWithFeService;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

public abstract class PartitionPruneTestBase extends TestWithFeService {
    protected List<TestCase> cases = new ArrayList<>();

    protected void doTest() throws Exception {
        for (RangePartitionPruneTest.TestCase testCase : cases) {
            assertExplainContains(testCase.sql, testCase.result);
        }
    }

    private void assertExplainContains(String sql, String subString) throws Exception {
        Assert.assertTrue(
                String.format("sql=%s, expectResult=%s, but got %s", sql, subString,
                        getSQLPlanOrErrorMsg("explain " + sql)),
                getSQLPlanOrErrorMsg("explain " + sql).contains(subString));
    }

    protected void addCase(String sql, String result) {
        cases.add(new TestCase(sql, result));
    }

    protected static class TestCase {
        final String sql;
        final String result;

        public TestCase(String sql, String v2Result) {
            this.sql = sql;
            this.result = v2Result;
        }
    }
}
