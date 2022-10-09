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

package org.apache.doris.nereids.util;

import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class RegisterCTETest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        SSBUtils.createTables(this);
    }

    @Test
    public void test() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("WITH cte1 AS (\n"
                    + "  \tSELECT s_suppkey\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 AS (\n"
                    + "  \tSELECT s_suppkey\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE s_suppkey < 3\n"
                    + ")\n"
                    + "SELECT *\n"
                    + "FROM cte1, cte2");
    }

    @Test
    public void test2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 (s_suppkey) AS (\n"
                    + "  \tSELECT s_suppkey\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 (s_suppkey) AS (\n"
                    + "  \tSELECT s_suppkey\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE s_suppkey < 3\n"
                    + ")\n"
                    + "SELECT *\n"
                    + "FROM cte1, cte2");
    }
}
