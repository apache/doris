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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class RegisterCTETest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        useDatabase("test");
        SSBUtils.createTables(this);
    }

    @Test
    public void cte_test_1() {
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
    public void cte_test_2() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 (skey, sname) AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 (sk2) AS (\n"
                    + "  \tSELECT skey\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE skey < 3\n"
                    + ")\n"
                    + "SELECT *\n"
                    + "FROM cte1, cte2");
    }

    @Test
    public void cte_test_3() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_suppkey < 5\n"
                    + "), cte2 AS (\n"
                    + "  \tSELECT s_suppkey\n"
                    + "  \tFROM cte1\n"
                    + "  \tWHERE s_suppkey < 3\n"
                    + ")\n"
                    + "  SELECT *\n"
                    + "  FROM supplier\n"
                    + "  WHERE s_suppkey in (select s_suppkey from cte2)");
    }

    @Test
    public void cte_test_4() {
        PlanChecker.from(connectContext)
                .checkPlannerResult("  WITH cte1 AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "), cte2 AS (\n"
                    + "  \tSELECT *\n"
                    + "  \tFROM supplier\n"
                    + "  \tWHERE s_region in (\"ASIA\", \"AFRICA\")\n"
                    + ")\n"
                    + "  SELECT s_region, count(*)\n"
                    + "  FROM cte1\n"
                    + "  GROUP BY s_region\n"
                    + "  HAVING s_region in (SELECT s_region FROM cte2)");
    }

    @Test
    public void cte_exception_test_1() {
        try {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 (skey, SKEY) AS (\n"
                        + "  \tSELECT s_suppkey, s_name\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertTrue(e.getMessage().contains("Duplicated CTE column alias"));
        }
    }

    @Test
    public void cte_exception_test_2() {
        try {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 (skey, sname) AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertTrue(e.getMessage().contains("The number of column labels must be "
                    + "smaller or equal to the number of returned columns"));
        }
    }

    @Test
    public void cte_exception_test_3() {
        try {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM cte2\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + "), cte2 (sk2) AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 3\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1, cte2");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof RuntimeException);
            Assertions.assertTrue(e.getMessage().contains("does not exist in database"));
        }
    }

    @Test
    public void cte_exception_test_4() {
        try {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + "), cte2 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM not_existed_table\n"
                        + "  \tWHERE s_suppkey < 3\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof RuntimeException);
            Assertions.assertTrue(e.getMessage().contains("does not exist in database"));
        }
    }

    @Test
    public void cte_exception_test_5() {
        try {
            PlanChecker.from(connectContext)
                    .checkPlannerResult("  WITH cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 5\n"
                        + "), cte1 AS (\n"
                        + "  \tSELECT s_suppkey\n"
                        + "  \tFROM supplier\n"
                        + "  \tWHERE s_suppkey < 3\n"
                        + ")\n"
                        + "SELECT *\n"
                        + "FROM cte1");
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof AnalysisException);
            Assertions.assertTrue(e.getMessage().contains("cannot be used more than once"));
        }
    }
}
