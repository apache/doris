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

package org.apache.doris.nereids.parser.spark;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Spark SQL to Doris function mapping test.
 */
public class FnTransformTest extends ParserTestBase {

    @Test
    public void testCommonFnTransformers() {
        // test json functions
        testFunction("SELECT json_extract('{\"c1\": 1}', '$.c1') as b FROM t",
                    "SELECT get_json_object('{\"c1\": 1}', '$.c1') as b FROM t",
                    "json_extract('{\"c1\": 1}', '$.c1')");

        testFunction("SELECT json_extract(c1, '$.c1') as b FROM t",
                    "SELECT get_json_object(c1, '$.c1') as b FROM t",
                    "json_extract('c1, '$.c1')");

        // test string functions
        testFunction("SELECT str_to_date('2023-12-16', 'yyyy-MM-dd') as b FROM t",
                    "SELECT to_date('2023-12-16', 'yyyy-MM-dd') as b FROM t",
                    "str_to_date('2023-12-16', 'yyyy-MM-dd')");
        testFunction("SELECT str_to_date(c1, 'yyyy-MM-dd') as b FROM t",
                "SELECT to_date(c1, 'yyyy-MM-dd') as b FROM t",
                "str_to_date('c1, 'yyyy-MM-dd')");

        testFunction("SELECT date_trunc('2023-12-16', 'YEAR') as a FROM t",
                    "SELECT trunc('2023-12-16', 'YEAR') as a FROM t",
                    "date_trunc('2023-12-16', 'YEAR')");
        testFunction("SELECT date_trunc(c1, 'YEAR') as a FROM t",
                "SELECT trunc(c1, 'YEAR') as a FROM t",
                "date_trunc('c1, 'YEAR')");

        testFunction("SELECT date_trunc('2023-12-16', 'YEAR') as a FROM t",
                    "SELECT trunc('2023-12-16', 'YY') as a FROM t",
                    "date_trunc('2023-12-16', 'YEAR')");
        testFunction("SELECT date_trunc(c1, 'YEAR') as a FROM t",
                "SELECT trunc(c1, 'YY') as a FROM t",
                "date_trunc('c1, 'YEAR')");

        testFunction("SELECT date_trunc('2023-12-16', 'MONTH') as a FROM t",
                    "SELECT trunc('2023-12-16', 'MON') as a FROM t",
                    "date_trunc('2023-12-16', 'MONTH')");
        testFunction("SELECT date_trunc(c1, 'MONTH') as a FROM t",
                "SELECT trunc(c1, 'MON') as a FROM t",
                "date_trunc('c1, 'MONTH')");

        // test numeric functions
        testFunction("SELECT avg(c1) as a from t",
                    "SELECT mean(c1) as a from t",
                    "avg('c1)");
    }

    private void testFunction(String sql, String dialectSql, String expectLogicalPlanStr) {
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        LogicalPlan dialectLogicalPlan = nereidsParser.parseSingle(dialectSql,
                new SparkSql3LogicalPlanBuilder());
        Assertions.assertEquals(dialectLogicalPlan, logicalPlan);
        String dialectLogicalPlanStr = dialectLogicalPlan.child(0).toString().toLowerCase();
        System.out.println("dialectLogicalPlanStr: " + dialectLogicalPlanStr);
        Assertions.assertTrue(StringUtils.containsIgnoreCase(dialectLogicalPlanStr, expectLogicalPlanStr));
    }
}
