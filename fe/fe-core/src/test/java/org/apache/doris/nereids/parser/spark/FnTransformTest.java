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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Spark SQL to Doris function mapping test.
 */
public class FnTransformTest extends ParserTestBase {

    @Test
    public void testCommonFnTransform() {
        NereidsParser nereidsParser = new NereidsParser();

        String sql1 = "SELECT json_extract('{\"a\": 1}', '$.a') as b FROM t";
        String dialectSql1 = "SELECT get_json_object('{\"a\": 1}', '$.a') as b FROM t";
        LogicalPlan logicalPlan1 = nereidsParser.parseSingle(sql1);
        LogicalPlan dialectLogicalPlan1 = nereidsParser.parseSingle(dialectSql1,
                    new SparkSql3LogicalPlanBuilder());
        Assertions.assertEquals(dialectLogicalPlan1, logicalPlan1);
        Assertions.assertTrue(dialectLogicalPlan1.child(0).toString().toLowerCase()
                    .contains("json_extract('{\"a\": 1}', '$.a')"));

        String sql2 = "SELECT json_extract(a, '$.a') as b FROM t";
        String dialectSql2 = "SELECT get_json_object(a, '$.a') as b FROM t";
        LogicalPlan logicalPlan2 = nereidsParser.parseSingle(sql2);
        LogicalPlan dialectLogicalPlan2 = nereidsParser.parseSingle(dialectSql2,
                new SparkSql3LogicalPlanBuilder());
        Assertions.assertEquals(dialectLogicalPlan2, logicalPlan2);
        Assertions.assertTrue(dialectLogicalPlan2.child(0).toString().toLowerCase()
                    .contains("json_extract('a, '$.a')"));
    }

}
