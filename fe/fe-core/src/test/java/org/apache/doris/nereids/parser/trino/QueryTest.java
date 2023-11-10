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

package org.apache.doris.nereids.parser.trino;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Test;

/**
 * Trino query tests.
 */
public class QueryTest extends ParserTestBase {

    @Test
    public void testParseCast1() {
        String sql = "SELECT CAST(1 AS DECIMAL(20, 6)) FROM t";
        NereidsParser nereidsParser = new NereidsParser();
        LogicalPlan logicalPlan = nereidsParser.parseSingle(sql);
        trinoDialectParsePlan(sql).assertEquals(logicalPlan);

        sql = "SELECT CAST(a AS DECIMAL(20, 6)) FROM t";
        logicalPlan = nereidsParser.parseSingle(sql);
        trinoDialectParsePlan(sql).assertEquals(logicalPlan);
    }
}
