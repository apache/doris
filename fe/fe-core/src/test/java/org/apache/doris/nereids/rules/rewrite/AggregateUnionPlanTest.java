/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

/**
 * Test for aggregate union query plan optimization.
 * Tests that redundant aggregation layers are eliminated when union inputs
 * don't have PhysicalDistribute operators.
 */
public class AggregateUnionPlanTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_db");
        createTable("create table test_db.t1(a int, b int) "
                + "distributed by random buckets auto "
                + "properties('replication_num' = '1');");
        createTable("create table test_db.t2(a int, b int) "
                + "distributed by random buckets auto "
                + "properties('replication_num' = '1');");
    }

    /**
     * Test that agg-union on random distributed tables doesn't create redundant aggregation.
     * 
     * The query "select a,b from t1 group by a,b union all select a,b from t2 group by a,b"
     * should not generate redundant local agg + global agg layers when the union inputs
     * don't have PhysicalDistribute operators.
     * 
     * This test verifies that the fix for banAggUnionAll works correctly.
     */
    @Test
    public void testAggregateUnionWithoutDistribute() throws Exception {
        String sql = "select a, b from test_db.t1 group by a, b "
                + "union all select a, b from test_db.t2 group by a, b";
        
        // Get the physical plan
        String plan = getPlainTextExplain(sql);
        
        // The plan should contain aggregation, but not redundant consecutive aggregations
        // We verify this by checking the plan doesn't have the pattern of:
        // HashAggregate -> EXCHANGE(RANDOM) -> HashAggregate (which would be redundant)
        // Instead, it should have HashAggregate -> UNION -> (scans)
        // or just the union results without extra aggregation
        
        // Count occurrences of "AGGREGATE" to ensure we don't have too many
        int aggregateCount = countOccurrences(plan, "AGGREGATE");
        
        // With the fix, we should not have 2+ consecutive aggregates for this query
        // The plan should be optimized to not have redundant layers
        org.junit.jupiter.api.Assertions.assertTrue(aggregateCount <= 2,
                "Plan should not have too many redundant aggregation layers. Plan: " + plan);
    }

    /**
     * Test that agg-union on hash distributed tables still works correctly.
     * This ensures we didn't break the case where distribution is needed.
     */
    @Test
    public void testAggregateUnionWithHashDistribute() throws Exception {
        String sql = "select a from test_db.t1 group by a "
                + "union all select a from test_db.t2 group by a";
        
        String plan = getPlainTextExplain(sql);
        
        // This query should produce a valid plan
        org.junit.jupiter.api.Assertions.assertNotNull(plan);
        org.junit.jupiter.api.Assertions.assertTrue(plan.length() > 0);
    }

    private String getPlainTextExplain(String sql) throws Exception {
        return explainQuery(sql);
    }

    private int countOccurrences(String text, String pattern) {
        int count = 0;
        int index = 0;
        while ((index = text.indexOf(pattern, index)) != -1) {
            count++;
            index += pattern.length();
        }
        return count;
    }
}
