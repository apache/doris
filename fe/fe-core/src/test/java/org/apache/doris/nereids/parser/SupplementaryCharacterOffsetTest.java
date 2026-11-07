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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.properties.SelectHintOrdered;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * The parser counts a supplementary character such as an emoji as one character, while the Java
 * string of the SQL counts it as two. These cases cut the SQL text after such characters.
 */
class SupplementaryCharacterOffsetTest extends ParserTestBase {
    private final NereidsParser parser = new NereidsParser();

    @Test
    void recognizesHintAfterSupplementaryCharacter() {
        LogicalPlan plan = parser.parseSingle("/* 😀 */ SELECT /*+ ORDERED */ a FROM t");
        List<LogicalSelectHint<Plan>> hintPlans = plan.collectToList(LogicalSelectHint.class::isInstance);

        Assertions.assertEquals(1, hintPlans.size());
        Assertions.assertTrue(hintPlans.get(0).getHints().stream()
                .anyMatch(SelectHintOrdered.class::isInstance));
    }

    @Test
    void extractsExplainPlanBodyAfterSupplementaryCharacters() {
        Assertions.assertEquals("SELECT 100",
                NereidsParser.tryParseExplainPlan("EXPLAIN /* abc */ PLAN SELECT 100").orElseThrow().second);
        Assertions.assertEquals("SELECT 100",
                NereidsParser.tryParseExplainPlan("EXPLAIN /* 😀😀😀 */ PLAN SELECT 100").orElseThrow().second);
    }

    @Test
    void rewritesSyncMvAggregateAfterSupplementaryCharacter() {
        Assertions.assertEquals("SELECT /* a */ k1, avg_union(avg_state(k2)) FROM t GROUP BY k1",
                parser.parseForSyncMv("CREATE MATERIALIZED VIEW mv AS "
                        + "SELECT /* a */ k1, avg(k2) FROM t GROUP BY k1").orElseThrow());
        Assertions.assertEquals("SELECT /* 😀 */ k1, avg_union(avg_state(k2)) FROM t GROUP BY k1",
                parser.parseForSyncMv("CREATE MATERIALIZED VIEW mv AS "
                        + "SELECT /* 😀 */ k1, avg(k2) FROM t GROUP BY k1").orElseThrow());
    }
}
