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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/** Runs actual capture, decompile, marker substitution and re-analysis against a test catalog. */
class SPMGroupByOrdinalTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("spm_ordinal_db");
        connectContext.setDatabase("spm_ordinal_db");
        createTable("CREATE TABLE t (k INT, v INT) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        connectContext.getSessionVariable().setEnableSpmFallback(false);
    }

    @Test
    void testCaptureAndReplayOrdinaryGroupingOrdinals() throws Exception {
        for (List<String> queries : List.of(
                List.of("SELECT 1 AS c, COUNT(*) AS n FROM t GROUP BY 1",
                        "SELECT 2 AS c, COUNT(*) AS n FROM t GROUP BY 1"),
                List.of("SELECT k % 2 AS c, COUNT(*) AS n FROM t GROUP BY 1 ORDER BY 1",
                        "SELECT k % 3 AS c, COUNT(*) AS n FROM t GROUP BY 1 ORDER BY 1"),
                List.of("SELECT 1 AS c, COUNT(*) AS n FROM t WHERE k < 0 GROUP BY 1",
                        "SELECT 2 AS c, COUNT(*) AS n FROM t WHERE k < 0 GROUP BY 1"),
                List.of("SELECT k % 2 AS c, v, COUNT(*) AS n FROM t GROUP BY 2, 1"
                                + " HAVING COUNT(*) > 1 ORDER BY 1, 2",
                        "SELECT k % 3 AS c, v, COUNT(*) AS n FROM t GROUP BY 2, 1"
                                + " HAVING COUNT(*) > 1 ORDER BY 1, 2"),
                List.of("SELECT k % 2 AS c, v AS c, COUNT(*) AS n FROM t GROUP BY 2, 1 ORDER BY 1, 2",
                        "SELECT k % 3 AS c, v AS c, COUNT(*) AS n FROM t GROUP BY 2, 1 ORDER BY 1, 2"),
                List.of("SELECT k % 2 AS c, v AS c, COUNT(*) AS n FROM t"
                                + " GROUP BY 2, 1 ORDER BY 1 DESC, 2 LIMIT 2",
                        "SELECT k % 3 AS c, v AS c, COUNT(*) AS n FROM t"
                                + " GROUP BY 2, 1 ORDER BY 1 DESC, 2 LIMIT 2"),
                List.of("SELECT k % 2 AS c, v % 3 AS d, COUNT(*) AS n FROM t GROUP BY 2, 1 ORDER BY 1, 2",
                        "SELECT k % 3 AS c, v % 4 AS d, COUNT(*) AS n FROM t GROUP BY 2, 1 ORDER BY 1, 2"))) {
            String bind = queries.get(0);
            createStatementCtx(bind);
            BaselinePlan baseline = new SPMPlanner().buildBaselineFromSql(connectContext, bind, bind, false);
            Assertions.assertTrue(baseline.getPlanFrozen(), baseline.getPlanSql());
            // A persisted frozen row has no parameterized-plan fallback: require the same
            // path here so a successful raw-tree replay cannot mask a broken decompile.
            baseline.setParameterizedPlanPlan(null);
            long id = connectContext.getSessionBaselineStore().createBaseline(baseline);
            try {
                for (String query : queries) {
                    createStatementCtx(query);
                    SPMPlanner planner = new SPMPlanner();
                    LogicalPlan replay = planner.tryRewritePlan(new NereidsParser().parseSingle(query),
                            System.currentTimeMillis() + 5000);
                    Assertions.assertNotNull(replay, query);
                    Assertions.assertEquals(id, planner.getUsedBaselineId(), query);
                    Assertions.assertFalse(SPMPlanTreeSupport.containsFrozenPlaceholder(replay), query);
                    Assertions.assertNotNull(SPMOptimizer.optimize(connectContext, replay, query, false)
                            .getPhysicalPlan(), query);
                }
            } finally {
                connectContext.getSessionBaselineStore().dropBaseline(id);
            }
        }
    }

    @Test
    void testGroupingSetsWithParameterizedOrdinalStayRaw() throws Exception {
        for (String grouping : List.of("ROLLUP(1)", "CUBE(1)", "GROUPING SETS((1), ())")) {
            String bind = "SELECT k % 2 AS c, COUNT(*) AS n FROM t GROUP BY " + grouping;
            createStatementCtx(bind);
            BaselinePlan baseline = new SPMPlanner().buildBaselineFromSql(connectContext, bind, bind, false);
            Assertions.assertFalse(baseline.getPlanFrozen(), grouping);
            Assertions.assertEquals(bind, baseline.getPlanSql());
        }
    }
}
