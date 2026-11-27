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
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.List;

/** Actual builtin binding, capture, decompile and frozen replay in the FE test catalog. */
class SPMContextFunctionsTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("spm_context_db");
        connectContext.setDatabase("spm_context_db");
        createTable("CREATE TABLE t (k INT, event_time DATETIME(6)) DUPLICATE KEY(k)"
                + " DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES('replication_num'='1')");
        connectContext.getSessionVariable().setEnableSpmFallback(false);
    }

    @Test
    void testDirectContextCallsFreezeAndReplay() throws Exception {
        for (String call : List.of("now()", "current_timestamp()", "localtimestamp()", "localtime()",
                "CURRENT_TIMESTAMP", "LOCALTIMESTAMP", "LOCALTIME", "CURRENT_TIME",
                "current_date()", "curdate()", "CURRENT_DATE", "current_time()", "curtime()",
                "utc_timestamp()", "utc_date()", "utc_time()", "unix_timestamp()",
                "now(3)", "now(6)", "now(9)", "now(1 + 2)", "current_timestamp(3)",
                "localtimestamp(3)", "localtime(3)", "utc_timestamp(3)", "utc_timestamp(9)",
                "utc_time(3)", "current_time(3)", "curtime(3)",
                "database()", "schema()", "current_database()", "current_catalog()",
                "current_user()", "CURRENT_USER", "session_user()", "SESSION_USER", "user()",
                "connection_id()", "last_query_id()", "version()")) {
            String bind = "SELECT " + call + " AS c FROM t WHERE k = 1";
            assertFrozen(bind, bind.replace("k = 1", "k = 2"));
        }
    }

    @Test
    void testNestedContextCallsAndPrecisionMatching() throws Exception {
        for (String query : List.of(
                "SELECT date(now()) AS c FROM t WHERE k = 1",
                "SELECT date(now(3)) AS c FROM t WHERE k = 1",
                "SELECT * FROM (VALUES (NOW())) t",
                "SELECT now() AS c",
                "SELECT l.k, r.event_time FROM t l ASOF JOIN t r"
                        + " MATCH_CONDITION(date_add(l.event_time, INTERVAL unix_timestamp() SECOND)"
                        + " >= r.event_time) USING(k) WHERE l.k = 1",
                "SELECT k FROM t WHERE event_time < now() AND k = 1",
                "SELECT c FROM (SELECT now() AS c, k FROM t) s WHERE k = 1",
                "WITH c AS (SELECT now() AS d, k FROM t) SELECT d FROM c WHERE k = 1",
                "SELECT * REPLACE(now() AS event_time) FROM t WHERE k = 1",
                "SELECT (SELECT max(event_time) FROM t WHERE event_time < now()) AS c FROM t WHERE k = 1")) {
            assertFrozen(query, query.replace("k = 1", "k = 2"));
        }
        String query = "SELECT now(3) AS c FROM t WHERE k = 1";
        BaselinePlan baseline = create(query);
        long id = connectContext.getSessionBaselineStore().createBaseline(baseline);
        try {
            String different = "SELECT now(6) AS c FROM t WHERE k = 2";
            createStatementCtx(different);
            Assertions.assertNull(new SPMPlanner().tryRewritePlan(new NereidsParser().parseSingle(different),
                    System.currentTimeMillis() + 5000));
        } finally {
            connectContext.getSessionBaselineStore().dropBaseline(id);
        }
    }

    @Test
    void testFrozenClockUsesEachStatementStartAndCallerZone() throws Exception {
        String bind = "SELECT k FROM t WHERE event_time < now() AND k = 1";
        Field field = ConnectContext.class.getDeclaredField("startTimeInstant");
        field.setAccessible(true);
        Instant original = connectContext.getStartTimeInstant();
        String zone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone("+00:00");
            field.set(connectContext, Instant.parse("2026-01-01T00:00:00Z"));
            BaselinePlan baseline = create(bind);
            baseline.setParameterizedPlanPlan(null);
            long id = connectContext.getSessionBaselineStore().createBaseline(baseline);
            try {
                for (String time : List.of("2026-01-01T00:00:05Z", "2026-01-01T00:00:15Z")) {
                    field.set(connectContext, Instant.parse(time));
                    LogicalPlan replay = replay(bind, id);
                    String physical = SPMOptimizer.optimize(connectContext, replay, bind, false)
                            .getPhysicalPlan().treeString();
                    Assertions.assertTrue(physical.contains(time.substring(0, 19).replace('T', ' ')), physical);
                }
                connectContext.getSessionVariable().setTimeZone("+08:00");
                field.set(connectContext, Instant.parse("2026-01-01T00:00:15Z"));
                String physical = SPMOptimizer.optimize(connectContext, replay(bind, id), bind, false)
                        .getPhysicalPlan().treeString();
                Assertions.assertTrue(physical.contains("2026-01-01 08:00:15"), physical);
            } finally {
                connectContext.getSessionBaselineStore().dropBaseline(id);
            }
        } finally {
            field.set(connectContext, original);
            connectContext.getSessionVariable().setTimeZone(zone);
        }
    }

    @Test
    void testLastQueryIdAndConnectionIdAreNotCreatorConstants() throws Exception {
        String query = "SELECT last_query_id() AS q, connection_id() AS c FROM t WHERE k = 1";
        BaselinePlan baseline = create(query);
        long id = connectContext.getSessionBaselineStore().createBaseline(baseline);
        int oldConnection = connectContext.getConnectionId();
        TUniqueId oldQuery = connectContext.queryId();
        try {
            connectContext.setQueryId(new TUniqueId(3, 4));
            connectContext.setQueryId(new TUniqueId(5, 6));
            connectContext.setConnectionId(12345);
            String physical = SPMOptimizer.optimize(connectContext, replay(query, id), query, false)
                    .getPhysicalPlan().treeString();
            Assertions.assertTrue(physical.contains("12345"), physical);
            Assertions.assertTrue(physical.contains(org.apache.doris.common.util.DebugUtil.printId(new TUniqueId(3, 4))), physical);
        } finally {
            connectContext.setConnectionId(oldConnection);
            connectContext.setQueryId(oldQuery);
            connectContext.getSessionBaselineStore().dropBaseline(id);
        }
    }

    @Test
    void testShadowedRenderedBuiltinSkipsEvenWithCachedRawTree() throws Exception {
        boolean prefer = connectContext.getSessionVariable().preferUdfOverBuiltin;
        try {
            createFunction("CREATE ALIAS FUNCTION now() WITH PARAMETER() AS abs(-42)");
            connectContext.getSessionVariable().preferUdfOverBuiltin = false;
            String query = "SELECT CURRENT_TIMESTAMP AS c FROM t WHERE k = 1";
            createStatementCtx(query);
            BaselinePlan baseline = new SPMPlanner().buildBaselineFromSql(connectContext, query, query, false);
            Assertions.assertTrue(baseline.getPlanFrozen(), baseline.getPlanSql());
            Assertions.assertNotNull(baseline.getParameterizedPlanPlan());
            long id = connectContext.getSessionBaselineStore().createBaseline(baseline);
            try {
                replay(query, id);
                connectContext.getSessionVariable().preferUdfOverBuiltin = true;
                createStatementCtx(query);
                Assertions.assertNull(new SPMPlanner().tryRewritePlan(new NereidsParser().parseSingle(query),
                        System.currentTimeMillis() + 5000));
            } finally {
                connectContext.getSessionBaselineStore().dropBaseline(id);
            }
        } finally {
            connectContext.getSessionVariable().preferUdfOverBuiltin = prefer;
        }
    }

    private BaselinePlan create(String query) throws Exception {
        createStatementCtx(query);
        BaselinePlan baseline = new SPMPlanner().buildBaselineFromSql(connectContext, query, query, false);
        Assertions.assertTrue(baseline.getPlanFrozen(), query + ": " + baseline.getPlanSql());
        baseline.setParameterizedPlanPlan(null);
        return baseline;
    }

    private LogicalPlan replay(String query, long id) {
        createStatementCtx(query);
        SPMPlanner planner = new SPMPlanner();
        LogicalPlan replay = planner.tryRewritePlan(new NereidsParser().parseSingle(query),
                System.currentTimeMillis() + 5000);
        Assertions.assertNotNull(replay, query);
        Assertions.assertEquals(id, planner.getUsedBaselineId(), query);
        Assertions.assertFalse(SPMContextFunctions.counts(replay).isEmpty(), query);
        return replay;
    }

    private void assertFrozen(String query, String variant) throws Exception {
        BaselinePlan baseline = create(query);
        long id = connectContext.getSessionBaselineStore().createBaseline(baseline);
        try {
            Assertions.assertNotNull(SPMOptimizer.optimize(connectContext, replay(query, id), query, false)
                    .getPhysicalPlan());
            Assertions.assertNotNull(SPMOptimizer.optimize(connectContext, replay(variant, id), variant, false)
                    .getPhysicalPlan());
        } finally {
            connectContext.getSessionBaselineStore().dropBaseline(id);
        }
    }
}
