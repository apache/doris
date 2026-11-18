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

import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.spm.SpmTreeTestAccess;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.SqlModeHelper;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/** Stored syntax reuse must retain parser modes and fresh logical-plan state. */
public class StoredSqlSyntaxCacheTest {
    private static List<String> describe(Plan plan) {
        List<String> result = new ArrayList<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            result.add(node.getClass().getSimpleName());
            node.getExpressions().forEach(expr -> result.add(expr.toSql()));
        });
        result.add(SpmTreeTestAccess.selectHintsFingerprint(plan));
        return result;
    }

    private static LogicalPlan reference(String sql, long mode) {
        return SelectHintSetVar.withoutParseTimeSetVar(() -> SqlModeHelper.withSqlMode(mode,
                () -> new NereidsParser().parseSingle(sql)));
    }

    @Test
    public void testCacheMatchesFreshParserAcrossModesAndHints() {
        boolean ansiBefore = GlobalVariable.enable_ansi_query_organization_behavior;
        try {
            for (boolean ansi : List.of(false, true)) {
                GlobalVariable.enable_ansi_query_organization_behavior = ansi;
                for (long mode : List.of(0L, SqlModeHelper.MODE_ANSI_QUOTES,
                        SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES, SqlModeHelper.MODE_PIPES_AS_CONCAT)) {
                    for (String sql : List.of(
                            "SELECT \"name\", 'a\\nb', 'x' || 'y' FROM t",
                            "WITH c AS (SELECT 1 AS k) SELECT k FROM c",
                            "SELECT /*+ SET_VAR(sql_mode=4) */ k FROM t",
                            "SELECT 1 UNION ALL SELECT 2 ORDER BY 1 LIMIT 1",
                            "SELECT sum(v) OVER (PARTITION BY k ORDER BY v ROWS"
                                    + " BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t")) {
                        List<String> expected = describe(reference(sql, mode));
                        for (int hit = 0; hit < 2; hit++) {
                            Assertions.assertEquals(expected,
                                    describe(SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(sql, mode)),
                                    "mode=" + mode + ", ansi=" + ansi + ": " + sql);
                        }
                    }
                }
            }
        } finally {
            GlobalVariable.enable_ansi_query_organization_behavior = ansiBefore;
        }
    }

    @Test
    public void testPlansAndRelationIdsAreNeverShared() {
        String sql = "WITH c AS (SELECT k FROM t) SELECT k FROM c";
        Plan first = SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(sql, 0L);
        Plan second = SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(sql, 0L);
        first.setMutableState("spm_syntax_isolation", true);
        Assertions.assertTrue(second.getMutableState("spm_syntax_isolation").isEmpty());
        Set<Plan> firstNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<Plan> secondNodes = Collections.newSetFromMap(new IdentityHashMap<>());
        Set<Object> firstIds = new HashSet<>();
        Set<Object> secondIds = new HashSet<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(first, node -> {
            firstNodes.add(node);
            if (node instanceof LogicalRelation) {
                firstIds.add(((LogicalRelation) node).getRelationId());
            }
        });
        SPMPlanTreeSupport.<RuntimeException>walkPlans(second, node -> {
            secondNodes.add(node);
            if (node instanceof LogicalRelation) {
                secondIds.add(((LogicalRelation) node).getRelationId());
            }
        });
        Assertions.assertFalse(firstIds.isEmpty());
        Assertions.assertTrue(Collections.disjoint(firstNodes, secondNodes));
        Assertions.assertTrue(Collections.disjoint(firstIds, secondIds));
    }

    @Test
    public void testConcurrentTemplateReuseBuildsEquivalentIndependentPlans() throws Exception {
        String sql = "SELECT /*+ SET_VAR(enable_decimal256=true) */ k, sum(v)"
                + " FROM t WHERE k > 1 GROUP BY k";
        List<String> expected = describe(reference(sql, 0L));
        ExecutorService pool = Executors.newFixedThreadPool(4);
        try {
            List<Callable<Plan>> tasks = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                tasks.add(() -> SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(sql, 0L));
            }
            Set<Plan> roots = Collections.newSetFromMap(new IdentityHashMap<>());
            for (Future<Plan> future : pool.invokeAll(tasks)) {
                Plan plan = future.get();
                Assertions.assertEquals(expected, describe(plan));
                Assertions.assertTrue(roots.add(plan));
            }
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    public void testSyntaxCacheHasAnExplicitWeightLimit() throws Exception {
        Field field = NereidsParser.class.getDeclaredField("STORED_SYNTAX");
        field.setAccessible(true);
        Cache<?, ?> cache = (Cache<?, ?>) field.get(null);
        Assertions.assertEquals(16L * 1024 * 1024,
                cache.policy().eviction().orElseThrow().getMaximum());
    }
}
