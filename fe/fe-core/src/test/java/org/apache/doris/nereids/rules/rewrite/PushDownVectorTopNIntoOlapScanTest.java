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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class PushDownVectorTopNIntoOlapScanTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("vector_topn");
        connectContext.setDatabase("vector_topn");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE vector_source (id INT, vec ARRAY<FLOAT> NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        for (String metric : new String[] {"l2_distance", "inner_product"}) {
            createTable("CREATE TABLE vector_" + metric + " (id INT, vec ARRAY<FLOAT> NOT NULL, "
                    + "INDEX ann_vec (vec) USING ANN PROPERTIES ('index_type' = 'hnsw', "
                    + "'metric_type' = '" + metric + "', 'dim' = '2')) "
                    + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "inner_product"})
    void testNullableWrapperPreservesAnnTopN(String metric) {
        for (String vector : new String[] {"vec", "nullable(vec)", "nullable(nullable(vec))"}) {
            for (String filter : new String[] {"", " WHERE id > 0"}) {
                String query = "SELECT id, " + metric + "_approximate(" + vector + ", [1.0, 2.0]) AS distance"
                        + " FROM vector_" + metric + filter + " ORDER BY distance "
                        + (metric.equals("inner_product") ? "DESC" : "ASC") + " LIMIT 3 OFFSET 1";
                Plan plan = PlanChecker.from(connectContext).analyze(query).rewrite()
                        .getCascadesContext().getRewritePlan();
                LogicalOlapScan scan = (LogicalOlapScan) plan.collectFirst(LogicalOlapScan.class::isInstance).get();
                Assertions.assertEquals(4L, scan.getAnnLimit().orElse(-1L), plan.treeString());
                Alias distance = (Alias) scan.getVirtualColumns().get(0);
                // The BE ANN runtime consumes a direct slot, while the original query may retain nullable outputs.
                Assertions.assertInstanceOf(SlotReference.class, distance.child().child(0), plan.treeString());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "inner_product"})
    void testCastsRetainScalarEvaluation(String metric) {
        String cast = "CAST(CAST(vec AS ARRAY<DOUBLE>) AS ARRAY<FLOAT>)";
        for (String vector : new String[] {"nullable(" + cast + ")", cast}) {
            for (String filter : new String[] {"", " WHERE id > 0"}) {
                String query = "SELECT id, " + metric + "_approximate(" + vector + ", [1.0, 2.0]) AS distance"
                        + " FROM vector_" + metric + filter + " ORDER BY distance "
                        + (metric.equals("inner_product") ? "DESC" : "ASC") + " LIMIT 3";
                Plan plan = PlanChecker.from(connectContext).analyze(query).rewrite()
                        .getCascadesContext().getRewritePlan();
                LogicalOlapScan scan = (LogicalOlapScan) plan.collectFirst(LogicalOlapScan.class::isInstance).get();
                // ANN evaluates indexed slots, so a surviving cast must stay in the scalar expression.
                Assertions.assertTrue(scan.getAnnLimit().isEmpty(), plan.treeString());
                Assertions.assertTrue(scan.getVirtualColumns().isEmpty(), plan.treeString());
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "inner_product"})
    void testVectorTopNRequestsEarlyMvRewrite(String metric) {
        String previous = connectContext.getSessionVariable().getPreMaterializedViewRewriteStrategy();
        try {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("TRY_IN_RBO");
            CascadesContext context = PlanChecker.from(connectContext).analyze("SELECT id, " + metric
                    + "_approximate(vec, [1.0, 2.0]) AS distance FROM vector_source ORDER BY distance LIMIT 3")
                    .rewrite().getCascadesContext();
            Plan plan = context.getRewritePlan();
            StatementContext statement = context.getStatementContext();
            statement.getCandidateMTMVs().add(Mockito.mock(MTMV.class));
            statement.getPlannerHooks().add(InitMaterializationContextHook.INSTANCE);
            statement.addTmpPlanForMvRewrite(plan);
            statement.getNeedPreMvRewriteRuleMasks().clear();
            Assertions.assertTrue(PreMaterializedViewRewriter.needPreRewrite(context));

            Plan scalar = PlanChecker.from(connectContext).analyze("SELECT id FROM vector_source ORDER BY id LIMIT 3")
                    .rewrite().getCascadesContext().getRewritePlan();
            context.setRewritePlan(scalar);
            Assertions.assertFalse(PreMaterializedViewRewriter.needPreRewrite(context));
            context.setRewritePlan(plan);
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("NOT_IN_RBO");
            Assertions.assertFalse(PreMaterializedViewRewriter.needPreRewrite(context));
        } finally {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(previous);
        }
    }
}
