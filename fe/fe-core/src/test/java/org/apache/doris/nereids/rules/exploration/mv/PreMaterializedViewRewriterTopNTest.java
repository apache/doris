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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

class PreMaterializedViewRewriterTopNTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("pre_mv_topn");
        connectContext.setDatabase("pre_mv_topn");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTable("CREATE TABLE vector_source (id INT, vec ARRAY<FLOAT> NULL) "
                + "DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')");
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "inner_product"})
    void testUnrelatedOrderKeyDoesNotRequestEarlyRewrite(String metric) {
        String previous = connectContext.getSessionVariable().getPreMaterializedViewRewriteStrategy();
        try {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("TRY_IN_RBO");
            CascadesContext context = prepareCandidate("SELECT id, " + metric
                    + "_approximate(vec, [1.0, 2.0]) AS distance FROM vector_source ORDER BY id LIMIT 3");
            Assertions.assertFalse(PreMaterializedViewRewriter.needPreRewrite(context),
                    context.getRewritePlan().treeString());
        } finally {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(previous);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "inner_product"})
    void testNestedApproximateOrderDoesNotRequestEarlyRewrite(String metric) {
        String previous = connectContext.getSessionVariable().getPreMaterializedViewRewriteStrategy();
        try {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("TRY_IN_RBO");
            CascadesContext context = prepareCandidate("SELECT id, abs(" + metric
                    + "_approximate(vec, [1.0, 2.0])) AS distance FROM vector_source ORDER BY distance LIMIT 3");
            Assertions.assertFalse(PreMaterializedViewRewriter.needPreRewrite(context),
                    context.getRewritePlan().treeString());
        } finally {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(previous);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"l2_distance", "inner_product"})
    void testApproximateOrderHonorsPreRewriteStrategy(String metric) {
        String previous = connectContext.getSessionVariable().getPreMaterializedViewRewriteStrategy();
        try {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("TRY_IN_RBO");
            CascadesContext context = prepareCandidate("SELECT id, " + metric
                    + "_approximate(vec, [1.0, 2.0]) AS distance FROM vector_source ORDER BY distance "
                    + (metric.equals("inner_product") ? "DESC" : "ASC") + " LIMIT 3");
            Assertions.assertTrue(PreMaterializedViewRewriter.needPreRewrite(context),
                    context.getRewritePlan().treeString());
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("NOT_IN_RBO");
            Assertions.assertFalse(PreMaterializedViewRewriter.needPreRewrite(context));
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy("FORCE_IN_RBO");
            Assertions.assertTrue(PreMaterializedViewRewriter.needPreRewrite(context));
        } finally {
            connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(previous);
        }
    }

    private CascadesContext prepareCandidate(String query) {
        CascadesContext context = PlanChecker.from(connectContext).analyze(query).rewrite().getCascadesContext();
        StatementContext statement = context.getStatementContext();
        statement.getCandidateMTMVs().add(Mockito.mock(MTMV.class));
        statement.getPlannerHooks().add(InitMaterializationContextHook.INSTANCE);
        statement.addTmpPlanForMvRewrite(context.getRewritePlan());
        statement.getNeedPreMvRewriteRuleMasks().clear();
        return context;
    }
}
