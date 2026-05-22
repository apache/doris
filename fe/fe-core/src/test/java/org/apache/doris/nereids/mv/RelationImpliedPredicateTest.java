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

package org.apache.doris.nereids.mv;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.PreMaterializedViewRewriter.PreRewriteStrategy;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests MV rewrite when an MV scan carries predicates implied by its definition.
 */
public class RelationImpliedPredicateTest extends SqlTestBase {

    @Test
    void testGenerateRelationImpliedPredicateOnMvScan() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        try {
            createMvByNereids("create materialized view mv_relation_implied_fact "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select T1.id from T1 where T1.score = 1;");
            mockCandidateMtmv("mv_relation_implied_fact");

            CascadesContext context = createCascadesContext(
                    "select T1.id from T1 where T1.score = 1",
                    connectContext);
            PlanChecker.from(context)
                    .setIsQuery()
                    .analyze()
                    .rewrite();

            Assertions.assertEquals(1, context.getMaterializationContexts().size());
            MaterializationContext materializationContext = context.getMaterializationContexts().get(0);
            Plan scanPlan = materializationContext.getScanPlan(null, context);
            Assertions.assertNotNull(scanPlan);
            Optional<LogicalOlapScan> scan = scanPlan.collectFirst(LogicalOlapScan.class::isInstance);
            Assertions.assertTrue(scan.isPresent(), scanPlan::treeString);
            Assertions.assertEquals(1, scan.get().getRelationImpliedPredicates().size());
            String predicates = scan.get().getRelationImpliedPredicates().toString();
            Assertions.assertTrue(predicates.contains("score") && predicates.contains("1"), predicates);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        }
    }

    @Test
    void testNestedMvRewriteByRelationImpliedPredicate() throws Exception {
        initMvRewriteSession();
        dropMvByNereids("drop materialized view if exists mv_relation_implied_join");
        dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        try {
            createMvByNereids("create materialized view mv_relation_implied_fact "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select T1.id from T1 where T1.score = 1;");
            mockCandidateMtmv("mv_relation_implied_fact");

            createMvByNereids("create materialized view mv_relation_implied_join "
                    + "BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                    + "DISTRIBUTED BY RANDOM BUCKETS 1\n"
                    + "PROPERTIES ('replication_num' = '1') \n"
                    + "as select f.id, T2.score as dim_score "
                    + "from mv_relation_implied_fact f "
                    + "inner join T2 on f.id = T2.id;");
            mockCandidateMtmv("mv_relation_implied_join");

            CascadesContext context = createCascadesContext(
                    "select T1.id, T2.score as dim_score "
                            + "from T1 inner join T2 on T1.id = T2.id "
                            + "where T1.score = 1",
                    connectContext);
            PhysicalPlan bestPlan = PlanChecker.from(context)
                    .setIsQuery()
                    .analyze()
                    .rewrite()
                    .preMvRewrite()
                    .optimize()
                    .getBestPlanTree();
            String plan = bestPlan.treeString();
            Assertions.assertTrue(plan.contains("mv_relation_implied_join"), plan);
        } finally {
            dropMvByNereids("drop materialized view if exists mv_relation_implied_join");
            dropMvByNereids("drop materialized view if exists mv_relation_implied_fact");
        }
    }

    private void initMvRewriteSession() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        installValidRelationManager();
        connectContext.getState().setIsQuery(true);
        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        connectContext.getSessionVariable().setPreMaterializedViewRewriteStrategy(PreRewriteStrategy.NOT_IN_RBO.name());
        connectContext.getSessionVariable().materializedViewRewriteDurationThresholdMs = 1000000;
    }
}
