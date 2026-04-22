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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVRelationManager;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.node.AbstractNode;
import org.apache.doris.nereids.rules.exploration.mv.AsyncMaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Relevant test case about mtmv cache.
 */
public class MTMVCacheTest extends SqlTestBase {

    @Test
    void testMTMVCacheIsCorrect() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx, boolean forceConsistent,
                    Map<List<String>, Set<String>> queryUsedPartitions) {
                return true;
            }
        };

        new MockUp<MTMV>() {
            @Mock
            public boolean canBeCandidate() {
                return true;
            }
        };
        connectContext.getState().setIsQuery(true);

        connectContext.getSessionVariable().enableMaterializedViewRewrite = true;
        connectContext.getSessionVariable().enableMaterializedViewNestRewrite = true;
        createMvByNereids("create materialized view mv1 BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1') \n"
                + "        as select T1.id, sum(score) from T1 group by T1.id;");
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(score) from T1 group by T1.id;",
                connectContext
        );
        PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .optimize()
                .printlnBestPlanTree();
        List<MaterializationContext> normalMaterializationContexts = c1.getMaterializationContexts();
        Assertions.assertEquals(1, normalMaterializationContexts.size());

        MTMV mtmv = ((AsyncMaterializationContext) normalMaterializationContexts.get(0)).getMtmv();
        MTMVCache cacheWithoutGuard = mtmv.getOrGenerateCache(connectContext);

        Optional<LogicalAggregate<? extends Plan>> aggregate = cacheWithoutGuard.getAllRulesRewrittenPlanAndStructInfo().key()
                .collectFirst(LogicalAggregate.class::isInstance);
        Assertions.assertTrue(aggregate.isPresent());
        // should not contain SessionVarGuardExpr
        Assertions.assertTrue(aggregate.get().getOutputExpressions().stream()
                .noneMatch(expr -> expr.containsType(SessionVarGuardExpr.class)));

        // set guard check session var
        connectContext.getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_UNSIGNED_SUBTRACTION);
        CascadesContext c2 = createCascadesContext(
                "select T1.id, sum(score) from T1 group by T1.id;",
                connectContext
        );
        connectContext.getState().setIsQuery(true);
        PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .optimize()
                .printlnBestPlanTree();

        List<MaterializationContext> sessionChangedMaterializationContexts = c2.getMaterializationContexts();
        Assertions.assertEquals(1, sessionChangedMaterializationContexts.size());

        MTMV mvWithGuard = ((AsyncMaterializationContext) sessionChangedMaterializationContexts.get(0)).getMtmv();
        MTMVCache cacheWithGuard = mvWithGuard.getOrGenerateCache(connectContext);

        aggregate = cacheWithGuard.getAllRulesRewrittenPlanAndStructInfo().key()
                .collectFirst(LogicalAggregate.class::isInstance);
        Assertions.assertTrue(aggregate.isPresent());
        // should contain SessionVarGuardExpr
        Assertions.assertTrue(aggregate.get().getOutputExpressions().stream()
                .anyMatch(expr -> expr.containsType(SessionVarGuardExpr.class)));
        dropMvByNereids("drop materialized view mv1");
    }

    @Test
    void testMTMVCacheKeepsUniqueKeysTraitFromMvHint() throws Exception {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        BitSet disableNereidsRules = connectContext.getSessionVariable().getDisableNereidsRules();
        new MockUp<SessionVariable>() {
            @Mock
            public BitSet getDisableNereidsRules() {
                return disableNereidsRules;
            }
        };
        new MockUp<MTMVRelationManager>() {
            @Mock
            public boolean isMVPartitionValid(MTMV mtmv, ConnectContext ctx, boolean forceConsistent,
                    Map<List<String>, Set<String>> queryUsedPartitions) {
                return true;
            }
        };

        new MockUp<MTMV>() {
            @Mock
            public boolean canBeCandidate() {
                return true;
            }
        };
        createTable("CREATE TABLE IF NOT EXISTS uk_hint_fact_sales (\n"
                + "    sale_id BIGINT,\n"
                + "    customer_id INT,\n"
                + "    product_id INT,\n"
                + "    sale_date DATE,\n"
                + "    quantity INT,\n"
                + "    amount DECIMAL(10,2)\n"
                + ")\n"
                + "DUPLICATE KEY(sale_id)\n"
                + "DISTRIBUTED BY HASH(sale_id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")");
        createTable("CREATE TABLE IF NOT EXISTS uk_hint_dim_customer (\n"
                + "    customer_id INT,\n"
                + "    customer_name VARCHAR(50),\n"
                + "    city VARCHAR(50),\n"
                + "    country VARCHAR(50)\n"
                + ")\n"
                + "DUPLICATE KEY(customer_id)\n"
                + "DISTRIBUTED BY HASH(customer_id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")");
        createTable("CREATE TABLE IF NOT EXISTS uk_hint_dim_product (\n"
                + "    product_id INT,\n"
                + "    product_name VARCHAR(50),\n"
                + "    category VARCHAR(50),\n"
                + "    price DECIMAL(10,2)\n"
                + ")\n"
                + "DUPLICATE KEY(product_id)\n"
                + "DISTRIBUTED BY HASH(product_id) BUCKETS 1\n"
                + "PROPERTIES (\"replication_num\" = \"1\")");

        createMvByNereids("create materialized view uk_hint_mv_fact_sales BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1')\n"
                + "        as select sale_id, customer_id, product_id, sale_date, quantity, amount\n"
                + "        from uk_hint_fact_sales;");
        createMvByNereids("create materialized view uk_hint_mv_dim_customer BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1')\n"
                + "        as select customer_id, customer_name, city, country\n"
                + "        from uk_hint_dim_customer;");
        createMvByNereids("create materialized view uk_hint_mv_dim_product BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1')\n"
                + "        as select product_id, product_name, category, price\n"
                + "        from uk_hint_dim_product;");
        createMvByNereids("create materialized view uk_hint_mv_joined_sales BUILD DEFERRED REFRESH COMPLETE ON MANUAL\n"
                + "        DISTRIBUTED BY RANDOM BUCKETS 1\n"
                + "        PROPERTIES ('replication_num' = '1')\n"
                + "        as select /*+ UNIQUE_KEYS('internal.test.uk_hint_mv_dim_customer'='customer_id',\n"
                + "                                 'internal.test.uk_hint_mv_dim_product'='product_id') */\n"
                + "               f.sale_id, f.sale_date, f.quantity, f.amount,\n"
                + "               c.customer_name, c.city, c.country,\n"
                + "               p.product_name, p.category, p.price\n"
                + "        from uk_hint_mv_fact_sales f\n"
                + "        left join uk_hint_mv_dim_customer c on f.customer_id = c.customer_id\n"
                + "        left join uk_hint_mv_dim_product p on f.product_id = p.product_id;");

        Database db = Env.getCurrentEnv().getInternalCatalog().getDbOrAnalysisException("test");
        MTMV mtmv = (MTMV) db.getTableOrAnalysisException("uk_hint_mv_joined_sales");

        connectContext.setStatementContext(new StatementContext(connectContext, new OriginStatement("select 1", 0)));
        MTMVCache cache = mtmv.getOrGenerateCache(connectContext);

        // Simulate later rewrite-time access under a different statement context with no UNIQUE_KEYS hints.
        connectContext.setStatementContext(new StatementContext(connectContext,
                new OriginStatement("select * from test.uk_hint_fact_sales", 0)));

        StructInfo structInfo = cache.getAllRulesRewrittenPlanAndStructInfo().value();
        Plan dimProductPlan = structInfo.getHyperGraph().getNodes().stream()
                .map(AbstractNode::getPlan)
                .filter(plan -> plan.getOutput().stream().anyMatch(slot -> slot.getName().equalsIgnoreCase("product_id"))
                        && plan.getOutput().stream().anyMatch(slot -> slot.getName().equalsIgnoreCase("product_name")))
                .findFirst()
                .orElseThrow(() -> new AssertionError("failed to find uk_hint_mv_dim_product node in MV cache"));
        Slot productIdSlot = dimProductPlan.getOutput().stream()
                .filter(slot -> slot.getName().equalsIgnoreCase("product_id"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("failed to find product_id slot in MV cache node"));

        Assertions.assertTrue(dimProductPlan.getLogicalProperties().getTrait().isUnique(productIdSlot));
    }
}
