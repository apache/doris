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
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.HyperGraphComparator;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.Predicates;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.ExpressionInfo;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.Map;

/** Test the method in Predicates*/
public class PredicatesTest extends SqlTestBase {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("predicates_test");
        useDatabase("predicates_test");

        createTables(
                "CREATE TABLE IF NOT EXISTS T1 (\n"
                        + "    id bigint,\n"
                        + "    score bigint\n"
                        + ")\n"
                        + "DUPLICATE KEY(id)\n"
                        + "DISTRIBUTED BY HASH(id, score) BUCKETS 10\n"
                        + "PROPERTIES (\n"
                        + "  \"replication_num\" = \"1\", \n"
                        + "  \"colocate_with\" = \"T0\"\n"
                        + ")\n"
        );

        // Should not make scan to empty relation when the table used by materialized view has no data
        connectContext.getSessionVariable().setDisableNereidsRules(
                "OLAP_SCAN_PARTITION_PRUNE"
                        + ",PRUNE_EMPTY_PARTITION"
                        + ",ELIMINATE_GROUP_BY_KEY_BY_UNIFORM"
                        + ",ELIMINATE_CONST_JOIN_CONDITION"
                        + ",CONSTANT_PROPAGATION"
                        + ",INFER_PREDICATES"
        );
    }

    @Test
    public void testCompensateCouldNotPullUpPredicatesFail() {
        CascadesContext mvContext = createCascadesContext(
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 10 and id < 5;",
                connectContext
        );
        Plan mvPlan = PlanChecker.from(mvContext)
                .analyze()
                .rewrite()
                .getPlan().child(0);

        CascadesContext queryContext = createCascadesContext(
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 10 and id < 1;",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .getAllPlan().get(0).child(0);

        StructInfo mvStructInfo = MaterializedViewUtils.extractStructInfo(mvPlan, mvPlan,
                mvContext, new BitSet()).get(0);
        StructInfo queryStructInfo = MaterializedViewUtils.extractStructInfo(queryPlan, queryPlan,
                queryContext, new BitSet()).get(0);
        RelationMapping relationMapping = RelationMapping.generate(mvStructInfo.getRelations(),
                        queryStructInfo.getRelations(), 16).get(0);

        SlotMapping mvToQuerySlotMapping = SlotMapping.generate(relationMapping);
        ComparisonResult comparisonResult = HyperGraphComparator.isLogicCompatible(
                queryStructInfo.getHyperGraph(),
                mvStructInfo.getHyperGraph(),
                constructContext(queryPlan, mvPlan, queryContext));

        Map<Expression, ExpressionInfo> expressionExpressionInfoMap = Predicates.compensateCouldNotPullUpPredicates(
                queryStructInfo, mvStructInfo, mvToQuerySlotMapping, comparisonResult);
        Assertions.assertNull(expressionExpressionInfoMap);
    }

    @Test
    public void testCompensateCouldNotPullUpPredicatesSuccess() {
        CascadesContext mvContext = createCascadesContext(
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 10 and id < 5;",
                connectContext
        );
        Plan mvPlan = PlanChecker.from(mvContext)
                .analyze()
                .rewrite()
                .getPlan().child(0);

        CascadesContext queryContext = createCascadesContext(
                "select \n"
                        + "id,\n"
                        + "FIRST_VALUE(id) OVER (\n"
                        + "        PARTITION BY score \n"
                        + "        ORDER BY score NULLS LAST\n"
                        + "        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW\n"
                        + "    ) AS first_value\n"
                        + "from \n"
                        + "T1\n"
                        + "where score > 15 and id < 5;",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .getAllPlan().get(0).child(0);

        StructInfo mvStructInfo = MaterializedViewUtils.extractStructInfo(mvPlan, mvPlan,
                mvContext, new BitSet()).get(0);
        StructInfo queryStructInfo = MaterializedViewUtils.extractStructInfo(queryPlan, queryPlan,
                queryContext, new BitSet()).get(0);
        RelationMapping relationMapping = RelationMapping.generate(mvStructInfo.getRelations(),
                queryStructInfo.getRelations(), 16).get(0);

        SlotMapping mvToQuerySlotMapping = SlotMapping.generate(relationMapping);
        ComparisonResult comparisonResult = HyperGraphComparator.isLogicCompatible(
                queryStructInfo.getHyperGraph(),
                mvStructInfo.getHyperGraph(),
                constructContext(queryPlan, mvPlan, queryContext));

        Map<Expression, ExpressionInfo> compensateCouldNotPullUpPredicates = Predicates.compensateCouldNotPullUpPredicates(
                queryStructInfo, mvStructInfo, mvToQuerySlotMapping, comparisonResult);
        Assertions.assertNotNull(compensateCouldNotPullUpPredicates);
        Assertions.assertTrue(compensateCouldNotPullUpPredicates.isEmpty());

        Map<Expression, ExpressionInfo> compensateRangePredicates = Predicates.compensateRangePredicate(
                queryStructInfo, mvStructInfo, mvToQuerySlotMapping, comparisonResult,
                queryContext);
        Assertions.assertNotNull(compensateRangePredicates);
        Assertions.assertEquals(1, compensateRangePredicates.size());
    }
}
