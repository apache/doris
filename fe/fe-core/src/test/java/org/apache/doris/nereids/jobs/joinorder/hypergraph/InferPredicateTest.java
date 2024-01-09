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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.AbstractMaterializedViewRule;
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.HyperGraphComparator;
import org.apache.doris.nereids.rules.exploration.mv.LogicalCompatibilityContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InferPredicateTest extends SqlTestBase {
    @Test
    void testPullUpQueryFilter() {
        CascadesContext c1 = createCascadesContext(
                "select * from T1 left join T2 on T1.id = T2.id where T1.id = 1",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals("(id = 1)", res.getQueryExpressions().get(0).toSql());
    }

    LogicalCompatibilityContext constructContext(Plan p1, Plan p2) {
        StructInfo st1 = AbstractMaterializedViewRule.extractStructInfo(p1,
                null).get(0);
        StructInfo st2 = AbstractMaterializedViewRule.extractStructInfo(p2,
                null).get(0);
        RelationMapping rm = RelationMapping.generate(st1.getRelations(), st2.getRelations()).get(0);
        SlotMapping sm = SlotMapping.generate(rm);
        return LogicalCompatibilityContext.from(rm, sm, st1, st2);
    }
}
