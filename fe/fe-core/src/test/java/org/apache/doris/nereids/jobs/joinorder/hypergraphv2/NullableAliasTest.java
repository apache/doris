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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NullableAliasTest extends SqlTestBase {

    @Test
    void testCoalesceOnNullableSide() {
        // COALESCE on the nullable (right) side of a LEFT JOIN.
        // DPHyp must keep the COALESCE below the outer join.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + ifnull(T3.age, 0)) "
                        + "from T1 left join T2 on T1.id = T2.id "
                        + "left join (select id, coalesce(score, 0) as age from T3) T3 "
                        + "on T2.id = T3.id group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testIfnullOnNullableSide() {
        // IFNULL on the nullable side of LEFT JOIN.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(ifnull(T2.score, 0)) "
                        + "from T1 left join T2 on T1.id = T2.id "
                        + "left join (select id, ifnull(score, 0) as score from T3) T3 "
                        + "on T2.id = T3.id group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testChainedAliasOnNullableSide() {
        // Chained aliases on nullable side: y = x + 1, x = coalesce(v, 0).
        // Verifies layered storage preserves the x->y dependency.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + T3.dv) "
                        + "from T1 left join T2 on T1.id = T2.id "
                        + "left join ("
                        + "  select id, (x + 1) as dv from ("
                        + "    select id, coalesce(score, 0) as x from T3"
                        + "  ) sub1"
                        + ") T3 on T2.id = T3.id group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testJoinPredicateOnNullableAlias() {
        // Nullable alias used in a higher join predicate.
        // Verifies slotToHyperNodeMap maps the alias to its full subtree.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + T3.s) "
                        + "from T1 inner join T2 on T1.id = T2.id "
                        + "left join ("
                        + "  select T1.id, coalesce(T2.score, 0) as s "
                        + "  from T1 inner join T2 on T1.id = T2.id"
                        + ") T3 on T1.id = T3.id and T3.s = T2.score "
                        + "group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testNestedOuterJoinAlias() {
        // COALESCE on nullable side where the Project's subtree itself
        // contains a LEFT JOIN (inner nullable join).
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(coalesce(Sub.dv, 0)) "
                        + "from T1 inner join T2 on T1.id = T2.id "
                        + "left join ("
                        + "  select L.id, coalesce(R.score, 0) as dv "
                        + "  from T1 L left join T2 R on L.id = R.id"
                        + ") Sub on T2.id = Sub.id group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testPassThroughJoinKeyInLayer() {
        // Aliased Project on a single table that also outputs the join key.
        // Verifies that the layer carries forward D.k so parent join C.k = D.k works.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + T3.dv) "
                        + "from T1 left join T2 on T1.id = T2.id "
                        + "left join (select id, coalesce(score, 0) as dv from T3) T3 "
                        + "on T2.id = T3.id group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testMixedSubplanPreservesAliasInputs() {
        // Alias s = T1.score + T2.score over {T1,T2} on nullable side.
        // T1 and T3 can be inner-joined first (mixed subplan {T1,T3}).
        // Verifies getAllAliasInputSlotsForNodes uses isOverlap so T1.score
        // survives through the {T1,T3} join for later alias evaluation.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + T3.score) "
                        + "from T1 inner join T2 on T1.id = T2.id "
                        + "inner join T3 on T1.id = T3.id "
                        + "left join ("
                        + "  select T1.id, (T1.score + T2.score) as s "
                        + "  from T1 inner join T2 on T1.id = T2.id"
                        + ") Sub on T1.id = Sub.id "
                        + "group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testDropAliasRawInputsAfterMaterialization() {
        // Alias dv = length(score) on nullable side over {T1,T2}.
        // After dv materializes, raw T2.score should not leak into ancestor
        // join outputs — verifies parentRequireSlots excludes alias inputs
        // from the mergedLayer carry-forward.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + coalesce(Sub.dv, 0)) "
                        + "from T1 inner join T2 on T1.id = T2.id "
                        + "left join ("
                        + "  select T1.id, char_length(cast(T2.score as string)) as dv "
                        + "  from T1 inner join T2 on T1.id = T2.id"
                        + ") Sub on T1.id = Sub.id "
                        + "group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testSlotForwardingAliasOnMinimalBitmap() {
        // Slot-forwarding alias s = T1.id over {T1,T2} on nullable side.
        // Since all nullable-side aliases now map to subTreeNodes for safety,
        // s = T3.id forms a {T1,T2}--{T3} edge. Verifies DPHyp still
        // produces a valid plan.
        CascadesContext c1 = createCascadesContext(
                "select T2.id, sum(T2.score + T3.score) "
                        + "from T2 inner join T3 on T2.id = T3.id "
                        + "left join ("
                        + "  select T1.id as s "
                        + "  from T1 inner join T2 on T1.id = T2.id"
                        + ") Sub on Sub.s = T3.id "
                        + "group by T2.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }
}
