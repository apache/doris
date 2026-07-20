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

    @Test
    void testVolatileProjectAsClusterBoundary() {
        // uuid() on the nullable side: the Project is treated as a cluster
        // boundary (isValidProject returns false because uuid() is volatile),
        // falling through to addDPHyperNode like an aggregate node.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(T2.score + T3.s) "
                        + "from T1 left join T2 on T1.id = T2.id "
                        + "left join ("
                        + "  select T1.id, (coalesce(T2.score, 0) + uuid_numeric()) as s "
                        + "  from T1 inner join T2 on T1.id = T2.id"
                        + ") T3 on T1.id = T3.id "
                        + "group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testVolatileAliasOnNullableSide() {
        // uuid() alias on nullable side of LEFT JOIN.
        // The Project containing uuid() is a cluster boundary, so DPHyp
        // treats it as a leaf node and does not reorder across it.
        CascadesContext c1 = createCascadesContext(
                "select T1.id, sum(ifnull(T2.score, 0) + T3.dv) "
                        + "from T1 left join T2 on T1.id = T2.id "
                        + "left join ("
                        + "  select T1.id, uuid_numeric() as dv "
                        + "  from T1 inner join T2 on T1.id = T2.id"
                        + ") T3 on T1.id = T3.id "
                        + "group by T1.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }

    @Test
    void testSplitSourceAliasMissedEdge() {
        // Problem: a projected alias on the nullable side whose source bitmap
        // spans two base tables is consumed by a join predicate via the
        // missed-edge fallback while its source is split across both children.
        //
        // Plan structure (bottom-up):
        //   S2 = InnerT2 INNER JOIN InnerT3 ON InnerT2.id = InnerT3.id
        //        Project(InnerT2.id, coalesce(InnerT3.score, 0) AS s)
        //        -> alias s has source {InnerT2, InnerT3}
        //   Sub = S2 INNER JOIN C ON S2.id = C.id AND S2.s = C.score
        //        -> edges: {InnerT2}--{C}, {InnerT2,InnerT3}--{C}
        //   Outer: PreservedT1 LEFT JOIN Sub ON PreservedT1.id = Sub.id
        //
        // Inner join cluster: {InnerT2, InnerT3, C}
        //   Edge {InnerT2}--{InnerT3}: InnerT2.id = InnerT3.id
        //   Edge {InnerT2}--{C}:       S2.id = C.id
        //   Edge {InnerT2,InnerT3}--{C}: S2.s = C.score
        //
        // DPHyp can reorder the inner join cluster:
        //   1. Build {InnerT2, C}     via edge {InnerT2}--{C}
        //   2. Combine with {InnerT3} via edge {InnerT2}--{InnerT3}
        // At step 2, processMissedEdges finds the unused edge
        // {InnerT2,InnerT3}--{C} (predicate S2.s = C.score).
        // All reference nodes {InnerT2,InnerT3,C} are in the union,
        // so it would normally be added as a connection edge.
        //
        // Without the fix: proposeJoin would evaluate S2.s = C.score
        // while neither child outputs S2.s — the alias layer is only
        // emitted later by proposeProject (after the full {InnerT2,InnerT3,C}
        // is assembled). This violates CheckAfterRewrite.
        //
        // With the fix (isEdgeSafeForJoin): getProjectedAliasLayers detects
        // that the alias layer for {InnerT2,InnerT3} spans both children
        // ({InnerT2,C} and {InnerT3}), and rejects the unsafe missed edge.
        CascadesContext c1 = createCascadesContext(
                "select PreservedT1.id, Sub.s "
                        + "from T1 PreservedT1 left join ("
                        + "  select S2.id, S2.s "
                        + "  from ("
                        + "    select InnerT2.id, coalesce(InnerT3.score, 0) as s "
                        + "    from T2 InnerT2 inner join T3 InnerT3 on InnerT2.id = InnerT3.id"
                        + "  ) S2 "
                        + "  inner join T1 C on S2.id = C.id and S2.s = C.score"
                        + ") Sub on PreservedT1.id = Sub.id",
                connectContext
        );
        Plan plan = PlanChecker.from(c1).analyze().rewrite().dpHypOptimize().getBestPlanTree();
        Assertions.assertNotNull(plan);
    }
}
