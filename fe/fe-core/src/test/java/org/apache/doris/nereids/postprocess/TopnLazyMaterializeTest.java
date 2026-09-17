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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.analysis.ColumnAccessPath;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessors;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.MaterializationNode;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TopnLazyMaterializeTest extends SSBTestBase {

    @Override
    public void runBeforeAll() throws Exception {
        super.runBeforeAll();
        connectContext.getSessionVariable().setRuntimeFilterMode("Global");
        connectContext.getSessionVariable().setRuntimeFilterType(8);
        connectContext.getSessionVariable().setEnableRuntimeFilterPrune(false);
        connectContext.getSessionVariable().expandRuntimeFilterByInnerJoin = false;
    }

    @Test
    public void test() {
        String sql = "select lineorder.*, dates.* from dates, lineorder where d_datekey > 19980101 and lo_orderdate = d_datekey order by d_date limit 10;";
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        PlanFragment fragment = new PhysicalPlanTranslator(new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        // MaterializationNode materializationNode = (MaterializationNode) fragment.getPlanRoot();
        System.out.println(fragment);
    }

    @Test
    public void test2() throws Exception {
        this.createTables("create table count_test(k1 varchar(1), k2 int) properties('replication_num' = '1')");
        String sql = "select count(*) from count_test";
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        PlanFragment fragment = new PhysicalPlanTranslator(
                new PlanTranslatorContext(checker.getCascadesContext())).translatePlan(plan);
        System.out.println(fragment);
        List scanNodes = Lists.newArrayList();
        fragment.getPlanRoot().collect(OlapScanNode.class, scanNodes);
        System.out.println(scanNodes);
        List<SlotDescriptor> slots = ((OlapScanNode) scanNodes.get(0)).getTupleDesc().getSlots();
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals("k2", slots.get(0).getColumn().getName());
    }

    @Test
    public void testNestedColumnAccessPathInLazyMaterialize() throws Exception {
        this.createTables("create table lazy_materialize_struct_tbl("
                + "id bigint, "
                + "user_profile struct<"
                + "personal:struct<name:varchar(100)>,"
                + "professional:struct<skills:array<varchar(100)>>>) "
                + "duplicate key(id) distributed by hash(id) buckets 1 "
                + "properties('replication_num' = '1')");
        String sql = "select struct_element(struct_element(user_profile, 'professional'), 'skills') "
                + "from lazy_materialize_struct_tbl order by id limit 10";

        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        PlanTranslatorContext translatorContext = new PlanTranslatorContext(checker.getCascadesContext());
        PlanFragment fragment = new PhysicalPlanTranslator(translatorContext).translatePlan(plan);

        List<MaterializationNode> materializationNodes = Lists.newArrayList();
        fragment.getPlanRoot().collect(MaterializationNode.class, materializationNodes);
        Assertions.assertEquals(1, materializationNodes.size());
        TupleDescriptor materializeTupleDesc = translatorContext.getTupleDesc(
                materializationNodes.get(0).getTupleIds().get(0));
        SlotDescriptor userProfileSlot = materializeTupleDesc.getSlots().stream()
                .filter(slot -> "user_profile".equals(slot.getColumn().getName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("lazy user_profile slot not found"));
        Assertions.assertTrue(userProfileSlot.getAllAccessPaths().contains(
                ColumnAccessPath.data(ImmutableList.of("user_profile", "professional", "skills"))));
    }

    @Test
    public void testLazyBaseColumnIndexUsesOriginalColumnNameForAlias() throws Exception {
        this.createTable("create table lazy_materialize_alias_tbl("
                + "sort_col int, lazy_col int) "
                + "duplicate key(sort_col) distributed by hash(sort_col) buckets 1 "
                + "properties('replication_num' = '1')");
        String sql = "select lazy_col as lazy_alias from lazy_materialize_alias_tbl "
                + "order by sort_col limit 1";

        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);

        List<PhysicalLazyMaterialize<? extends Plan>> materializeNodes = plan.collectToList(
                node -> node instanceof PhysicalLazyMaterialize);
        Assertions.assertEquals(1, materializeNodes.size(), plan.treeString());
        Assertions.assertEquals(ImmutableList.of(ImmutableList.of(1)),
                materializeNodes.get(0).getLazyBaseColumnIndices());
    }

    @Test
    public void testOrderByAliasKeepsItsSourceMaterialized() throws Exception {
        this.createTable("create table lazy_materialize_order_by_alias_tbl("
                + "sort_col int, lazy_col int, other_col int) "
                + "duplicate key(sort_col) distributed by hash(sort_col) buckets 1 "
                + "properties('replication_num' = '1')");
        // TestWithFeService enables feDebug, which makes LazyMaterializeTopN validate its own result and
        // silently fall back on an invalid plan. Turn it off so an invalid plan fails the whole test.
        boolean feDebug = connectContext.getSessionVariable().feDebug;
        connectContext.getSessionVariable().feDebug = false;
        try {
            // The TopN sorts by an alias of lazy_col, so lazy_col must be materialized for the sort.
            // Otherwise LazySlotPruning removes it from the scan while `lazy_col AS x` below the TopN
            // still reads it, and the resulting plan references a slot its child no longer produces.
            PhysicalPlan plan = postProcess("select lazy_col as x, lazy_col as y "
                    + "from lazy_materialize_order_by_alias_tbl order by x limit 1");
            Assertions.assertTrue(
                    plan.collectToList(node -> node instanceof PhysicalLazyMaterialize).isEmpty(),
                    plan.treeString());

            // Sorting by a bare column that is also aliased must keep that column materialized.
            plan = postProcess("select lazy_col, lazy_col as y "
                    + "from lazy_materialize_order_by_alias_tbl order by lazy_col limit 1");
            Assertions.assertTrue(
                    plan.collectToList(node -> node instanceof PhysicalLazyMaterialize).isEmpty(),
                    plan.treeString());

            // Only the column the order key reads is forced materialized, other_col is still fetched lazily.
            plan = postProcess("select lazy_col as x, other_col as y "
                    + "from lazy_materialize_order_by_alias_tbl order by x limit 1");
            List<PhysicalLazyMaterialize<? extends Plan>> materializeNodes = plan.collectToList(
                    node -> node instanceof PhysicalLazyMaterialize);
            Assertions.assertEquals(1, materializeNodes.size(), plan.treeString());
            Assertions.assertEquals(ImmutableList.of(ImmutableList.of(2)),
                    materializeNodes.get(0).getLazyBaseColumnIndices());

            // Sorting by a column that is not aliased keeps every projected column lazily fetched.
            plan = postProcess("select lazy_col as x, other_col as y "
                    + "from lazy_materialize_order_by_alias_tbl order by sort_col limit 1");
            materializeNodes = plan.collectToList(node -> node instanceof PhysicalLazyMaterialize);
            Assertions.assertEquals(1, materializeNodes.size(), plan.treeString());
            List<Integer> lazyColumnIndexes = Lists.newArrayList();
            materializeNodes.get(0).getLazyBaseColumnIndices().forEach(lazyColumnIndexes::addAll);
            lazyColumnIndexes.sort(Integer::compareTo);
            Assertions.assertEquals(ImmutableList.of(1, 2), lazyColumnIndexes);
        } finally {
            connectContext.getSessionVariable().feDebug = feDebug;
        }
    }

    @Test
    public void testNestedTopNKeepsAliasSourceMaterialized() throws Exception {
        this.createTable("create table lazy_materialize_nested_topn_tbl("
                + "sort_col int, lazy_col int, other_col int) "
                + "duplicate key(sort_col) distributed by hash(sort_col) buckets 1 "
                + "properties('replication_num' = '1')");
        boolean feDebug = connectContext.getSessionVariable().feDebug;
        connectContext.getSessionVariable().feDebug = false;
        try {
            // Only the outer TopN is rewritten. Probing its output `y` resolves to lazy_col, which the
            // inner TopN still reads through `lazy_col AS x`, so lazy_col must stay materialized.
            PhysicalPlan plan = postProcess("select y from (select lazy_col as x, lazy_col as y, other_col as z "
                    + "from lazy_materialize_nested_topn_tbl order by x limit 2) s order by z limit 1");
            Assertions.assertTrue(
                    plan.collectToList(node -> node instanceof PhysicalLazyMaterialize).isEmpty(),
                    plan.treeString());

            // A column that nothing below the TopN reads may still be fetched lazily through the TopNs.
            plan = postProcess("select w from (select lazy_col as x, other_col as w, lazy_col as y, sort_col "
                    + "from lazy_materialize_nested_topn_tbl order by x limit 2) s order by sort_col limit 1");
            List<PhysicalLazyMaterialize<? extends Plan>> materializeNodes = plan.collectToList(
                    node -> node instanceof PhysicalLazyMaterialize);
            Assertions.assertEquals(1, materializeNodes.size(), plan.treeString());
            Assertions.assertEquals(ImmutableList.of(ImmutableList.of(2)),
                    materializeNodes.get(0).getLazyBaseColumnIndices());
        } finally {
            connectContext.getSessionVariable().feDebug = feDebug;
        }
    }

    private PhysicalPlan postProcess(String sql) {
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        return new PlanPostProcessors(checker.getCascadesContext()).process(checker.getPhysicalPlan());
    }

    @Test
    public void testLightSchemaChangeFalse() throws Exception {
        this.createTable("create table tm_lsc_false (k int, v int) duplicate key(k) "
                + "distributed by hash(k) buckets 1 "
                + "properties('replication_num' = '1', 'light_schema_change' = 'false')");
        String sql = "select * from tm_lsc_false order by k limit 1";
        PlanChecker checker = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .implement();
        PhysicalPlan plan = checker.getPhysicalPlan();
        plan = new PlanPostProcessors(checker.getCascadesContext()).process(plan);
        PlanTranslatorContext translatorContext = new PlanTranslatorContext(checker.getCascadesContext());
        PlanFragment fragment = new PhysicalPlanTranslator(translatorContext).translatePlan(plan);

        // TopN lazy materialization should be skipped for light_schema_change=false,
        // so no MaterializationNode should be created.
        List<MaterializationNode> materializationNodes = Lists.newArrayList();
        fragment.getPlanRoot().collect(MaterializationNode.class, materializationNodes);
        Assertions.assertTrue(materializationNodes.isEmpty(),
                "TopN lazy materialization should be skipped for light_schema_change=false");

        // All columns should be in the scan output (no lazy pruned columns).
        List<OlapScanNode> scanNodes = Lists.newArrayList();
        fragment.getPlanRoot().collect(OlapScanNode.class, scanNodes);
        Assertions.assertEquals(1, scanNodes.size());
        List<SlotDescriptor> slots = scanNodes.get(0).getTupleDesc().getSlots();
        Assertions.assertEquals(2, slots.size());
        Assertions.assertTrue(slots.stream().anyMatch(s -> s.getColumn().getName().equals("k")));
        Assertions.assertTrue(slots.stream().anyMatch(s -> s.getColumn().getName().equals("v")));
    }
}
