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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class VariantPruningLogicTest extends TestWithFeService {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_variant_pruning_logic");
        useDatabase("test_variant_pruning_logic");
        createTable("create table variant_tbl(\n"
                + "  id int,\n"
                + "  v variant\n"
                + ") properties ('replication_num'='1')");
        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        connectContext.getSessionVariable().enableNereidsTimeout = false;
        connectContext.getSessionVariable().enablePruneNestedColumns = true;
    }

    @Test
    public void testVariantNumericIndexSubPath() throws Exception {
        assertVariantSubColumnSlots(
                "select v['arr'][0]['x'] from variant_tbl",
                ImmutableList.of(
                        ImmutableList.of("arr", "0", "x")
                )
        );
        assertAllAccessPathsContain(
                "select v['arr'][0]['x'] from variant_tbl",
                ImmutableList.of(path("v", "arr", "0", "x")),
                ImmutableList.of()
        );
    }

    @Test
    public void testVariantOrPredicatePaths() throws Exception {
        assertPredicateAccessPathsEqual(
                "select 1 from variant_tbl where v['a'] = 1 or v['b']['c'] = 2",
                ImmutableList.of(path("v", "a"), path("v", "b", "c"))
        );
        assertVariantSubColumnSlots(
                "select 1 from variant_tbl where v['a'] = 1 or v['b']['c'] = 2",
                ImmutableList.of(
                        ImmutableList.of("a"),
                        ImmutableList.of("b", "c")
                )
        );
    }

    @Test
    public void testVariantIfExpressionPaths() throws Exception {
        assertVariantSubColumnSlots(
                "select if(v['a'] is null, v['b']['c'], v['d']) from variant_tbl",
                ImmutableList.of(
                        ImmutableList.of("a"),
                        ImmutableList.of("b", "c"),
                        ImmutableList.of("d")
                )
        );
        assertAllAccessPathsContain(
                "select if(v['a'] is null, v['b']['c'], v['d']) from variant_tbl",
                ImmutableList.of(path("v", "a"), path("v", "b", "c"), path("v", "d")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeWholeVariantAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['k'] from variant_tbl lateral view explode(v) tmp as x",
                ImmutableList.of(path("v", "k")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeVariantArrayWithOuterFilterAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['x'] from variant_tbl lateral view explode(v['arr']) tmp as x "
                        + "where v['filter']['k'] = 1 and x['y'] is not null",
                ImmutableList.of(
                        path("v", "arr", "x"),
                        path("v", "arr", "y"),
                        path("v", "filter", "k")
                ),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeVariantDeepNestedAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['a']['b'][0]['c'] from variant_tbl lateral view explode(v['arr']) tmp as x",
                ImmutableList.of(path("v", "arr", "a", "b", "0", "c")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeSubqueryJoinAggAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select cast(t2.v['k'] as string) as k, count(*) from (select id, v from variant_tbl) t1 "
                        + "lateral view explode(t1.v['arr']) tmp as x "
                        + "join variant_tbl t2 on t1.id=t2.id "
                        + "where x['a']['b'] = 1 and t2.v['k'] is not null "
                        + "group by cast(t2.v['k'] as string)",
                ImmutableList.of(
                        path("v", "arr", "a", "b"),
                        path("v", "k")
                ),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeAggAndFilterAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select sum(cast(x['metric'] as int)) from variant_tbl lateral view explode(v['arr']) tmp as x "
                        + "where x['metric'] is not null",
                ImmutableList.of(path("v", "arr", "metric")),
                ImmutableList.of()
        );
    }

    @Test
    public void testExplodeOuterAccessPaths() throws Exception {
        assertAllAccessPathsContain(
                "select x['k'] from variant_tbl lateral view explode_outer(v['arr']) tmp as x",
                ImmutableList.of(path("v", "arr", "k")),
                ImmutableList.of()
        );
    }

    private Pair<PhysicalPlan, List<SlotDescriptor>> collectVariantSlots(String sql) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) executeNereidsSql(sql).planner();
        List<SlotDescriptor> variantSlots = new ArrayList<>();
        PhysicalPlan physicalPlan = planner.getPhysicalPlan();
        for (PlanFragment fragment : planner.getFragments()) {
            List<OlapScanNode> olapScanNodes =
                    fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance);
            for (OlapScanNode olapScanNode : olapScanNodes) {
                List<SlotDescriptor> slots = olapScanNode.getTupleDesc().getSlots();
                for (SlotDescriptor slot : slots) {
                    Type type = slot.getType();
                    if (type.isVariantType()) {
                        variantSlots.add(slot);
                    }
                }
            }
        }
        return Pair.of(physicalPlan, variantSlots);
    }

    private void assertVariantSubColumnSlots(String sql, List<List<String>> expectedSubColPaths) throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<String> actualSubColPaths = new TreeSet<>();
        for (SlotDescriptor slotDescriptor : result.second) {
            List<String> subColPath = slotDescriptor.getSubColLables();
            if (subColPath == null || subColPath.isEmpty()) {
                continue;
            }
            actualSubColPaths.add(String.join(".", subColPath));
        }

        TreeSet<String> expectedSubColPathSet = new TreeSet<>();
        for (List<String> expected : expectedSubColPaths) {
            expectedSubColPathSet.add(String.join(".", expected));
        }

        Assertions.assertEquals(expectedSubColPathSet, actualSubColPaths);
    }

    private void assertPredicateAccessPathsEqual(String sql, List<TColumnAccessPath> expected) throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<TColumnAccessPath> actualSet = new TreeSet<>();
        for (SlotDescriptor slotDescriptor : result.second) {
            List<TColumnAccessPath> predicate = slotDescriptor.getPredicateAccessPaths();
            if (predicate != null) {
                actualSet.addAll(predicate);
            }
        }

        TreeSet<TColumnAccessPath> expectedSet = new TreeSet<>(expected);
        Assertions.assertEquals(expectedSet, actualSet);
    }

    private void assertAllAccessPathsContain(
            String sql, List<TColumnAccessPath> expectedContain, List<TColumnAccessPath> expectedNotContain)
            throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectVariantSlots(sql);
        TreeSet<TColumnAccessPath> allAccessPaths = new TreeSet<>();
        for (SlotDescriptor slotDescriptor : result.second) {
            allAccessPaths.addAll(slotDescriptor.getAllAccessPaths());
        }
        for (TColumnAccessPath accessPath : expectedContain) {
            Assertions.assertTrue(allAccessPaths.contains(accessPath));
        }
        for (TColumnAccessPath accessPath : expectedNotContain) {
            Assertions.assertFalse(allAccessPaths.contains(accessPath));
        }
    }

    private TColumnAccessPath path(String... path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
        accessPath.data_access_path = new TDataAccessPath(ImmutableList.copyOf(path));
        return accessPath;
    }
}
