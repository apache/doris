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
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnNameAccessPath;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class PruneNestedColumn extends TestWithFeService {
    @BeforeAll
    public void createTable() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("create table tbl(\n"
                + "  id int,\n"
                + "  s struct<\n"
                + "    city: string,\n"
                + "    data: array<map<\n"
                + "      int,\n"
                + "      struct<a: int, b: double>\n"
                + "    >>\n"
                + ">)\n"
                + "properties ('replication_num'='1')");

        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
    }

    @Test
    public void testProject() throws Exception {
        assertColumn("select 100 from tbl", null, null, null);
        assertColumn("select s from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(s, 'city'), s from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(s, 'city') from tbl",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(s, 'data') from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(s, 'data')[1] from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*")),
                ImmutableList.of()
        );
        assertColumn("select map_keys(struct_element(s, 'data')[1]) from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "KEYS")),
                ImmutableList.of()
        );
        assertColumn("select map_values(struct_element(s, 'data')[1]) from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(map_values(struct_element(s, 'data')[1])[1], 'a') from tbl",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "a")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(s, 'data')[1][1] from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "*")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(struct_element(s, 'data')[1][1], 'a') from tbl",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "*", "a")),
                ImmutableList.of()
        );
        assertColumn("select struct_element(struct_element(s, 'data')[1][1], 'b') from tbl",
                "struct<data:array<map<int,struct<b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "*", "b")),
                ImmutableList.of()
        );
        // assertColumn("select struct_element(struct_element(s, 'data')[1][1], 'b') from tbl where struct_element(s, 'city')='beijing",
        //         "struct<data:array<map<int,struct<b:double>>>>",
        //         predicatePath("city"),
        //         path("data", "*", "*", "b")
        // );

        // assertColumn("select array_map(x -> x[2], struct_element(s, 'data')) from tbl", "struct<data:array<map<int,struct<a:int,b:double>>>>", path("data"));
        // assertColumn("select array_map(x -> struct_element(x[2], 'b'), struct_element(s, 'data')) from tbl", "struct<data:array<map<int,struct<b:double>>>>", path("data", "*", "*", "b"));
    }

    @Test
    public void testFilter() throws Throwable {
        assertColumn("select 100 from tbl where s is not null",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of(path("s"))
        );

        assertColumn("select 100 from tbl where if(id = 1, null, s) is not null and struct_element(s, 'city') = 'beijing'",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of(path("s"))
        );

        assertColumn("select 100 from tbl where struct_element(s, 'city') is not null",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of(path("s", "city"))
        );

        assertColumn("select 100 from tbl where struct_element(s, 'data') is not null",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data")),
                ImmutableList.of(path("s", "data"))
        );
        assertColumn("select 100 from tbl where struct_element(s, 'data')[1] is not null",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*")),
                ImmutableList.of(path("s", "data", "*"))
        );
        assertColumn("select 100 from tbl where map_keys(struct_element(s, 'data')[1]) is not null",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "KEYS")),
                ImmutableList.of(path("s", "data", "*", "KEYS"))
        );
        assertColumn("select 100 from tbl where map_values(struct_element(s, 'data')[1]) is not null",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES")),
                ImmutableList.of(path("s", "data", "*", "VALUES"))
        );
        assertColumn("select 100 from tbl where struct_element(map_values(struct_element(s, 'data')[1])[1], 'a') is not null",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "a")),
                ImmutableList.of(path("s", "data", "*", "VALUES", "a"))
        );
        assertColumn("select 100 from tbl where struct_element(s, 'data')[1][1] is not null",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "*")),
                ImmutableList.of(path("s", "data", "*", "*"))
        );
        assertColumn("select 100 from tbl where struct_element(struct_element(s, 'data')[1][1], 'a') is not null",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "*", "a")),
                ImmutableList.of(path("s", "data", "*", "*", "a"))
        );
        assertColumn("select 100 from tbl where struct_element(struct_element(s, 'data')[1][1], 'b') is not null",
                "struct<data:array<map<int,struct<b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "*", "b")),
                ImmutableList.of(path("s", "data", "*", "*", "b"))
        );
    }

    @Test
    public void testProjectFilter() throws Throwable {
        assertColumn("select s from tbl where struct_element(s, 'city') is not null",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of(path("s", "city"))
        );

        assertColumn("select struct_element(s, 'data') from tbl where struct_element(s, 'city') is not null",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data"), path("s", "city")),
                ImmutableList.of(path("s", "city"))
        );

        assertColumn("select struct_element(s, 'data') from tbl where struct_element(s, 'city') is not null and struct_element(s, 'data') is not null",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data"), path("s", "city")),
                ImmutableList.of(path("s", "data"), path("s", "city"))
        );
    }

    private void assertColumn(String sql, String expectType,
            List<TColumnNameAccessPath> expectAllAccessPaths,
            List<TColumnNameAccessPath> expectPredicateAccessPaths) throws Exception {
        List<SlotDescriptor> slotDescriptors = collectComplexSlots(sql);
        if (expectType == null) {
            Assertions.assertEquals(0, slotDescriptors.size());
            return;
        }

        Assertions.assertEquals(1, slotDescriptors.size());
        Assertions.assertEquals(expectType, slotDescriptors.get(0).getType().toString());

        Assertions.assertEquals(TAccessPathType.NAME, slotDescriptors.get(0).getAllAccessPaths().type);
        TreeSet<TColumnNameAccessPath> expectAllAccessPathSet = new TreeSet<>(expectAllAccessPaths);
        TreeSet<TColumnNameAccessPath> actualAllAccessPaths
                = new TreeSet<>(slotDescriptors.get(0).getAllAccessPaths().name_access_paths);
        Assertions.assertEquals(expectAllAccessPathSet, actualAllAccessPaths);

        Assertions.assertEquals(TAccessPathType.NAME, slotDescriptors.get(0).getPredicateAccessPaths().type);
        TreeSet<TColumnNameAccessPath> expectPredicateAccessPathSet = new TreeSet<>(expectPredicateAccessPaths);
        TreeSet<TColumnNameAccessPath> actualPredicateAccessPaths
                = new TreeSet<>(slotDescriptors.get(0).getPredicateAccessPaths().name_access_paths);
        Assertions.assertEquals(expectPredicateAccessPathSet, actualPredicateAccessPaths);
    }

    private List<SlotDescriptor> collectComplexSlots(String sql) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) getSqlStmtExecutor(sql).planner();
        List<SlotDescriptor> complexSlots = new ArrayList<>();
        for (PlanFragment fragment : planner.getFragments()) {
            List<OlapScanNode> olapScanNodes = fragment.getPlanRoot().collectInCurrentFragment(OlapScanNode.class::isInstance);
            for (OlapScanNode olapScanNode : olapScanNodes) {
                List<SlotDescriptor> slots = olapScanNode.getTupleDesc().getSlots();
                for (SlotDescriptor slot : slots) {
                    Type type = slot.getType();
                    if (type.isComplexType() || type.isVariantType()) {
                        complexSlots.add(slot);
                    }
                }
            }
        }
        return complexSlots;
    }

    private TColumnNameAccessPath path(String... path) {
        return new TColumnNameAccessPath(ImmutableList.copyOf(path));
    }
}
