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
import org.apache.doris.common.Triple;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.NestedColumnPruning.DataTypeAccessTree;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Coalesce;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NestedColumnPrunable;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TAccessPathType;
import org.apache.doris.thrift.TColumnAccessPath;
import org.apache.doris.thrift.TDataAccessPath;
import org.apache.doris.thrift.TMetaAccessPath;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.function.Consumer;

public class PruneNestedColumnTest extends TestWithFeService implements MemoPatternMatchSupported {
    @BeforeAll
    public void createTable() throws Exception {
        createDatabase("test");
        useDatabase("test");

        createTable("create table tbl(\n"
                + "  id int,\n"
                + "  value int,\n"
                + "  s struct<\n"
                + "    city: string,\n"
                + "    data: array<map<\n"
                + "      int,\n"
                + "      struct<a: int, b: double>\n"
                + "    >>\n"
                + ">)\n"
                + "properties ('replication_num'='1')");

        createTable("create table tbl2(\n"
                + "  id2 int,\n"
                + "  value int,\n"
                + "  s2 struct<\n"
                + "    city2: string,\n"
                + "    data2: array<map<\n"
                + "      int,\n"
                + "      struct<a2: int, b2: double>\n"
                + "    >>\n"
                + ">)\n"
                + "properties ('replication_num'='1')");

        connectContext.getSessionVariable().setDisableNereidsRules(RuleType.PRUNE_EMPTY_PARTITION.name());
        connectContext.getSessionVariable().enableNereidsTimeout = false;
    }

    @Test
    public void testCaseInsensitive() throws Exception {
        assertColumn("select struct_element(MAP_VALUES(struct_element(S, 'DATA')[1])[1], 'B') from tbl",
                "struct<data:array<map<int,struct<b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "b")),
                ImmutableList.of()
        );
    }

    @Test
    public void testMap() throws Exception {
        assertColumn("select MAP_KEYS(struct_element(s, 'data')[0])[1] from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "KEYS")),
                ImmutableList.of()
        );

        assertColumn("select MAP_VALUES(struct_element(s, 'data')[0])[1] from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES")),
                ImmutableList.of()
        );
    }

    @Test
    public void testStruct() throws Throwable {
        assertColumn("select struct_element(s, 1) from tbl",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("select struct_element(map_values(struct_element(s, 'data')[0])[0], 1) from tbl",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "a")),
                ImmutableList.of()
        );
    }

    @Test
    public void testPruneCast() throws Exception {
        // the map type is changed, so we can not prune type
        assertColumn("select struct_element(cast(s as struct<k:text,l:array<map<int,struct<x:int,y:int>>>>), 'k') from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );

        assertColumn("select struct_element(cast(s as struct<k:text,l:array<map<int,struct<x:int,y:double>>>>), 'k') from tbl",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("select struct_element(map_values(struct_element(cast(s as struct<k:text,l:array<map<int,struct<x:int,y:double>>>>), 'l')[0])[0], 'x') from tbl",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "a")),
                ImmutableList.of()
        );

        assertColumns("select struct_element(s, 'city') from (select * from tbl union all select * from tbl2)t",
                ImmutableList.of(
                        Triple.of(
                                "struct<city2:text>",
                                ImmutableList.of(path("s2", "city2")),
                                ImmutableList.of()
                        ),
                        Triple.of(
                                "struct<city:text>",
                                ImmutableList.of(path("s", "city")),
                                ImmutableList.of()
                        )
                )
        );

        assertColumns("select struct_element(s, 'city'), struct_element(map_values(struct_element(s, 'data')[0])[0], 'b') from (select * from tbl union all select * from tbl2)t",
                ImmutableList.of(
                        Triple.of(
                                "struct<city2:text,data2:array<map<int,struct<b2:double>>>>",
                                ImmutableList.of(path("s2", "city2"), path("s2", "data2", "*", "VALUES", "b2")),
                                ImmutableList.of()
                        ),
                        Triple.of(
                                "struct<city:text,data:array<map<int,struct<b:double>>>>",
                                ImmutableList.of(path("s", "city"), path("s", "data", "*", "VALUES", "b")),
                                ImmutableList.of()
                        )
                )
        );
    }

    @Test
    public void testPruneArrayLambda() throws Exception {
        // map_values(struct_element(s, 'data').*)[0].a
        assertColumn("select struct_element(array_map(x -> map_values(x)[0], struct_element(s, 'data'))[0], 'a') from tbl",
                "struct<data:array<map<int,struct<a:int>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "a")),
                ImmutableList.of()
        );

        assertColumn("select array_map((x, y) -> struct_element(map_values(x)[0], 'a') + struct_element(map_values(y)[0], 'b'), struct_element(s, 'data'), struct_element(s, 'data')) from tbl",
                "struct<data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s", "data", "*", "VALUES", "a"), path("s", "data", "*", "VALUES", "b")),
                ImmutableList.of()
        );
    }

    @Test
    public void testProject() throws Exception {
        assertColumn("select 100 from tbl", null, null, null);
        assertColumn("select * from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );
        assertColumn("select tbl.* from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );
        assertColumn("select test.tbl.* from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );
        assertColumn("select internal.test.tbl.* from tbl",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of()
        );
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
    }

    @Test
    public void testFilter() throws Throwable {
        assertColumn("select 100 from tbl where s is not null",
                "struct<city:text,data:array<map<int,struct<a:int,b:double>>>>",
                ImmutableList.of(path("s")),
                ImmutableList.of(path("s"))
        );

        assertColumn("select 100 from tbl where if(id = 1, null, s) is not null or struct_element(s, 'city') = 'beijing'",
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

    @Test
    public void testCte() throws Throwable {
        assertColumn("with t as (select id, s from tbl) select struct_element(t1.s, 'city') from t t1 join t t2 on t1.id = t2.id",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("with t as (select id, struct_element(s, 'city') as c from tbl) select t1.c from t t1 join t t2 on t1.id = t2.id",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );
    }

    @Test
    public void testUnion() throws Throwable {
        assertColumn("select coalesce(struct_element(s, 'city'), 'abc') from (select s from tbl union all select null)a",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("select * from (select coalesce(struct_element(s, 'city'), 'abc') from tbl union all select null)a",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );
    }

    @Test
    public void testCteAndUnion() throws Throwable {
        assertColumn("with t as (select id, s from tbl) select struct_element(s, 'city') from (select * from t union all select 1, null) tmp",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("with t as (select id, s from tbl) select * from (select struct_element(s, 'city') from t union all select null) tmp",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );
    }

    @Test
    public void testDereference() throws Exception {
        assertColumn("select s.city from tbl",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );
    }

    @Test
    public void testPushDownThroughJoin() {
        PlanChecker.from(connectContext)
                .analyze("select coalesce(struct_element(s, 'city'), 'abc') from (select * from tbl)a join (select 100 id, 'f1' name)b on a.id=b.id")
                .rewrite()
                .matches(
                    logicalResultSink(
                        logicalProject(
                            logicalJoin(
                                logicalProject(
                                    logicalFilter(
                                        logicalOlapScan()
                                    )
                                ).when(p -> {
                                    Assertions.assertEquals(2, p.getProjects().size());
                                    Assertions.assertTrue(p.getProjects().stream()
                                            .anyMatch(o -> o instanceof Alias && o.child(0) instanceof StructElement));
                                    return true;
                                }),
                                logicalOneRowRelation()
                            )
                        ).when(p -> {
                            Assertions.assertTrue(p.getProjects().size() == 1 && p.getProjects().get(0) instanceof Alias
                                    && p.getProjects().get(0).child(0) instanceof Coalesce
                                    && p.getProjects().get(0).child(0).child(0) instanceof Slot);
                            return true;
                        })
                    )
                );
    }

    @Test
    public void testAggregate() throws Exception {
        assertColumn("select count(struct_element(s, 'city')) from tbl",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );
    }

    @Test
    public void testJoin() throws Exception {
        assertColumns("select 100 from tbl t1 join tbl t2 on struct_element(t1.s, 'city')=struct_element(t2.s, 'city')",
                ImmutableList.of(
                        Triple.of(
                                "struct<city:text>",
                                ImmutableList.of(path("s", "city")),
                                ImmutableList.of()
                        ),
                        Triple.of(
                                "struct<city:text>",
                                ImmutableList.of(path("s", "city")),
                                ImmutableList.of()
                        )
                )
        );
    }

    @Test
    public void testPushDownThroughWindow() {
        PlanChecker.from(connectContext)
                .analyze("select struct_element(s, 'city'), r from (select s, rank() over(partition by id) r from tbl t)a")
                .rewrite()
                .matches(
                    logicalResultSink(
                        logicalProject(
                            logicalWindow(
                                logicalProject(
                                    logicalOlapScan()
                                ).when(p -> {
                                    Assertions.assertEquals(2, p.getProjects().size());
                                    Assertions.assertTrue(p.getProjects().stream()
                                            .anyMatch(o -> o instanceof Alias && o.child(0) instanceof StructElement));
                                    return true;
                                })
                            )
                        ).when(p -> {
                            Assertions.assertTrue(p.getProjects().size() == 2
                                    && (p.getProjects().get(0) instanceof SlotReference
                                        || (p.getProjects().get(0) instanceof Alias && p.getProjects().get(0).child(0) instanceof SlotReference)));
                            return true;
                        })
                    )
                );
    }

    @Test
    public void testPushDownThroughPartitionTopN() {
        PlanChecker.from(connectContext)
                .analyze("select struct_element(s, 'city'), r from (select s, rank() over(partition by id) r from tbl t limit 10)a")
                .rewrite()
                .matches(
                    logicalResultSink(
                        logicalLimit(
                            logicalLimit(
                                logicalProject(
                                    logicalWindow(
                                        logicalPartitionTopN(
                                            logicalProject(
                                                    logicalOlapScan()
                                            ).when(p -> {
                                                Assertions.assertEquals(2, p.getProjects().size());
                                                Assertions.assertTrue(p.getProjects().stream()
                                                        .anyMatch(o -> o instanceof Alias && o.child(0) instanceof StructElement));
                                                return true;
                                            })
                                        )
                                    )
                                ).when(p -> {
                                    Assertions.assertTrue(p.getProjects().size() == 2
                                            && (p.getProjects().get(0) instanceof SlotReference
                                                || p.getProjects().get(0) instanceof Alias && p.getProjects().get(0).child(0) instanceof SlotReference));
                                    return true;
                                })
                            )
                        )
                    )
                );
    }

    @Test
    public void testPushDownThroughUnion() {
        PlanChecker.from(connectContext)
                .analyze("select struct_element(s, 'city') from (select id, s from tbl union all select 1, null) tmp")
                .rewrite()
                .matches(
                    logicalResultSink(
                        logicalUnion(
                            logicalProject(
                                logicalOlapScan()
                            ).when(p -> {
                                Assertions.assertEquals(1, p.getProjects().size());
                                Assertions.assertInstanceOf(StructElement.class, p.getProjects().get(0).child(0));
                                return true;
                            })
                        ).when(u -> {
                            Assertions.assertEquals(1, u.getConstantExprsList().size());
                            Assertions.assertInstanceOf(NullLiteral.class, u.getConstantExprsList().get(0).get(0).child(0));
                            return true;
                        })
                    )
                );
    }

    @Test
    public void testDataTypeAccessTree() {
        List<Pair<SlotReference, DataTypeAccessTree>> trees = getDataTypeAccessTrees(
                "select struct_element(s, 'city') from (select id, s from tbl union all select 1, null) tmp");

        Assertions.assertEquals(1, trees.size());
        DataTypeAccessTree tree = trees.get(0).second;
        Assertions.assertEquals(NullType.INSTANCE, tree.getType());
        Assertions.assertEquals(1, tree.getChildren().size());
        Assertions.assertEquals("STRUCT<city:TEXT>", tree.getChildren().get("s").getType().toSql());

        SlotReference slot = trees.get(0).first;
        Type columnType = slot.getOriginalColumn().get().getType();
        Assertions.assertEquals("struct<city:text,data:array<map<int,struct<a:int,b:double>>>>", columnType.toSql());

        setAccessPathAndAssertType(slot, ImmutableList.of("s", "city"), "STRUCT<city:TEXT>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT,b:DOUBLE>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT,b:DOUBLE>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "KEYS"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT,b:DOUBLE>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "VALUES"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT,b:DOUBLE>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "VALUES", "a"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "VALUES", "b"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<b:DOUBLE>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "*"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT,b:DOUBLE>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "*", "a"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<a:INT>>>>");
        setAccessPathAndAssertType(slot, ImmutableList.of("s", "data", "*", "*", "b"), "STRUCT<data:ARRAY<MAP<INT,STRUCT<b:DOUBLE>>>>");

        setAccessPathsAndAssertType(slot,
                ImmutableList.of(
                        ImmutableList.of("s", "data", "*", "*", "b"),
                        ImmutableList.of("s", "city")
                ),
                "STRUCT<city:TEXT,data:ARRAY<MAP<INT,STRUCT<b:DOUBLE>>>>"
        );
    }

    @Test
    public void testWithVariant() throws Exception {
        connectContext.getSessionVariable().enableDecimal256 = true;

        createTable("CREATE TABLE test.`table_20_undef_partitions2_keys3_properties4_distributed_by56` (\n"
                + "  `col_tinyint_undef_signed_index_inverted` tinyint NULL,\n"
                + "  `col_smallint_undef_signed_not_null_index_inverted` smallint NOT NULL,\n"
                + "  `pk` int NULL,\n"
                + "  `col_int_undef_signed` int NULL,\n"
                + "  `col_bigint_undef_signed` bigint NULL,\n"
                + "  `col_decimal_10_0__undef_signed` decimal(10,0) NULL,\n"
                + "  `col_largeint_undef_signed` largeint NULL,\n"
                + "  `col_boolean_undef_signed` boolean NULL,\n"
                + "  `col_boolean_undef_signed_not_null` boolean NOT NULL,\n"
                + "  `col_tinyint_undef_signed` tinyint NULL,\n"
                + "  `col_tinyint_undef_signed_not_null` tinyint NOT NULL,\n"
                + "  `col_tinyint_undef_signed_not_null_index_inverted` tinyint NOT NULL,\n"
                + "  `col_smallint_undef_signed` smallint NULL,\n"
                + "  `col_smallint_undef_signed_index_inverted` smallint NULL,\n"
                + "  `col_smallint_undef_signed_not_null` smallint NOT NULL,\n"
                + "  `col_int_undef_signed_index_inverted` int NULL,\n"
                + "  `col_int_undef_signed_not_null` int NOT NULL,\n"
                + "  `col_int_undef_signed_not_null_index_inverted` int NOT NULL,\n"
                + "  `col_bigint_undef_signed_index_inverted` bigint NULL,\n"
                + "  `col_bigint_undef_signed_not_null` bigint NOT NULL,\n"
                + "  `col_bigint_undef_signed_not_null_index_inverted` bigint NOT NULL,\n"
                + "  `col_largeint_undef_signed_not_null` largeint NOT NULL,\n"
                + "  `col_decimal_10_0__undef_signed_index_inverted` decimal(10,0) NULL,\n"
                + "  `col_decimal_10_0__undef_signed_not_null` decimal(10,0) NOT NULL,\n"
                + "  `col_decimal_10_0__undef_signed_not_null_index_inverted` decimal(10,0) NOT NULL,\n"
                + "  `col_decimal_16_10__undef_signed` decimal(16,10) NULL,\n"
                + "  `col_decimal_16_10__undef_signed_index_inverted` decimal(16,10) NULL,\n"
                + "  `col_decimal_16_10__undef_signed_not_null` decimal(16,10) NOT NULL,\n"
                + "  `col_decimal_16_10__undef_signed_not_null_index_inverted` decimal(16,10) NOT NULL,\n"
                + "  `col_decimal_37__12__undef_signed` decimal(37,12) NULL,\n"
                + "  `col_decimal_37__12__undef_signed_index_inverted` decimal(37,12) NULL,\n"
                + "  `col_decimal_37__12__undef_signed_not_null` decimal(37,12) NOT NULL,\n"
                + "  `col_decimal_37__12__undef_signed_not_null_index_inverted` decimal(37,12) NOT NULL,\n"
                + "  `col_decimal_17_0__undef_signed` decimal(17,0) NULL,\n"
                + "  `col_decimal_17_0__undef_signed_index_inverted` decimal(17,0) NULL,\n"
                + "  `col_decimal_17_0__undef_signed_not_null` decimal(17,0) NOT NULL,\n"
                + "  `col_decimal_17_0__undef_signed_not_null_index_inverted` decimal(17,0) NOT NULL,\n"
                + "  `col_decimal_8_4__undef_signed` decimal(8,4) NULL,\n"
                + "  `col_decimal_8_4__undef_signed_index_inverted` decimal(8,4) NULL,\n"
                + "  `col_decimal_8_4__undef_signed_not_null` decimal(8,4) NOT NULL,\n"
                + "  `col_decimal_8_4__undef_signed_not_null_index_inverted` decimal(8,4) NOT NULL,\n"
                + "  `col_decimal_9_0__undef_signed` decimal(9,0) NULL,\n"
                + "  `col_decimal_9_0__undef_signed_index_inverted` decimal(9,0) NULL,\n"
                + "  `col_decimal_9_0__undef_signed_not_null` decimal(9,0) NOT NULL,\n"
                + "  `col_decimal_9_0__undef_signed_not_null_index_inverted` decimal(9,0) NOT NULL,\n"
                + "  `col_decimal_76__56__undef_signed` decimal(76,56) NULL,\n"
                + "  `col_decimal_76__56__undef_signed_index_inverted` decimal(76,56) NULL,\n"
                + "  `col_decimal_76__56__undef_signed_not_null` decimal(76,56) NOT NULL,\n"
                + "  `col_decimal_76__56__undef_signed_not_null_index_inverted` decimal(76,56) NOT NULL,\n"
                + "  `col_datetime_undef_signed` datetime NULL,\n"
                + "  `col_datetime_undef_signed_index_inverted` datetime NULL,\n"
                + "  `col_datetime_undef_signed_not_null` datetime NOT NULL,\n"
                + "  `col_datetime_undef_signed_not_null_index_inverted` datetime NOT NULL,\n"
                + "  `col_map_boolean__boolean__undef_signed` map<boolean,boolean> NULL,\n"
                + "  `col_map_boolean__boolean__undef_signed_not_null` map<boolean,boolean> NOT NULL,\n"
                + "  `col_map_tinyint__tinyint__undef_signed` map<tinyint,tinyint> NULL,\n"
                + "  `col_map_tinyint__tinyint__undef_signed_not_null` map<tinyint,tinyint> NOT NULL,\n"
                + "  `col_map_smallint__smallint__undef_signed` map<smallint,smallint> NULL,\n"
                + "  `col_map_smallint__smallint__undef_signed_not_null` map<smallint,smallint> NOT NULL,\n"
                + "  `col_map_int__int__undef_signed` map<int,int> NULL,\n"
                + "  `col_map_int__int__undef_signed_not_null` map<int,int> NOT NULL,\n"
                + "  `col_map_bigint__bigint__undef_signed` map<bigint,bigint> NULL,\n"
                + "  `col_map_bigint__bigint__undef_signed_not_null` map<bigint,bigint> NOT NULL,\n"
                + "  `col_map_largeint__largeint__undef_signed` map<largeint,largeint> NULL,\n"
                + "  `col_map_largeint__largeint__undef_signed_not_null` map<largeint,largeint> NOT NULL,\n"
                + "  `col_map_decimal_10_0___decimal_10_0___undef_signed` map<decimal(10,0),decimal(10,0)> NULL,\n"
                + "  `col_map_decimal_10_0___decimal_10_0___undef_signed_not_null` map<decimal(10,0),decimal(10,0)> NOT NULL,\n"
                + "  `col_map_decimal_16_10___decimal_16_10___undef_signed` map<decimal(16,10),decimal(16,10)> NULL,\n"
                + "  `col_map_decimal_16_10___decimal_16_10___undef_signed_not_null` map<decimal(16,10),decimal(16,10)> NOT NULL,\n"
                + "  `col_map_decimal_37__12___decimal_37__12___undef_signed` map<decimal(37,12),decimal(37,12)> NULL,\n"
                + "  `col_map_decimal_37__12___decimal_37__12___undef_signed_not_null` map<decimal(37,12),decimal(37,12)> NOT NULL,\n"
                + "  `col_map_decimal_8_4___decimal_8_4___undef_signed` map<decimal(8,4),decimal(8,4)> NULL,\n"
                + "  `col_map_decimal_8_4___decimal_8_4___undef_signed_not_null` map<decimal(8,4),decimal(8,4)> NOT NULL,\n"
                + "  `col_map_decimal_76__56___decimal_76__56___undef_signed` map<decimal(76,56),decimal(76,56)> NULL,\n"
                + "  `col_map_decimal_76__56___decimal_76__56___undef_signed_not_null` map<decimal(76,56),decimal(76,56)> NOT NULL,\n"
                + "  `col_map_char_255___boolean__undef_signed` map<character(255),boolean> NULL,\n"
                + "  `col_map_char_255___boolean__undef_signed_not_null` map<character(255),boolean> NOT NULL,\n"
                + "  `col_map_char_255___tinyint__undef_signed` map<character(255),tinyint> NULL,\n"
                + "  `col_map_char_255___tinyint__undef_signed_not_null` map<character(255),tinyint> NOT NULL,\n"
                + "  `col_map_varchar_255___int__undef_signed` map<varchar(255),int> NULL,\n"
                + "  `col_map_varchar_255___int__undef_signed_not_null` map<varchar(255),int> NOT NULL,\n"
                + "  `col_map_varchar_65533___largeint__undef_signed` map<varchar(65533),largeint> NULL,\n"
                + "  `col_map_varchar_65533___largeint__undef_signed_not_null` map<varchar(65533),largeint> NOT NULL,\n"
                + "  `col_map_string__decimal_10_0___undef_signed` map<text,decimal(10,0)> NULL,\n"
                + "  `col_map_string__decimal_10_0___undef_signed_not_null` map<text,decimal(10,0)> NOT NULL,\n"
                + "  `col_map_varchar_65533___decimal_76__50___undef_signed` map<varchar(65533),decimal(76,50)> NULL,\n"
                + "  `col_map_varchar_65533___decimal_76__50___undef_signed_not_null` map<varchar(65533),decimal(76,50)> NOT NULL,\n"
                + "  `col_map_date__boolean__undef_signed` map<date,boolean> NULL,\n"
                + "  `col_map_date__boolean__undef_signed_not_null` map<date,boolean> NOT NULL,\n"
                + "  `col_map_date__tinyint__undef_signed` map<date,tinyint> NULL,\n"
                + "  `col_map_date__tinyint__undef_signed_not_null` map<date,tinyint> NOT NULL,\n"
                + "  `col_map_date__smallint__undef_signed` map<date,smallint> NULL,\n"
                + "  `col_map_date__smallint__undef_signed_not_null` map<date,smallint> NOT NULL,\n"
                + "  `col_map_date__int__undef_signed` map<date,int> NULL,\n"
                + "  `col_map_date__int__undef_signed_not_null` map<date,int> NOT NULL,\n"
                + "  `col_map_datetime_6___bigint__undef_signed` map<datetime(6),bigint> NULL,\n"
                + "  `col_map_datetime_6___bigint__undef_signed_not_null` map<datetime(6),bigint> NOT NULL,\n"
                + "  `col_map_datetime_3___largeint__undef_signed` map<datetime(3),largeint> NULL,\n"
                + "  `col_map_datetime_3___largeint__undef_signed_not_null` map<datetime(3),largeint> NOT NULL,\n"
                + "  `col_map_datetime__decimal_76__50___undef_signed` map<datetime,decimal(76,50)> NULL,\n"
                + "  `col_map_datetime__decimal_76__50___undef_signed_not_null` map<datetime,decimal(76,50)> NOT NULL,\n"
                + "  `col_map_date__decimal_16_10___undef_signed` map<date,decimal(16,10)> NULL,\n"
                + "  `col_map_date__decimal_16_10___undef_signed_not_null` map<date,decimal(16,10)> NOT NULL,\n"
                + "  `col_map_date__decimal_37__12___undef_signed` map<date,decimal(37,12)> NULL,\n"
                + "  `col_map_date__decimal_37__12___undef_signed_not_null` map<date,decimal(37,12)> NOT NULL,\n"
                + "  `col_struct` struct<c_boolean:boolean,c_tinyint:tinyint,c_smallint:smallint,c_int:int,c_bigint:bigint,c_largeint:largeint,c_decimal_10_0:decimal(10,0),c_decimal_16_10:decimal(16,10),c_decimal_37_12:decimal(37,12),c_decimal_17_0:decimal(17,0),c_decimal_8_4:decimal(8,4),c_decimal_9_0:decimal(9,0),c_decimal_76_56:decimal(76,56)> NULL,\n"
                + "  `col_struct2` struct<c_boolean:boolean,c_tinyint:tinyint,c_smallint:smallint,c_int:int,c_bigint:bigint,c_largeint:largeint,c_decimal_10_0:decimal(10,0),c_decimal_16_10:decimal(16,10),c_decimal_37_12:decimal(37,12),c_decimal_17_0:decimal(17,0),c_decimal_8_4:decimal(8,4),c_decimal_9_0:decimal(9,0),c_decimal_76_56:decimal(76,56)> NOT NULL,\n"
                + "  `col_variant_undef_signed` variant NULL,\n"
                + "  `col_variant_undef_signed_not_null` variant NOT NULL,\n"
                + "  INDEX col_tinyint_undef_signed_index_inverted_idx (`col_tinyint_undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_tinyint_undef_signed_not_null_index_inverted_idx (`col_tinyint_undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_smallint_undef_signed_index_inverted_idx (`col_smallint_undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_smallint_undef_signed_not_null_index_inverted_idx (`col_smallint_undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_int_undef_signed_index_inverted_idx (`col_int_undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_int_undef_signed_not_null_index_inverted_idx (`col_int_undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_bigint_undef_signed_index_inverted_idx (`col_bigint_undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_bigint_undef_signed_not_null_index_inverted_idx (`col_bigint_undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_10_0__undef_signed_index_inverted_idx (`col_decimal_10_0__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_10_0__undef_signed_not_null_index_inverted_idx (`col_decimal_10_0__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_16_10__undef_signed_index_inverted_idx (`col_decimal_16_10__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_16_10__undef_signed_not_null_index_inverted_idx (`col_decimal_16_10__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_37__12__undef_signed_index_inverted_idx (`col_decimal_37__12__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_37__12__undef_signed_not_null_index_inverted_idx (`col_decimal_37__12__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_17_0__undef_signed_index_inverted_idx (`col_decimal_17_0__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_17_0__undef_signed_not_null_index_inverted_idx (`col_decimal_17_0__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_8_4__undef_signed_index_inverted_idx (`col_decimal_8_4__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_8_4__undef_signed_not_null_index_inverted_idx (`col_decimal_8_4__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_9_0__undef_signed_index_inverted_idx (`col_decimal_9_0__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_9_0__undef_signed_not_null_index_inverted_idx (`col_decimal_9_0__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_76__56__undef_signed_index_inverted_idx (`col_decimal_76__56__undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_decimal_76__56__undef_signed_not_null_index_inverted_idx (`col_decimal_76__56__undef_signed_not_null_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_datetime_undef_signed_index_inverted_idx (`col_datetime_undef_signed_index_inverted`) USING INVERTED,\n"
                + "  INDEX col_datetime_undef_signed_not_null_index_inverted_idx (`col_datetime_undef_signed_not_null_index_inverted`) USING INVERTED\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(`col_tinyint_undef_signed_index_inverted`, `col_smallint_undef_signed_not_null_index_inverted`, `pk`, `col_int_undef_signed`, `col_bigint_undef_signed`, `col_decimal_10_0__undef_signed`, `col_largeint_undef_signed`)\n"
                + "PARTITION BY RANGE(`col_tinyint_undef_signed_index_inverted`, `col_smallint_undef_signed_not_null_index_inverted`)\n"
                + "(PARTITION p0 VALUES [(\"-128\", \"-32768\"), (\"0\", \"0\")),\n"
                + "PARTITION p1 VALUES [(\"0\", \"0\"), (\"10\", \"256\")),\n"
                + "PARTITION p2 VALUES [(\"10\", \"256\"), (\"50\", \"10240\")),\n"
                + "PARTITION p3 VALUES [(\"50\", \"10240\"), (\"100\", \"32767\")),\n"
                + "PARTITION p4 VALUES [(\"100\", \"32767\"), (MAXVALUE, MAXVALUE)))\n"
                + "DISTRIBUTED BY HASH(`pk`) BUCKETS 10\n"
                + "PROPERTIES (\n"
                + "\"replication_allocation\" = \"tag.location.default: 1\""
                + ")");

        PlanChecker.from(connectContext)
                .analyze("SELECT col_variant_undef_signed_not_null,\n"
                        + "          col_struct,\n"
                        + "          col_variant_undef_signed_not_null[\"c_map_largeint\"]\n"
                        + " FROM table_20_undef_partitions2_keys3_properties4_distributed_by56")
                .rewrite()
                .getCascadesContext()
                .getRewritePlan();
    }

    private void setAccessPathAndAssertType(SlotReference slot, List<String> path, String expectedType) {
        setAccessPathsAndAssertType(slot, ImmutableList.of(path), expectedType);
    }

    private void setAccessPathsAndAssertType(SlotReference slot, List<List<String>> paths, String expectedType) {
        DataType columnType = DataType.fromCatalogType(slot.getOriginalColumn().get().getType());
        SlotReference originColumnTypeSlot = new SlotReference(slot.getName(), columnType);
        DataTypeAccessTree tree = DataTypeAccessTree.ofRoot(originColumnTypeSlot, TAccessPathType.DATA);
        for (List<String> path : paths) {
            tree.setAccessByPath(path, 0, TAccessPathType.DATA);
        }
        DataType dataType = tree.pruneDataType().get();
        Assertions.assertEquals(expectedType, dataType.toSql());
    }

    private List<Pair<SlotReference, DataTypeAccessTree>> getDataTypeAccessTrees(String sql) {
        Plan rewritePlan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .getCascadesContext()
                .getRewritePlan();

        List<Slot> output = ((LogicalOlapScan) rewritePlan.collect(LogicalOlapScan.class::isInstance)
                .iterator()
                .next())
                .getOutput();
        List<Pair<SlotReference, DataTypeAccessTree>> trees = new ArrayList<>();
        for (Slot slot : output) {
            if (slot.getDataType() instanceof NestedColumnPrunable) {
                DataTypeAccessTree dataTypeAccessTree = DataTypeAccessTree.ofRoot(slot, TAccessPathType.DATA);
                trees.add(Pair.of((SlotReference) slot, dataTypeAccessTree));
            }
        }
        return trees;
    }

    private void assertColumn(String sql, String expectType,
            List<TColumnAccessPath> expectAllAccessPaths,
            List<TColumnAccessPath> expectPredicateAccessPaths) throws Exception {
        assertColumns(sql, expectType == null ? null : ImmutableList.of(Triple.of(expectType, expectAllAccessPaths, expectPredicateAccessPaths)));
    }

    private void assertColumns(String sql,
            List<Triple<String, List<TColumnAccessPath>, List<TColumnAccessPath>>> expectResults) throws Exception {
        Pair<PhysicalPlan, List<SlotDescriptor>> result = collectComplexSlots(sql);
        PhysicalPlan physicalPlan = result.first;
        List<SlotDescriptor> slotDescriptors = result.second;
        if (expectResults == null) {
            Assertions.assertEquals(0, slotDescriptors.size());
            return;
        }

        Assertions.assertEquals(expectResults.size(), slotDescriptors.size());
        int slotIndex = 0;
        for (Triple<String, List<TColumnAccessPath>, List<TColumnAccessPath>> expectResult : expectResults) {
            String expectType = expectResult.left;
            List<TColumnAccessPath> expectAllAccessPaths = expectResult.middle;
            List<TColumnAccessPath> expectPredicateAccessPaths = expectResult.right;
            SlotDescriptor slotDescriptor = slotDescriptors.get(slotIndex++);
            Assertions.assertEquals(expectType, slotDescriptor.getType().toString());

            TreeSet<TColumnAccessPath> expectAllAccessPathSet = new TreeSet<>(expectAllAccessPaths);
            TreeSet<TColumnAccessPath> actualAllAccessPaths
                    = new TreeSet<>(slotDescriptor.getAllAccessPaths());
            Assertions.assertEquals(expectAllAccessPathSet, actualAllAccessPaths);

            TreeSet<TColumnAccessPath> expectPredicateAccessPathSet = new TreeSet<>(expectPredicateAccessPaths);
            TreeSet<TColumnAccessPath> actualPredicateAccessPaths
                    = new TreeSet<>(slotDescriptor.getPredicateAccessPaths());
            Assertions.assertEquals(expectPredicateAccessPathSet, actualPredicateAccessPaths);

            Map<Integer, DataType> slotIdToDataTypes = new LinkedHashMap<>();
            Consumer<Expression> assertHasSameType = e -> {
                if (e instanceof NamedExpression) {
                    DataType dataType = slotIdToDataTypes.get(((NamedExpression) e).getExprId().asInt());
                    if (dataType != null) {
                        Assertions.assertEquals(dataType, e.getDataType());
                    } else {
                        slotIdToDataTypes.put(((NamedExpression) e).getExprId().asInt(), e.getDataType());
                    }
                }
            };

            // assert same slot id has same type
            physicalPlan.foreachUp(plan -> {
                List<? extends Expression> expressions = ((PhysicalPlan) plan).getExpressions();
                for (Expression expression : expressions) {
                    expression.foreach(e -> {
                        assertHasSameType.accept((Expression) e);
                        if (e instanceof Alias && e.child(0) instanceof Slot) {
                            assertHasSameType.accept((Alias) e);
                        } else if (e instanceof ArrayItemReference) {
                            assertHasSameType.accept((ArrayItemReference) e);
                        }
                    });
                }

                if (plan instanceof PhysicalCTEConsumer) {
                    for (Entry<Slot, Collection<Slot>> kv : ((PhysicalCTEConsumer) plan).getProducerToConsumerSlotMap()
                            .asMap().entrySet()) {
                        Slot producerSlot = kv.getKey();
                        for (Slot consumerSlot : kv.getValue()) {
                            Assertions.assertEquals(producerSlot.getDataType(), consumerSlot.getDataType());
                        }
                    }
                } else if (plan instanceof PhysicalUnion) {
                    List<Slot> output = ((PhysicalUnion) plan).getOutput();
                    for (List<SlotReference> regularChildrenOutput : ((PhysicalUnion) plan).getRegularChildrenOutputs()) {
                        Assertions.assertEquals(output.size(), regularChildrenOutput.size());
                        for (int i = 0; i < output.size(); i++) {
                            Assertions.assertEquals(output.get(i).getDataType(), regularChildrenOutput.get(i).getDataType());
                        }
                    }
                }
            });
        }
    }

    private Pair<PhysicalPlan, List<SlotDescriptor>> collectComplexSlots(String sql) throws Exception {
        NereidsPlanner planner = (NereidsPlanner) executeNereidsSql(sql).planner();
        List<SlotDescriptor> complexSlots = new ArrayList<>();
        PhysicalPlan physicalPlan = planner.getPhysicalPlan();
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
        return Pair.of(physicalPlan, complexSlots);
    }

    private TColumnAccessPath path(String... path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.DATA);
        accessPath.data_access_path = new TDataAccessPath(ImmutableList.copyOf(path));
        return accessPath;
    }

    private TColumnAccessPath metaPath(String... path) {
        TColumnAccessPath accessPath = new TColumnAccessPath(TAccessPathType.META);
        accessPath.meta_access_path = new TMetaAccessPath(ImmutableList.copyOf(path));
        return accessPath;
    }
}
