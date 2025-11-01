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
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.StructElement;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.types.DataType;
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
        assertColumn("select struct_element(cast(s as struct<k:text,l:array<map<int,struct<x:int,y:double>>>>), 'k') from tbl",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("select struct_element(map_values(struct_element(cast(s as struct<k:text,l:array<map<int,struct<x:double,y:double>>>>), 'l')[0])[0], 'x') from tbl",
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
        assertColumn("select struct_element(s, 'city') from (select s from tbl union all select null)a",
                "struct<city:text>",
                ImmutableList.of(path("s", "city")),
                ImmutableList.of()
        );

        assertColumn("select * from (select struct_element(s, 'city') from tbl union all select null)a",
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
    public void testPushDownThroughJoin() {
        PlanChecker.from(connectContext)
                .analyze("select struct_element(s, 'city') from (select * from tbl)a join (select 100 id, 'f1' name)b on a.id=b.id")
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
                            Assertions.assertTrue(p.getProjects().size() == 1 && p.getProjects().get(0) instanceof SlotReference);
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
                            Assertions.assertTrue(p.getProjects().size() == 2 && p.getProjects().get(0) instanceof SlotReference);
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
                                    Assertions.assertTrue(p.getProjects().size() == 2 && p.getProjects().get(0) instanceof SlotReference);
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
