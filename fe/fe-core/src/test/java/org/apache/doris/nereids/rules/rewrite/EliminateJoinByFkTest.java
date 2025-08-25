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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

class EliminateJoinByFkTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS pri (\n"
                        + "    id1 int not null\n"
                        + ")\n"
                        + "UNIQUE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_not_null (\n"
                        + "    id2 int not null\n"
                        + ")\n"
                        + "UNIQUE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_null (\n"
                        + "    id3 int\n"
                        + ")\n"
                        + "UNIQUE KEY(id3)\n"
                        + "DISTRIBUTED BY HASH(id3) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
        addConstraint("Alter table pri add constraint pk primary key (id1)");
        addConstraint("Alter table foreign_not_null add constraint f_not_null foreign key (id2)\n"
                + "references pri(id1)");
        addConstraint("Alter table foreign_null add constraint f_not_null foreign key (id3)\n"
                + "references pri(id1)");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testPriWithJoin() {
        String sql = "select pri.id1 from (select p1.id1 from pri as p1 cross join pri as p2) pri "
                + "inner join foreign_not_null on pri.id1 = foreign_not_null.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin())
                .printlnTree();
    }

    @Test
    void testNotNull() {
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();

        sql = "select foreign_not_null.id2 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .printlnTree();
    }

    @Test
    void testNotNullWithPredicate() {
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .matches(logicalFilter().when(f -> {
                    Assertions.assertTrue(f.getPredicate().toSql().contains("(id2 = 1)"));
                    return true;
                }))
                .printlnTree();
        sql = "select foreign_not_null.id2 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .matches(logicalFilter().when(f -> {
                    Assertions.assertTrue(f.getPredicate().toSql().contains("(id2 = 1)"));
                    return true;
                }))
                .printlnTree();
    }

    @Test
    void testNull() throws Exception {
        String sql = "select pri.id1, 1 + foreign_null.id3 as k from pri inner join foreign_null on pri.id1 = foreign_null.id3";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .matches(
                    logicalResultSink(
                        logicalProject(
                            logicalFilter().when(f -> {
                                Assertions.assertTrue(f.getPredicate().toSql().contains("( not id3 IS NULL)"));
                                return true;
                            })
                        ).when(project -> {
                            List<NamedExpression> projects = project.getProjects();
                            Assertions.assertEquals(2, projects.size());
                            Assertions.assertEquals("non_nullable(id3) AS `id1`", projects.get(0).toSql());
                            Assertions.assertEquals("(cast(non_nullable(id3) as BIGINT) + 1) AS `k`", projects.get(1).toSql());
                            Assertions.assertFalse(projects.get(0).nullable());
                            Assertions.assertFalse(projects.get(1).nullable());
                            return true;
                        })
                    ).when(sink -> {
                        List<NamedExpression> projects = sink.getOutputExprs();
                        Assertions.assertEquals(2, projects.size());
                        Assertions.assertEquals("id1", projects.get(0).toSql());
                        Assertions.assertFalse(projects.get(0).nullable());
                        Assertions.assertEquals("k", projects.get(1).toSql());
                        Assertions.assertFalse(projects.get(1).nullable());
                        return true;
                    })
                )
                .printlnTree();
        sql = "select foreign_null.id3 from pri inner join foreign_null on pri.id1 = foreign_null.id3";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .matches(logicalFilter().when(f -> {
                    Assertions.assertTrue(f.getPredicate().toSql().contains("( not id3 IS NULL)"));
                    return true;
                }))
                .printlnTree();
    }

    @Test
    void testNullWithPredicate() throws Exception {
        String sql = "select pri.id1 from pri inner join foreign_null on pri.id1 = foreign_null.id3\nwhere pri.id1 = 1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .matches(logicalFilter().when(f -> {
                    Assertions.assertEquals("( not id3 IS NULL)", f.getExpressions().get(0).toSql());
                    Assertions.assertEquals("(id3 = 1)", f.getExpressions().get(1).toSql());
                    return true;
                }))
                .printlnTree();
        sql = "select id3 from pri inner join foreign_null on pri.id1 = foreign_null.id3\n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin())
                .matches(logicalFilter().when(f -> {
                    Assertions.assertEquals("( not id3 IS NULL)", f.getExpressions().get(0).toSql());
                    Assertions.assertEquals("(id3 = 1)", f.getExpressions().get(1).toSql());
                    return true;
                }))
                .printlnTree();
    }

    @Test
    void testMultiJoinCanPassForeign() throws Exception {
        String sql = "select id1 from "
                + "foreign_null inner join foreign_not_null on id2 = id3\n"
                + "inner join pri on id1 = id3";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalOlapScan().when(scan -> scan.getTable().getName().equals("pri")))
                .printlnTree();
    }

    @Test
    void testMultiJoinCannotPassPrimary() throws Exception {
        String sql = "select id1 from "
                + "foreign_null "
                + "inner join pri on id1 = id3\n"
                + "inner join foreign_not_null on id2 = id3\n";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalOlapScan().when(scan -> scan.getTable().getName().equals("pri")))
                .printlnTree();
    }

    @Test
    void testOtherCond() {
        String sql = "select pri.id1 from pri inner join foreign_null on pri.id1 > foreign_null.id3 \n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalOlapScan().when(scan -> scan.getTable().getName().equals("pri")));

        sql = "select pri.id1 from pri inner join foreign_null on pri.id1 != foreign_null.id3 \n"
                + "where pri.id1 = 1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalOlapScan().when(scan -> scan.getTable().getName().equals("pri")));
    }

    @Test
    void testReplaceMap() {
        Slot a = new SlotReference("a", IntegerType.INSTANCE);
        Slot b = new SlotReference("b", IntegerType.INSTANCE);
        Slot x = new SlotReference("x", IntegerType.INSTANCE);
        Slot y = new SlotReference("y", IntegerType.INSTANCE);
        Slot z = new SlotReference("z", IntegerType.INSTANCE);
        Map<Slot, Slot> outputToForeign = Maps.newHashMap();
        outputToForeign.put(a, x);
        outputToForeign.put(b, y);

        Set<Slot> compensationForeignSlots = Sets.newHashSet();
        compensationForeignSlots.add(x);
        compensationForeignSlots.add(z);

        Map<Slot, Expression> replacedSlots = new EliminateJoinByFK().getReplaceSlotMap(outputToForeign, compensationForeignSlots);
        Map<Slot, Expression> expectedReplacedSlots = Maps.newHashMap();
        expectedReplacedSlots.put(a, new NonNullable(x));
        expectedReplacedSlots.put(b, y);
        expectedReplacedSlots.put(x, new NonNullable(x));
        expectedReplacedSlots.put(z, new NonNullable(z));
        Assertions.assertEquals(expectedReplacedSlots, replacedSlots);
    }

    @Test
    void testyNullCompensationFilter() {
        EliminateJoinByFK instance = new EliminateJoinByFK();
        SlotReference notNull1 = new SlotReference("notNull1", IntegerType.INSTANCE, false);
        SlotReference notNull2 = new SlotReference("notNull2", IntegerType.INSTANCE, false);
        SlotReference null1 = new SlotReference("null1", IntegerType.INSTANCE, true);
        SlotReference null2 = new SlotReference("null2", IntegerType.INSTANCE, true);
        LogicalOneRowRelation oneRowRelation = new LogicalOneRowRelation(new RelationId(100), ImmutableList.of());
        Pair<Plan, Set<Slot>> result1 = instance.applyNullCompensationFilter(oneRowRelation, ImmutableSet.of(notNull1, notNull2));
        Assertions.assertEquals(ImmutableSet.of(), result1.second);
        Assertions.assertEquals(oneRowRelation, result1.first);
        Pair<Plan, Set<Slot>> result2 = instance.applyNullCompensationFilter(oneRowRelation, ImmutableSet.of(notNull1, notNull2, null1, null2));
        Assertions.assertEquals(ImmutableSet.of(null1, null2), result2.second);
        LogicalFilter<?> expectFilter = new LogicalFilter<>(
                ImmutableSet.of(new Not(new IsNull(null1)), new Not(new IsNull(null2))),
                oneRowRelation);
        Assertions.assertEquals(expectFilter, result2.first);
    }
}
