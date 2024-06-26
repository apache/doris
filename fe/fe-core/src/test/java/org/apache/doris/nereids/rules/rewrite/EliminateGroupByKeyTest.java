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

import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class EliminateGroupByKeyTest extends TestWithFeService implements MemoPatternMatchSupported {
    Slot s1 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot s2 = new SlotReference("2", IntegerType.INSTANCE, false);
    Slot s3 = new SlotReference("3", IntegerType.INSTANCE, false);
    Slot s4 = new SlotReference("4", IntegerType.INSTANCE, false);
    Set<Slot> set1 = Sets.newHashSet(s1);
    Set<Slot> set2 = Sets.newHashSet(s2);
    Set<Slot> set3 = Sets.newHashSet(s3);
    Set<Slot> set4 = Sets.newHashSet(s4);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.t1 (\n"
                + "id int not null,\n"
                + "name varchar(128) not null)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        createTable("create table test.uni (\n"
                + "id int not null,\n"
                + "name varchar(128) not null)\n"
                + "UNIQUE KEY(id)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testEliminateChain() {
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(set1, set2);
        funcDeps.addFuncItems(set2, set3);
        funcDeps.addFuncItems(set3, set4);
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(ImmutableSet.of(set1, set2, set3, set4), ImmutableSet.of());
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(set1, slots.iterator().next());
    }

    @Test
    void testEliminateCircle() {
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(set1, set2);
        funcDeps.addFuncItems(set2, set3);
        funcDeps.addFuncItems(set3, set4);
        funcDeps.addFuncItems(set4, set1);
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(ImmutableSet.of(set1, set2, set3, set4), ImmutableSet.of());
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(set1, slots.iterator().next());
    }

    @Test
    void testEliminateTree() {
        FuncDeps funcDeps = new FuncDeps();
        funcDeps.addFuncItems(set1, set2);
        funcDeps.addFuncItems(set1, set3);
        funcDeps.addFuncItems(set1, set4);
        Set<Set<Slot>> slots = funcDeps.eliminateDeps(ImmutableSet.of(set1, set2, set3, set4), ImmutableSet.of());
        Assertions.assertEquals(1, slots.size());
        Assertions.assertEquals(set1, slots.iterator().next());
    }

    @Test
    void testEliminateByUniform() {
        PlanChecker.from(connectContext)
                .analyze("select count(name) from t1 where id = 1 group by name, id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1 && agg.getGroupByExpressions().get(0).toSql().equals("name")));
    }

    @Test
    void testProjectAlias() {
        PlanChecker.from(connectContext)
                .analyze("select id as c from t1 where id = 1 group by name, id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select id as c, name as n from t1 group by name, id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 2));
        PlanChecker.from(connectContext)
                .analyze("select name as n, count(id) as c from t1 where name = \"\" group by name, id having c = 2")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 2));
    }

    @Test
    void testEliminateByUnique() {
        PlanChecker.from(connectContext)
                .analyze("select count(t1.id) from uni as t1 cross join uni as t2 group by t1.name, t1.id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1 && agg.getGroupByExpressions().get(0).toSql().equals("id")));
        PlanChecker.from(connectContext)
                .analyze("select count(t1.id) from uni as t1 cross join uni as t2 group by t1.name, t2.id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 2));
    }

    @Test
    void testEliminateByPk() throws Exception {
        addConstraint("alter table t1 add constraint pk primary key (id)");
        PlanChecker.from(connectContext)
                .analyze("select count(t1.id) from t1 as t1 cross join t1 as t2 group by t1.name, t1.id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1 && agg.getGroupByExpressions().get(0).toSql().equals("id")));
        PlanChecker.from(connectContext)
                .analyze("select count(t1.id) from t1 as t1 cross join t1 as t2 group by t1.name, t2.id")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 2));
        dropConstraint("alter table t1 drop constraint pk");
    }

    @Test
    void testEliminateByEqual() {
        PlanChecker.from(connectContext)
                .analyze("select t1.name from t1 as t1 join t1 as t2 on t1.name = t2.name group by t1.name, t2.name")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("name")));

        PlanChecker.from(connectContext)
                .analyze("select t2.name from t1 as t1 join t1 as t2 "
                        + "on t1.name = t2.name group by t1.name, t2.name")
                .rewrite()
                .printlnTree()
                .matches(logicalAggregate().when(agg ->
                        agg.getGroupByExpressions().size() == 1
                                && agg.getGroupByExpressions().get(0).toSql().equals("name")));
    }
}
