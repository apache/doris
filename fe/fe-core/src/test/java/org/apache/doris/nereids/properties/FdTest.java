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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

class FdTest extends TestWithFeService {
    Slot slot1 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot slot2 = new SlotReference("2", IntegerType.INSTANCE, false);
    Slot slot3 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot slot4 = new SlotReference("1", IntegerType.INSTANCE, false);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.agg (\n"
                + "id int not null,\n"
                + "id2 int replace not null,\n"
                + "name varchar(128) replace not null )\n"
                + "AGGREGATE KEY(id)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        createTable("create table test.uni (\n"
                + "id int not null,\n"
                + "id2 int not null,\n"
                + "name varchar(128) not null)\n"
                + "UNIQUE KEY(id)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void testAgg() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select sum(id2), id2 from agg group by id2")
                .getPlan();
        Set<Slot> output = ImmutableSet.copyOf(plan.getOutputSet());
        System.out.println(plan.getLogicalProperties()
                .getTrait().getAllValidFuncDeps(output));
        Assertions.assertTrue(
                plan.getLogicalProperties()
                .getTrait().getAllValidFuncDeps(output)
                        .isFuncDeps(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
    }

    @Test
    void testTopNLimit() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg group by id, id2 order by id limit 1")
                .getPlan();
        Set<Slot> output = ImmutableSet.copyOf(plan.getOutputSet());
        System.out.println(plan.getLogicalProperties()
                .getTrait().getAllValidFuncDeps(output));
        Assertions.assertTrue(
                plan.getLogicalProperties()
                        .getTrait().getAllValidFuncDeps(output)
                        .isFuncDeps(output, ImmutableSet.of(plan.getOutput().get(0))));
    }

    @Test
    void testSetOp() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id2 = id intersect select id, id2 from agg")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id2 = id except select id, id2 from agg")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id2 = id union all select id, id2 from agg")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isEmpty());
        plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg union all select id, id2 from agg where id2 = id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isEmpty());
    }

    @Test
    void testFilterHaving() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id = 1")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
        plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg  group by id, id2 having id = 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
    }

    @Test
    void testGenerate() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from  agg lateral view explode([1,2,3]) tmp1 as e1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
    }

    @Test
    void testJoin() {
        // inner join
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id, agg.id2 from agg join uni "
                        + "where agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(2))));

        // foj
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 full outer join uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));

        // loj
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 left outer join uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));

        // roj
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 right outer join uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));
    }

    @Test
    void testOneRowRelation() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select 1, 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
    }

    @Test
    void testProject() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id as o1, id as o2, id2 as o4, 1 as c1, 1 as c2 from uni where id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
    }

    @Test
    void testSubQuery() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from (select id, id2 from agg where id = id2) t")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
    }

    @Test
    void testWindow() {
        // partition by uniform
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2, row_number() over(partition by id) from agg where id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
    }

}
