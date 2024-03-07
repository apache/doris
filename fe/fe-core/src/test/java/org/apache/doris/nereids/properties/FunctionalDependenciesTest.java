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

import org.apache.doris.nereids.properties.FunctionalDependencies.Builder;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class FunctionalDependenciesTest extends TestWithFeService {
    Slot slot1 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot slot2 = new SlotReference("2", IntegerType.INSTANCE, false);
    Slot slot3 = new SlotReference("1", IntegerType.INSTANCE, false);
    Slot slot4 = new SlotReference("1", IntegerType.INSTANCE, false);

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createTable("create table test.agg (\n"
                + "id int not null,\n"
                + "name varchar(128) replace not null )\n"
                + "AGGREGATE KEY(id)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        createTable("create table test.uni (\n"
                + "id int not null,\n"
                + "name varchar(128) not null)\n"
                + "UNIQUE KEY(id)\n"
                + "distributed by hash(id) buckets 10\n"
                + "properties('replication_num' = '1');");
        connectContext.setDatabase("test");
    }

    @Test
    void testUniform() {
        Builder fdBuilder = new Builder();
        fdBuilder.addUniformSlot(slot1);
        FunctionalDependencies fd = fdBuilder.build();
        Assertions.assertTrue(fd.isUniformAndNotNull(slot1));
        Assertions.assertFalse(fd.isUniformAndNotNull(slot2));
        fdBuilder.addUniformSlot(ImmutableSet.of(slot2));
        fd = fdBuilder.build();
        Assertions.assertTrue(fd.isUniformAndNotNull(slot2));
        ImmutableSet<Slot> slotSet = ImmutableSet.of(slot1, slot2, slot3);
        fdBuilder.addUniformSlot(slotSet);
        fd = fdBuilder.build();
        Assertions.assertTrue(fd.isUniformAndNotNull(slotSet));
        Assertions.assertFalse(fd.isUniformAndNotNull(ImmutableSet.of(slot1, slot2, slot3, slot4)));
        Assertions.assertTrue(fd.isUniformAndNotNull(ImmutableSet.of(slot1, slot2)));
        Assertions.assertFalse(fd.isUniformAndNotNull(ImmutableSet.of(slot3, slot2)));
    }

    @Test
    void testUnique() {
        Builder fdBuilder = new Builder();
        fdBuilder.addUniqueSlot(slot1);
        FunctionalDependencies fd = fdBuilder.build();
        Assertions.assertTrue(fd.isUniqueAndNotNull(slot1));
        Assertions.assertFalse(fd.isUniqueAndNotNull(slot2));
        fdBuilder.addUniqueSlot(slot2);
        fd = fdBuilder.build();
        Assertions.assertTrue(fd.isUniqueAndNotNull(slot2));
        ImmutableSet<Slot> slotSet = ImmutableSet.of(slot1, slot2, slot3);
        fdBuilder.addUniqueSlot(slotSet);
        fd = fdBuilder.build();
        Assertions.assertTrue(fd.isUniqueAndNotNull(slotSet));
        Assertions.assertTrue(fd.isUniqueAndNotNull(ImmutableSet.of(slot1, slot2, slot3, slot4)));
        Assertions.assertFalse(fd.isUniqueAndNotNull(ImmutableSet.of(slot3, slot4)));
    }

    @Test
    void testMergeFD() {
        Builder fdBuilder1 = new Builder();
        fdBuilder1.addUniformSlot(slot1);
        Builder fdBuilder2 = new Builder();
        fdBuilder2.addUniformSlot(slot2);

        fdBuilder1.addFunctionalDependencies(fdBuilder2.build());
        FunctionalDependencies fd = fdBuilder1.build();
        Assertions.assertTrue(fd.isUniformAndNotNull(slot1));
        Assertions.assertTrue(fd.isUniformAndNotNull(slot2));
    }

    @Test
    void testScan() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id from uni")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testFilter() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg where name = \"1\"")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies()
                .isUniformAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testJoin() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select * from agg full outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies().isEmpty());

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg full outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies().isEmpty());

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left semi join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left anti join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id from agg right outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(1)));
    }

    @Test
    void testAgg() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select count(1), name from agg group by name")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(1)));

        plan = PlanChecker.from(connectContext)
                .analyze("select count(1), max(id), id from agg group by id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(2)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select count(1), id from agg where id = 1 group by id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select count(name) from agg")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select id from agg where id = 1 group by cube(id, name)")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testGenerate() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select k1 from (select 1 k1) as t lateral view explode([1,2,3]) tmp1 as e1")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testWindow() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select row_number() over(partition by id) from agg")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select row_number() over(partition by name) from agg where name = '1'")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select row_number() over(partition by name) from agg where name = '1' limit 1")
                .rewrite()
                .getPlan();
        LogicalPartitionTopN<?> ptopn = (LogicalPartitionTopN<?>) plan.child(0).child(0).child(0).child(0).child(0);
        System.out.println(ptopn.getLogicalProperties().getFunctionalDependencies());
        System.out.println(ptopn.getOutput());
        Assertions.assertTrue(ptopn.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(ImmutableSet.copyOf(ptopn.getOutputSet())));

        plan = PlanChecker.from(connectContext)
                .analyze("select row_number() over(partition by name) from agg limit 1")
                .rewrite()
                .getPlan();
        ptopn = (LogicalPartitionTopN<?>) plan.child(0).child(0).child(0).child(0).child(0);
        Assertions.assertTrue(ptopn.getLogicalProperties()
                .getFunctionalDependencies().isEmpty());
    }

    @Test
    void testProject() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg where id = 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isEmpty());
    }

    @Test
    void testLimitAndTopN() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg limit 1")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select name from agg order by id limit 1")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testSubQuery() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from (select * from agg) t")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Disabled
    @Test
    void testCTE() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("with t as (select * from agg) select id from t where id = 1")
                .getPlan();
        System.out.println(plan.treeString());
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testSetOperation() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select * from agg union select * from uni")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(ImmutableSet.copyOf(plan.getOutput())));

        plan = PlanChecker.from(connectContext)
                .analyze("select * from agg union all select * from uni")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertFalse(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(ImmutableSet.copyOf(plan.getOutput())));

        plan = PlanChecker.from(connectContext)
                .analyze("select * from agg intersect select * from uni where name = \"1\"")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(ImmutableSet.copyOf(plan.getOutput())));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniformAndNotNull(plan.getOutput().get(1)));

        plan = PlanChecker.from(connectContext)
                .analyze("select * from agg except select * from uni")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(ImmutableSet.copyOf(plan.getOutput())));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniqueAndNotNull(plan.getOutput().get(0)));
    }
}
