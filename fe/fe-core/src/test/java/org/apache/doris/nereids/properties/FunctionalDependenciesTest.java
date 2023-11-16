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
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
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
        connectContext.setDatabase("default_cluster:test");
    }

    @Test
    void testUniform() {
        FunctionalDependencies fd = new FunctionalDependencies();
        fd.addUniformSlot(slot1);
        Assertions.assertTrue(fd.isUniform(slot1));
        Assertions.assertFalse(fd.isUniform(slot2));
        fd.addUniformSlot(ImmutableSet.of(slot2));
        Assertions.assertTrue(fd.isUniform(slot2));
        ImmutableSet<Slot> slotSet = ImmutableSet.of(slot1, slot2, slot3);
        fd.addUniformSlot(slotSet);
        Assertions.assertTrue(fd.isUniform(slotSet));
        Assertions.assertFalse(fd.isUniform(ImmutableSet.of(slot1, slot2, slot3, slot4)));
        Assertions.assertTrue(fd.isUniform(ImmutableSet.of(slot1, slot2)));
        Assertions.assertFalse(fd.isUniform(ImmutableSet.of(slot3, slot2)));
    }

    @Test
    void testUnique() {
        FunctionalDependencies fd = new FunctionalDependencies();
        fd.addUniqueSlot(slot1);
        Assertions.assertTrue(fd.isUnique(slot1));
        Assertions.assertFalse(fd.isUnique(slot2));
        fd.addUniqueSlot(ImmutableSet.of(slot2));
        Assertions.assertTrue(fd.isUnique(slot2));
        ImmutableSet<Slot> slotSet = ImmutableSet.of(slot1, slot2, slot3);
        fd.addUniqueSlot(slotSet);
        Assertions.assertTrue(fd.isUnique(slotSet));
        Assertions.assertTrue(fd.isUnique(ImmutableSet.of(slot1, slot2, slot3, slot4)));
        Assertions.assertFalse(fd.isUnique(ImmutableSet.of(slot3, slot4)));
    }

    @Test
    void testMergeFD() {
        FunctionalDependencies fd1 = new FunctionalDependencies();
        fd1.addUniqueSlot(slot1);
        FunctionalDependencies fd2 = new FunctionalDependencies();
        fd2.addUniformSlot(slot2);

        fd1.addFunctionalDependencies(fd2);
        Assertions.assertTrue(fd1.isUniform(slot2));
    }

    @Test
    void testScan() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies()
                .isUnique(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id from uni")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies()
                .isUnique(plan.getOutput().get(0)));
    }

    @Test
    void testFilter() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg where name = \"1\"")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getFunctionalDependencies()
                .isUniform(plan.getOutput().get(0)));
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
                .getFunctionalDependencies().isUnique(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left semi join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left anti join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id from agg right outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(1)));
    }

    @Test
    void testAgg() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select count(1), name from agg group by name")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(1)));

        plan = PlanChecker.from(connectContext)
                .analyze("select count(1), max(id), id from agg group by id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(2)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select count(1), id from agg where id = 1 group by id")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select count(name) from agg")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testWindow() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select rank() over(partition by id) from agg")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select rank() over(partition by name) from agg where name = '1'")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUnique(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select rank() over(partition by name) from agg where name = '1' limit 1")
                .rewrite()
                .getPlan();
        LogicalPartitionTopN ptopn = (LogicalPartitionTopN) plan.child(0).child(0).child(0).child(0).child(0);
        System.out.println(ptopn.getLogicalProperties().getFunctionalDependencies());
        System.out.println(ptopn.getOutput());
        Assertions.assertTrue(ptopn.getLogicalProperties()
                .getFunctionalDependencies().isUniform(ImmutableSet.copyOf(ptopn.getOutputSet())));

        plan = PlanChecker.from(connectContext)
                .analyze("select rank() over(partition by name) from agg limit 1")
                .rewrite()
                .getPlan();
        ptopn = (LogicalPartitionTopN) plan.child(0).child(0).child(0).child(0).child(0);
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
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));

        plan = PlanChecker.from(connectContext)
                .analyze("select name from agg order by id limit 1")
                .rewrite()
                .getPlan();
        System.out.println(plan.getLogicalProperties().getFunctionalDependencies());
        Assertions.assertTrue(plan.getLogicalProperties()
                .getFunctionalDependencies().isUniform(plan.getOutput().get(0)));
    }
}
