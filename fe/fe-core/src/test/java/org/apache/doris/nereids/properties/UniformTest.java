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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UniformTest extends TestWithFeService {
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
        // group by unique
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select count(id) from agg group by id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniform(plan.getOutput().get(0)));

        // propagate uniform
        plan = PlanChecker.from(connectContext)
                .analyze("select id from agg where id = 1 group by id ")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniform(plan.getOutput().get(0)));

        // group by all/uniform
        plan = PlanChecker.from(connectContext)
                .analyze("select sum(id) from agg ")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniform(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select sum(id) from agg where id = 1 group by id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniform(plan.getOutput().get(0)));

    }

    @Test
    void testTopNLimit() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg limit 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniformAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select name from agg order by name limit 1 ")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniformAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testSetOp() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg limit 1 except select name from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniform(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id from agg intersect select name from agg limit 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testFilterHaving() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg where id = 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniformAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select name from uni group by name having name = \"\"")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniformAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testGenerate() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg lateral view explode([1,2,3]) tmp1 as e1")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties()
                .getTrait().isUniform(plan.getOutputSet()));
    }

    @Test
    void testJoin() {
        // foj doesn't propagate any
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select uni.id from agg join uni "
                        + "on agg.id = uni.id where uni.id = 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testOneRowRelation() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testProject() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id as id2, 1 as id3 from uni where id = 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniformAndNotNull(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniformAndNotNull(plan.getOutput().get(1)));
    }

    @Test
    void testSubQuery() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from (select id from agg where id = 1) t")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniformAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testWindow() {
        // partition by uniform
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select row_number() over(partition by id) from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniform(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select rank() over(partition by id) from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniform(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select dense_rank() over(partition by id) from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniform(plan.getOutput().get(0)));
    }

    @Test
    void testEqual() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id2 from agg where id = 1 and id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutputSet()));
    }
}
