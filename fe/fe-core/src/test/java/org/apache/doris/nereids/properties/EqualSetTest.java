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

import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnion;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class EqualSetTest extends TestWithFeService {
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
                .analyze("select id, id2 from agg where id2 = id group by id, id2")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
    }

    @Test
    void testTopNLimit() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id2 = id limit 1")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        plan = PlanChecker.from(connectContext)
                .analyze("(select id, id2 from agg where id2 = id limit 1) order by id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
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
                .analyze("select id, id2 from agg where id2 = id union all select id, id2 from agg where id2 = id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg union all select id, id2 from agg where id2 = id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isEmpty());
    }

    @Test
    void testUnionEqualSetUsesRegularChildOutputMapping() {
        String sql = "select name, id, id2 from agg where id = id2 "
                + "union all select name, id, id2 from agg where id = id2";
        LogicalUnion union = analyzeLogicalUnion(sql);
        Assertions.assertNotEquals(union.child(0).getOutput(), union.getRegularChildOutput(0),
                "the test must exercise an ordinal mapping that differs from child.getOutput()");
        assertUnionEqualPair(sql, union, 1, 2, true);
        assertUnionEqualPair(sql, union, 0, 1, false);
    }

    @Test
    void testUnionEqualSetChecksEveryConstantRow() {
        assertUnionEqualPair(
                "select id, id2 from agg where id = id2 "
                        + "union all select 1, 1 union all select 2, 2",
                0, 1, true);
        assertUnionEqualPair(
                "select id, id2 from agg where id = id2 "
                        + "union all select 1, 1 union all select 2, 3",
                0, 1, false);
        assertUnionEqualPair(
                "select id, id2 from agg where id = id2 "
                        + "union all select 1, 2 union all select 2, 1",
                0, 1, false);
    }

    @Test
    void testConstantOnlyUnionEqualSetUsesSqlEquality() {
        assertUnionEqualPair(
                "select cast(1 as int), cast(1 as bigint) "
                        + "union all select cast(2 as int), cast(2 as bigint)",
                0, 1, true);
        assertUnionEqualPair(
                "select 1 + 1, cast(2 as bigint) "
                        + "union all select 2 * 2, cast(4 as bigint)",
                0, 1, true);
        assertUnionEqualPair(
                "select 1, 1 union all select 2, 3",
                0, 1, false);
        assertUnionEqualPair(
                "select cast(null as int), cast(null as bigint) union all select 1, 1",
                0, 1, false);
    }

    private void assertUnionEqualPair(String sql, int leftIndex, int rightIndex, boolean expected) {
        assertUnionEqualPair(sql, analyzeLogicalUnion(sql), leftIndex, rightIndex, expected);
    }

    private void assertUnionEqualPair(String sql, LogicalUnion logicalUnion,
            int leftIndex, int rightIndex, boolean expected) {
        Assertions.assertEquals(expected, logicalUnion.getLogicalProperties().getTrait().isNullSafeEqual(
                logicalUnion.getOutput().get(leftIndex), logicalUnion.getOutput().get(rightIndex)));

        Plan physicalPlan = PlanChecker.from(connectContext).analyze(sql).rewrite().implement().getPhysicalPlan();
        PhysicalUnion physicalUnion = findPhysicalUnion(physicalPlan);
        Assertions.assertNotNull(physicalUnion, "expected a PhysicalUnion in: " + physicalPlan.treeString());
        DataTrait.Builder builder = new DataTrait.Builder();
        physicalUnion.computeEqualSet(builder);
        DataTrait physicalTrait = builder.build();
        Assertions.assertEquals(expected, physicalTrait.isNullSafeEqual(
                physicalUnion.getOutput().get(leftIndex), physicalUnion.getOutput().get(rightIndex)));
    }

    private LogicalUnion analyzeLogicalUnion(String sql) {
        Plan rewritten = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        LogicalUnion logicalUnion = findLogicalUnion(rewritten);
        Assertions.assertNotNull(logicalUnion, "expected a LogicalUnion in: " + rewritten.treeString());
        return logicalUnion;
    }

    private LogicalUnion findLogicalUnion(Plan plan) {
        if (plan instanceof LogicalUnion) {
            return (LogicalUnion) plan;
        }
        for (Plan child : plan.children()) {
            LogicalUnion union = findLogicalUnion(child);
            if (union != null) {
                return union;
            }
        }
        return null;
    }

    private PhysicalUnion findPhysicalUnion(Plan plan) {
        if (plan instanceof PhysicalUnion) {
            return (PhysicalUnion) plan;
        }
        for (Plan child : plan.children()) {
            PhysicalUnion union = findPhysicalUnion(child);
            if (union != null) {
                return union;
            }
        }
        return null;
    }

    @Test
    void testFilterHaving() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id2 = id")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg  group by id, id2 having id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
    }

    @Test
    void testGenerate() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg lateral view explode([1,2,3]) tmp1 as e1 where id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
    }

    @Test
    void testJoin() {
        // inner join
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg join uni "
                        + "where agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));

        // foj
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t2.id, t3.id from agg as t1 join uni as t2 "
                        + " on t1.id = t2.id  full outer join uni as t3 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isEqualAndNotNotNull(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(2)));

        // loj
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t2.id, t3.id from agg as t1 join uni as t2 "
                        + " on t1.id = t2.id  left outer join uni as t3 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isEqualAndNotNotNull(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(2)));

        // roj
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t2.id, t3.id from agg as t1 join uni as t2 "
                        + " on t1.id = t2.id  right outer join uni as t3 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isEqualAndNotNotNull(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(2)));
    }

    @Test
    void testOneRowRelation() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select 1, 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
    }

    @Test
    void testProject() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id as o1, id as o2, id2 as o4, 1 as c1, 1 as c2 from uni where id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(2)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(1), plan.getOutput().get(2)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(3), plan.getOutput().get(4)));
    }

    @Test
    void testSubQuery() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from (select id, id2 from agg where id = id2) t")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
    }

    @Test
    void testWindow() {
        // partition by uniform
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2, row_number() over(partition by id) from agg where id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isNullSafeEqual(plan.getOutput().get(0), plan.getOutput().get(1)));
    }

}
