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

import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

class UniqueTest extends TestWithFeService {
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
                .analyze("select name from agg group by name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select sum(id) from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id, sum(id), avg(id), max(id), min(id), topn(id,2) from agg group by id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutput().get(1)));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutput().get(2)));
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutput().get(3)));

    }

    @Test
    void testScan() throws Exception {
        // test agg key
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));

        // test unique key
        plan = PlanChecker.from(connectContext)
                .analyze("select id from uni")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));

        // test unique constraint
        addConstraint("alter table agg add constraint uq unique(name)");
        plan = PlanChecker.from(connectContext)
                .analyze("select name from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        dropConstraint("alter table agg drop constraint uq");

        // test primary constraint
        addConstraint("alter table agg add constraint pk primary key(name)");
        plan = PlanChecker.from(connectContext)
                .analyze("select name from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        dropConstraint("alter table agg drop constraint pk");
    }

    @Test
    void testTopNLimit() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select name from agg limit 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select name from agg order by name limit 1 ")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testSetOp() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("(select name from agg limit 1) except select name from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id from agg intersect select id from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select id from agg union all select id from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isEmpty());
        plan = PlanChecker.from(connectContext)
                .analyze("select name, id from agg union select name, id from agg")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutputSet()));
    }

    @Test
    void testFilterHaving() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg where id = 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select name from uni group by name having name = \"\"")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testGenerate() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg lateral view explode([1,2,3]) tmp1 as e1")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
    }

    @Test
    void testJoin() {
        // foj doesn't propagate any
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg full outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties().getTrait().isUnique(plan.getOutputSet()));

        // loj propagate unique when right is unique
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg left outer join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg left outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(1)));

        // roj propagate unique when left is unique
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg right outer join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(1)));

        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg right outer join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(1)));
        Assertions.assertFalse(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));

        // semi/anti join propagate all
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left semi join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id from agg left anti join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id from agg right semi join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id from agg right anti join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties().getTrait().isUnique(plan.getOutput().get(0)));

        // inner join propagate unique only when join key is unique
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(1)));

        // Even though the hash key (uni.name) is not unique, the join output is still unique on
        // {agg.id, uni.id} because both inputs are unique on their respective ids
        // (union propagation: U_L ∪ U_R is unique).
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
        // Non-equal join condition: union propagation does not depend on hash equi conjuncts.
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id < uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
        // Disjunctive condition: still unique on {agg.id, uni.id} by union propagation.
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id = uni.id or agg.name > uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
        plan = PlanChecker.from(connectContext)
                .analyze("select uni.id, agg.id from agg inner join uni "
                        + "on agg.id = uni.id and agg.name > uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(1)));
    }

    @Test
    void testJoinUniqueUnionPropagation() {
        // INNER: union propagation { agg.id } ∪ { uni.id } -> output unique on {agg.id, uni.id}
        // even when hash key (uni.name) is not unique on its side
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg inner join uni on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));

        // CROSS: union propagation does not depend on equi conjuncts
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg cross join uni")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));

        // LEFT OUTER: union propagation also applies (NULL distinct in unique-set semantics)
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg left outer join uni on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));

        // RIGHT OUTER
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg right outer join uni on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));

        // FULL OUTER
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.id, uni.id from agg full outer join uni on agg.id = uni.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));

        // Equal-set canonicalization: agg.id = uni.id, output {agg.id} alone should be unique
        // because { agg.id, uni.id } gets canonicalized via the equal set so a single
        // representative slot in the output identifies the row.
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id2, t1.name,t2.name from (select distinct id2,name from agg) t1 inner join (select distinct id2,name from uni) t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
        plan = PlanChecker.from(connectContext)
                .analyze("select t2.id2, t1.name,t2.name,t3.name from (select distinct id2,name from agg) t1 inner join (select distinct id2,name from uni) t2 on t1.id2 = t2.id2 inner join (select distinct id2,name from uni) t3 on t2.id2=t3.id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
        plan = PlanChecker.from(connectContext)
                .analyze("select t3.id2,t2.id2, t1.name,t2.name from (select distinct id2,name from agg) t1 inner join (select distinct id2,name from uni) t2 on t1.id2 = t2.id2 inner join (select distinct id2,name from uni) t3 on t2.name=t3.name")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));

        // Negative: when output drops both join keys and lacks any unique combination
        // covering both sides, it must NOT be considered unique.
        plan = PlanChecker.from(connectContext)
                .analyze("select agg.name, uni.name from agg inner join uni on agg.id = uni.id")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties()
                .getTrait().isUnique(plan.getOutputSet()));
    }

    @Test
    void testOneRowRelation() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select 1")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testProject() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id as id2 from uni")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testRepeat() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg  group by GROUPING SETS ((id, name), (id))")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutputSet()));
        plan = PlanChecker.from(connectContext)
                .analyze("select id from agg group by rollup (id, name)")
                .rewrite()
                .getPlan();
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isUnique(plan.getOutputSet()));
    }

    @Test
    void testSubQuery() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from (select id from agg) t")
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testAssertRows() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id from agg where id = (select id from uni)")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
    }

    @Test
    void testWindow() {
        // partition by uniform
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, row_number() over(partition by name) from agg where name ='d'")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(1)));

        // partition by None
        plan = PlanChecker.from(connectContext)
                .analyze("select id, row_number() over() from agg where id =1")
                .rewrite()
                .getPlan();
        Assertions.assertEquals(Optional.of(new IntegerLiteral(1)),
                plan.getLogicalProperties().getTrait().getUniformValue(plan.getOutput().get(0)));
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(1)));
    }

    @Test
    void testEqual() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select id, id2 from agg where id = id2")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutput().get(1)));

        plan = PlanChecker.from(connectContext)
                .analyze("select t1.name, t2.id from "
                        + "(select id, name from agg group by id, name) t1 "
                        + "join (select id from uni) t2 "
                        + "on t1.id = t2.id")
                .rewrite()
                .getPlan();
        Assertions.assertTrue(plan.getLogicalProperties()
                .getTrait().isUniqueAndNotNull(plan.getOutputSet()));
    }
}
