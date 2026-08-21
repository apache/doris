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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

class FdTest extends TestWithFeService {
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
        createTable("create table test.nullable_uni (\n"
                + "id int,\n"
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
                        .isFuncDeps(ImmutableSet.of(plan.getOutput().get(1)), ImmutableSet.of(plan.getOutput().get(0))));
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
        // constant propagation will rewrite agg
        // Assertions.assertTrue(plan.getLogicalProperties().getTrait()
        //        .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
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

        // foj: both sides nullable — keep FDs with NOT NULL determinants
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 full outer join uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        // t1.id is NOT NULL, so {t1.id} -> {t1.id2} survives null extension
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        // t2.id is NOT NULL, so {t2.id} -> {t2.id2} survives null extension
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));

        // loj: left side preserved, right side nullable — only NOT NULL-determinant FDs from right propagate
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 left outer join uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        // t1.id is NOT NULL, left side always preserved
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        // t2.id is NOT NULL, so {t2.id} -> {t2.id2} survives null extension
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));

        // roj: right side preserved, left side nullable — only NOT NULL-determinant FDs from left propagate
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 right outer join uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        // t1.id is NOT NULL, so {t1.id} -> {t1.id2} survives null extension
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        // t2.id is NOT NULL, right side always preserved
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));

        // loj with nullable determinant: FD should be dropped
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 left outer join nullable_uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        // t1 side preserved
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        // t2.id is nullable, so {t2.id} -> {t2.id2} should be dropped
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));

        // foj with nullable determinant on one side
        plan = PlanChecker.from(connectContext)
                .analyze("select t1.id, t1.id2, t2.id, t2.id2 "
                        + "from uni as t1 full outer join nullable_uni as t2 on t1.id2 = t2.id2")
                .rewrite()
                .getPlan();
        // t1.id is NOT NULL, so {t1.id} -> {t1.id2} survives
        Assertions.assertTrue(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(0)), ImmutableSet.of(plan.getOutput().get(1))));
        // t2.id is nullable, so {t2.id} -> {t2.id2} should be dropped
        Assertions.assertFalse(plan.getLogicalProperties().getTrait()
                .isDependent(ImmutableSet.of(plan.getOutput().get(2)), ImmutableSet.of(plan.getOutput().get(3))));
    }

    @Test
    void testNestedOuterJoinNullableDeterminant() {
        // Reduced failing tree from review "Check determinant nullability against the current child output":
        //   Aggregate(group by r_id, c)
        //     RightOuterJoin
        //       Project(l_id, r_id, coalesce(r_id, 1) AS c)
        //         LeftOuterJoin
        //           Scan L
        //           Scan R(r_id NOT NULL UNIQUE)
        //       Scan V
        // r_id is NOT NULL in R but becomes nullable at the inner LOJ output; the Project derives
        // r_id -> c from the expression. At the outer join output this FD must be dropped:
        // unmatched V rows inject (r_id=NULL, c=NULL), which collides with the Project's own
        // (r_id=NULL, c=1). After rewrite the sub-query alias is inlined into a plain project
        // (LogicalSubQueryAliasToLogicalProject) whose trait keeps the stale non-nullable r_id,
        // so the outer join must still be checked against the immediate child's current output.
        // c is kept in the select list so that it is not pruned away before the trait check.
        // Disable join reorder to keep the join tree stable (v LEFT OUTER JOIN p as written).
        connectContext.getSessionVariable().setDisableJoinReorder(true);
        String sql = "select p.id, p.c, count(*) "
                + "from uni as v "
                + "left outer join ("
                + "select l.id2, r.id, coalesce(r.id, 1) as c "
                + "from agg as l left outer join uni as r on l.id2 = r.id2) p "
                + "on v.id2 = p.id2 "
                + "group by p.id, p.c";

        LogicalAggregate<?> aggregate = (LogicalAggregate<?>) findNode(
                PlanChecker.from(connectContext).analyze(sql).getPlan(), n -> n instanceof LogicalAggregate);
        Assertions.assertNotNull(aggregate);
        // group by (r_id, c); both are plain slots after subquery inlining
        Slot rId = (Slot) aggregate.getGroupByExpressions().get(0);
        Slot c = (Slot) aggregate.getGroupByExpressions().get(1);

        // logical path: the outer join's trait must not contain r_id -> c
        Plan rewritten = PlanChecker.from(connectContext).analyze(sql).rewrite().getPlan();
        LogicalJoin<?, ?> outerJoin = (LogicalJoin<?, ?>) findNode(rewritten, n -> n instanceof LogicalJoin);
        Assertions.assertNotNull(outerJoin, "rewritten plan: " + rewritten.treeString());
        Assertions.assertFalse(outerJoin.getLogicalProperties().getTrait()
                        .isDependent(ImmutableSet.of(rId), ImmutableSet.of(c)),
                "r_id -> c must be dropped at the outer join since r_id is nullable on the outer side");

        // physical path: PhysicalHashJoin must drop r_id -> c as well; pick the outer join
        // (its subtree contains the inner join). implement() applies the implementation rules
        // directly (no CBO), so no table statistics are required.
        PhysicalPlan physicalPlan = PlanChecker.from(connectContext)
                .analyze(sql).rewrite().implement().getPhysicalPlan();
        PhysicalHashJoin<?, ?> physicalOuterJoin = (PhysicalHashJoin<?, ?>) findNode(physicalPlan,
                n -> n instanceof PhysicalHashJoin
                        && n.anyMatch(p -> p instanceof PhysicalHashJoin && p != n));
        Assertions.assertNotNull(physicalOuterJoin, "physical plan: " + physicalPlan.treeString());
        Assertions.assertFalse(physicalOuterJoin.getLogicalProperties().getTrait()
                        .isDependent(ImmutableSet.of(rId), ImmutableSet.of(c)),
                "physical join must also drop r_id -> c since r_id is nullable on the outer side");
    }

    private Plan findNode(Plan plan, Predicate<Plan> predicate) {
        if (predicate.test(plan)) {
            return plan;
        }
        for (Plan child : plan.children()) {
            Plan found = findNode(child, predicate);
            if (found != null) {
                return found;
            }
        }
        return null;
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

    @Test
    void testScanOutputMissingConstraintColumns() throws Exception {
        // P1 from review: findSlotsByColumn() registers a PARTIAL unique key when the scan's
        // output does not contain every constrained column (e.g. a non-base index that only
        // covers (a, c) of a table-level UNIQUE(a, b)).
        // Output {a, c} ∩ constraint {a, b} = {a}: {a} must NOT be advertised as unique,
        // otherwise EliminateGroupByKey derives a -> c and wrongly wraps c for GROUP BY a, c.
        createTable("create table test.idx_t (\n"
                + "a int not null,\n"
                + "b int not null,\n"
                + "c int not null)\n"
                + "distributed by hash(a) buckets 3\n"
                + "properties('replication_num'='1')");
        addConstraint("alter table test.idx_t add constraint uk unique (a, b)");

        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
        OlapTable table = (OlapTable) db.getTableOrMetaException("idx_t", Table.TableType.OLAP);
        // Simulate a scan whose output only exposes (a, c) — the same shape a non-base index
        // (e.g. an MV index) would produce. cachedOutput overrides the scan's output slots.
        List<Slot> partialOutput = ImmutableList.of(
                SlotReference.fromColumn(StatementScopeIdGenerator.getExprIdGenerator().getNextId(),
                        table, table.getColumn("a"), "a", ImmutableList.of()),
                SlotReference.fromColumn(StatementScopeIdGenerator.getExprIdGenerator().getNextId(),
                        table, table.getColumn("c"), "c", ImmutableList.of()));
        LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), table,
                ImmutableList.of("test"), Optional.empty(), Optional.empty(),
                table.getPartitionIds(), false, ImmutableList.of(),
                table.getBaseIndexId(), false, PreAggStatus.unset(), ImmutableList.of(), ImmutableList.of(),
                Maps.newHashMap(), Optional.of(partialOutput), Optional.empty(), false, Maps.newHashMap(),
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of(),
                Optional.empty(), Optional.empty(), ImmutableList.of(), Optional.empty(), Optional.empty());

        List<Slot> output = scan.getOutput();
        Assertions.assertEquals(2, output.size(), "scan output: " + output);
        Slot a = output.get(0);
        Assertions.assertEquals("a", ((SlotReference) a).getName());
        // UNIQUE(a,b) requires BOTH columns; the scan output misses b, so {a} is not unique
        Assertions.assertFalse(scan.getLogicalProperties().getTrait().isUnique(a),
                "partial constraint registration: {a} must not be unique when the scan output misses column b");
    }

    @Test
    void testMorReadAsDupSuppressesUniqueConstraint() throws Exception {
        // P1 from review: for MOR unique-key tables read as DUP (read_mor_as_dup_tables),
        // the data exposes every version (e.g. (1,10),(1,20),(1,30)) so the unique key k is
        // NOT unique. LogicalOlapScan.computeUnique() must suppress the constraint imported
        // by super.computeUnique() before its own raw-version guard returns.
        createTable("create table test.mor_t (k int not null, v int not null) "
                + "unique key(k) distributed by hash(k) buckets 3 "
                + "properties('replication_num'='1', 'enable_unique_key_merge_on_write'='false')");
        addConstraint("alter table test.mor_t add constraint uk unique (k)");
        connectContext.getSessionVariable().readMorAsDupTables = "*";
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrMetaException("test");
            OlapTable table = (OlapTable) db.getTableOrMetaException("mor_t", Table.TableType.OLAP);
            LogicalOlapScan scan = new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), table);
            Slot k = scan.getOutput().get(0);
            Assertions.assertEquals("k", ((SlotReference) k).getName());
            Assertions.assertFalse(scan.getLogicalProperties().getTrait().isUnique(k),
                    "MOR table read as DUP exposes all versions: k must not be unique");
        } finally {
            connectContext.getSessionVariable().readMorAsDupTables = "";
        }
    }

}
