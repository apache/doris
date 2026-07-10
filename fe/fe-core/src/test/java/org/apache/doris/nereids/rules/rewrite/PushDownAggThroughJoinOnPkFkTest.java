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

import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class PushDownAggThroughJoinOnPkFkTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");
        createTables(
                "CREATE TABLE IF NOT EXISTS pri (\n"
                        + "    id1 int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_not_null (\n"
                        + "    id2 int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS foreign_null (\n"
                        + "    id3 int,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(id3)\n"
                        + "DISTRIBUTED BY HASH(id3) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                // Tables for tree-structured multi-join test (like TPC-H Q10):
                // t_primary acts as the "bridge" table (like customer):
                //   - pk(pk_id)
                //   - fk(fk_to_other) references t_other_primary(other_pk_id)
                // t_foreign1 acts like orders:
                //   - fk(fk_to_primary) references t_primary(pk_id)
                // t_foreign2 acts like lineitem (no pk/fk constraints, just for tree structure)
                // t_other_primary acts like nation:
                //   - pk(other_pk_id)
                "CREATE TABLE IF NOT EXISTS t_primary (\n"
                        + "    pk_id int not null,\n"
                        + "    name char,\n"
                        + "    fk_to_other int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(pk_id)\n"
                        + "DISTRIBUTED BY HASH(pk_id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t_foreign1 (\n"
                        + "    fk1_id int not null,\n"
                        + "    name char,\n"
                        + "    fk_to_primary int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(fk1_id)\n"
                        + "DISTRIBUTED BY HASH(fk1_id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t_foreign2 (\n"
                        + "    fk2_id int not null,\n"
                        + "    name char,\n"
                        + "    fk_to_foreign1 int not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(fk2_id)\n"
                        + "DISTRIBUTED BY HASH(fk2_id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t_other_primary (\n"
                        + "    other_pk_id int not null,\n"
                        + "    name char\n"
                        + ")\n"
                        + "DUPLICATE KEY(other_pk_id)\n"
                        + "DISTRIBUTED BY HASH(other_pk_id) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n"
        );
        addConstraint("Alter table pri add constraint pk primary key (id1)");
        addConstraint("Alter table foreign_not_null add constraint f_not_null foreign key (id2)\n"
                + "references pri(id1)");
        addConstraint("Alter table foreign_null add constraint f_not_null foreign key (id3)\n"
                + "references pri(id1)");
        // Constraints for tree-structured multi-join test
        addConstraint("Alter table t_primary add constraint pk_t_primary primary key (pk_id)");
        addConstraint("Alter table t_foreign1 add constraint fk1_to_primary foreign key (fk_to_primary)\n"
                + "references t_primary(pk_id)");
        addConstraint("Alter table t_other_primary add constraint pk_other primary key (other_pk_id)");
        addConstraint("Alter table t_primary add constraint fk_primary_to_other foreign key (fk_to_other)\n"
                + "references t_other_primary(other_pk_id)");
        connectContext.getSessionVariable().setDisableNereidsRules(
                "PRUNE_EMPTY_PARTITION,ELIMINATE_JOIN_BY_FK");
    }

    @Test
    void testGroupByFk() {
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkAndOther() {
        String sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, foreign_not_null.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalAggregate()), any()))
                .printlnTree();
        sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
        sql = "select pri.id1 from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name, foreign_not_null.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalProject(logicalAggregate()), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkWithCount() {
        String sql = "select count(pri.id1) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
        sql = "select count(foreign_not_null.id2) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkWithForeigAgg() {
        String sql = "select sum(foreign_not_null.id2) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testGroupByFkWithPrimaryAgg() {
        String sql = "select sum(pri.id1) from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalAggregate(logicalProject(logicalJoin())))
                .printlnTree();
    }

    @Test
    void testMultiJoin() {
        String sql = "select count(pri.id1), pri.name from foreign_not_null inner join foreign_null on foreign_null.name = foreign_not_null.name\n"
                + " inner join pri on pri.id1 = foreign_not_null.id2\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();

        sql = "select count(pri.id1), pri.name from pri inner join foreign_not_null on pri.id1 = foreign_not_null.id2\n"
                + "inner join foreign_null on foreign_null.name = foreign_not_null.name\n"
                + "group by foreign_not_null.id2, pri.id1, pri.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testMissSlot() {
        String sql = "select count(pri.name) from pri inner join foreign_not_null on pri.name = foreign_not_null.name";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalAggregate(logicalProject(logicalJoin())))
                .printlnTree();
    }

    @Test
    void testPushDownAggThroughTreeJoin() {
        // Test pushing agg through a tree-structured join (like TPC-H Q10):
        // Join(t_primary.fk_to_other = t_other_primary.other_pk_id)  ← root (pk-fk: other is pk)
        //   /  \
        // Join(t_foreign1.fk1_id = t_foreign2.fk_to_foreign1)        t_other_primary
        //   /  \
        // Join(t_primary.pk_id = t_foreign1.fk_to_primary)           t_foreign2
        //   /  \
        // t_primary  t_foreign1
        // The pk-fk join with t_other_primary as pk is recognized,
        // and agg should be pushed down through it.
        String sql = "select t_primary.pk_id, t_primary.fk_to_other, sum(t_foreign2.fk2_id) "
                + "from t_primary "
                + "inner join t_foreign1 on t_primary.pk_id = t_foreign1.fk_to_primary "
                + "inner join t_foreign2 on t_foreign1.fk1_id = t_foreign2.fk_to_foreign1 "
                + "inner join t_other_primary on t_primary.fk_to_other = t_other_primary.other_pk_id "
                + "group by t_primary.pk_id, t_primary.fk_to_other";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testPushDownAggThroughTreeJoinOtherSide() {
        // When the pk-fk join's primary is t_primary (the connector table that
        // also joins to other tables), the remaining leaves can still form a
        // connected chain t_foreign1 ↔ t_foreign2 ↔ t_other_primary via joins
        // {1,2} and {2,3}, allowing constructPlan to assemble them into a
        // single plan. The agg should be pushed down through the pk-fk join.
        // Note: the join to t_other_primary uses t_foreign2.name = t_other_primary.name
        // (a non-PK-FK join) so the remaining leaves form a connected chain.
        String sql = "select t_primary.pk_id, t_foreign1.fk_to_primary, sum(t_foreign2.fk2_id) "
                + "from t_primary "
                + "inner join t_foreign1 on t_primary.pk_id = t_foreign1.fk_to_primary "
                + "inner join t_foreign2 on t_foreign1.fk1_id = t_foreign2.fk_to_foreign1 "
                + "inner join t_other_primary on t_foreign2.name = t_other_primary.name "
                + "group by t_primary.pk_id, t_foreign1.fk_to_primary";
        // In this case, pk-fk is t_primary(pk)↔t_foreign1(fk). t_primary is the
        // connector table but the remaining leaves {t_foreign1, t_foreign2, t_other_primary}
        // are connected (t_foreign1↔t_foreign2, t_foreign2↔t_other_primary).
        // constructPlan successfully assembles them, and the agg is pushed down
        // below the pk-fk join.
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }

    @Test
    void testSkipIneligibleEdgeAndSucceedOnLaterEdge() {
        // Regression: pushAgg iterates all PK/FK edges but used to return null
        // on the first ineligible candidate instead of continuing to later edges.
        // The lower edge {t_primary, t_foreign1} (t_primary is pk) is visited
        // first on Java 17 HashMap<BitSet> iteration, and sum(t_primary.pk_id)
        // makes eliminatePrimaryOutput return null because the Sum aggregate
        // function references a primary-side column that cannot be rewritten.
        // With the fix, the rule skips that edge and succeeds on the root edge
        // {t_other_primary, t_primary} (t_other_primary is pk), which pushes
        // the aggregate through because the query outputs no t_other_primary cols.
        String sql = "select t_primary.pk_id, t_primary.fk_to_other, sum(t_primary.pk_id) "
                + "from t_primary "
                + "inner join t_foreign1 on t_primary.pk_id = t_foreign1.fk_to_primary "
                + "inner join t_foreign2 on t_foreign1.fk1_id = t_foreign2.fk_to_foreign1 "
                + "inner join t_other_primary on t_primary.fk_to_other = t_other_primary.other_pk_id "
                + "group by t_primary.pk_id, t_primary.fk_to_other";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin(logicalAggregate(), any()))
                .printlnTree();
    }
}
