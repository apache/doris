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

import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.algebra.Project;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class ConvertInnerJoinToSemiJoinTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTables(
                "CREATE TABLE IF NOT EXISTS t1 (\n"
                        + "    id1 int not null,\n"
                        + "    v1 int not null,\n"
                        + "    d1 datetime not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id1)\n"
                        + "DISTRIBUTED BY HASH(id1) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n",
                "CREATE TABLE IF NOT EXISTS t2 (\n"
                        + "    id2 int not null,\n"
                        + "    v2 int not null,\n"
                        + "    d2 datetime not null\n"
                        + ")\n"
                        + "DUPLICATE KEY(id2)\n"
                        + "DISTRIBUTED BY HASH(id2) BUCKETS 10\n"
                        + "PROPERTIES (\"replication_num\" = \"1\")\n");
    }

    // select distinct t1.id1 from t1 join t2 on t1.id1 = t2.id2
    // t2's columns are not used above the join, the join keys are equal conjuncts,
    // and the DISTINCT aggregate dedups the output:
    // inner join -> left semi join
    @Test
    void testConvertInnerJoinToSemiJoin() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .anyMatches(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }

    // The headline case of the rule: the DISTINCT aggregate consumes several left side
    // columns, so column pruning inserts an all-slot project between the aggregate and
    // the join. This exercises the second rule pattern Aggregate -> Project(all slots)
    // -> Join explicitly: the join below the all-slot project must become a left semi
    // join.
    @Test
    void testConvertWithProjectBetweenAggregateAndJoin() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1, t1.v1 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .matches(logicalAggregate(logicalProject(logicalJoin()
                        .when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                        .when(Project::isAllSlots)))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }

    // t2.id2 is projected above the join, so the right side columns leak:
    // keep inner join
    @Test
    void testNotConvertWhenRightSideColumnsUsed() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1, t2.id2 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN));
    }

    // no DISTINCT (or group-by) dedup guarantee above the join:
    // in bag semantics inner join row multiplication matters, keep inner join
    @Test
    void testNotConvertWithoutDistinct() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select t1.id1 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN));
    }

    // non-equi join condition goes into otherJoinConjuncts:
    // not a pure equi-join, keep inner join
    @Test
    void testNotConvertWithNonEquiCondition() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1 from t1 join t2 on t1.id1 > t2.id2")
                .rewrite()
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN));
    }

    // aggregation with aggregate functions must NOT be converted:
    // count(*) observes the input row multiplicity, changing it would be wrong
    @Test
    void testNotConvertWithAggregateFunction() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select t1.id1, count(*) from t1 join t2 on t1.id1 = t2.id2 group by t1.id1")
                .rewrite()
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN));
    }

    // ASOF join is never converted: its MATCH_CONDITION (t1.d1 >= t2.d2) is kept in
    // otherJoinConjuncts and the join type is ASOF_LEFT_INNER_JOIN, which the rule's
    // innerLogicalJoin() pattern never matches
    @Test
    void testNotConvertAsofJoin() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1 from t1 asof inner join t2 "
                        + "match_condition(t1.d1 >= t2.d2) on t1.id1 = t2.id2")
                .rewrite()
                .anyMatches(logicalJoin().when(j -> j.getJoinType() == JoinType.ASOF_LEFT_INNER_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.RIGHT_SEMI_JOIN));
    }

    // the group-by keys cover more than the output columns (all slots): the reused
    // Aggregate#isDistinct check still allows the conversion, because every group-by
    // key comes from the output side of the join (condition 1), so the row
    // multiplication of the inner join never creates new groups
    @Test
    void testConvertWhenGroupByCoversOutput() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select t1.id1 from t1 join t2 on t1.id1 = t2.id2 "
                        + "group by t1.id1, t1.v1")
                .rewrite()
                .anyMatches(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }

    // <=> (NullSafeEqual) is an EqualPredicate: FindHashConditionForJoin extracts it into
    // hashJoinConjuncts, so the inner join can also be converted to left semi join
    @Test
    void testConvertWithNullSafeEqual() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1 from t1 join t2 on t1.id1 <=> t2.id2")
                .rewrite()
                .anyMatches(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }

    // select distinct t2.id2 from t1 join t2 on t1.id1 = t2.id2
    // t1's columns are not used above the join, the join keys are equal conjuncts,
    // and the DISTINCT aggregate dedups the output, so the inner join acts as an
    // existence filter on t2: inner join -> right semi join
    @Test
    void testConvertInnerJoinToRightSemiJoin() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t2.id2 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .anyMatches(logicalJoin().when(j -> j.getJoinType() == JoinType.RIGHT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }

    // symmetric of testConvertWithProjectBetweenAggregateAndJoin: the DISTINCT aggregate
    // consumes several right side columns, column pruning inserts an all-slot project
    // between the aggregate and the join; the join below the all-slot project must
    // become a right semi join
    @Test
    void testConvertToRightSemiJoinWithProjectBetweenAggregateAndJoin() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t2.id2, t2.v2 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .matches(logicalAggregate(logicalProject(logicalJoin()
                        .when(j -> j.getJoinType() == JoinType.RIGHT_SEMI_JOIN))
                        .when(Project::isAllSlots)))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }

    // the left side columns leak above the join, so t1 is not an existence filter:
    // keep inner join, neither left nor right semi join
    @Test
    void testNotConvertToRightSemiJoinWhenLeftSideColumnsUsed() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t1.id1, t2.id2 from t1 join t2 on t1.id1 = t2.id2")
                .rewrite()
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.LEFT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.RIGHT_SEMI_JOIN));
    }

    // the right side variant also covers the null-safe equal case
    @Test
    void testConvertToRightSemiJoinWithNullSafeEqual() throws Exception {
        PlanChecker.from(connectContext)
                .analyze("select distinct t2.id2 from t1 join t2 on t1.id1 <=> t2.id2")
                .rewrite()
                .anyMatches(logicalJoin().when(j -> j.getJoinType() == JoinType.RIGHT_SEMI_JOIN))
                .nonMatch(logicalJoin().when(j -> j.getJoinType() == JoinType.INNER_JOIN));
    }
}
