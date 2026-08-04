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
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class EliminateMarkJoinTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        connectContext.setDatabase("test");
        // tables are empty in the ut env, keep the scans from collapsing to empty relations
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");

        createTable("CREATE TABLE t1 (id int not null, score int null)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES(\"replication_num\"=\"1\");");
        createTable("CREATE TABLE t2 (id int not null)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES(\"replication_num\"=\"1\");");
        createTable("CREATE TABLE t3 (id int not null, score int null)\n"
                + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                + "PROPERTIES(\"replication_num\"=\"1\");");
    }

    @Test
    void inSubqueryInJoinOnCondition() {
        // the IN subquery sits in the join ON clause, so unnesting produces a mark join;
        // the mark slot ends up consumed by a bare filter conjunct and must be eliminated
        String sql = "select t1.id from t1 join t2 on t1.id = t2.id"
                + " and t1.id in (select id from t3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin().when(LogicalJoin::isMarkJoin))
                .matches(logicalJoin().when(join ->
                        join.getJoinType() == JoinType.LEFT_SEMI_JOIN && !join.isMarkJoin()));
    }

    @Test
    void inSubqueryInJoinOnConditionNullableColumn() {
        // nullable compare column: the mark conjuncts may stay separate from the hash
        // conjuncts, the rule must fold them into the plain semi join as well
        String sql = "select t1.id from t1 join t2 on t1.id = t2.id"
                + " and t1.score in (select score from t3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .nonMatch(logicalJoin().when(LogicalJoin::isMarkJoin))
                .matches(logicalJoin().when(join ->
                        join.getJoinType() == JoinType.LEFT_SEMI_JOIN && !join.isMarkJoin()));
    }

    @Test
    void markSlotProjectedToOutput() {
        // the mark slot is the query output, not a filter conjunct: must keep the mark join
        String sql = "select t1.id, t1.id in (select id from t3) as flag from t1";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin().when(LogicalJoin::isMarkJoin));
    }

    @Test
    void markSlotInNullDistinguishingPredicate() {
        // the consumer keeps rows whose mark is NULL, so three-valued mark semantics is
        // observable and the mark join must stay as it is
        String sql = "select t1.id from t1"
                + " where (t1.score in (select score from t3) and t1.id > 0) is null";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin().when(LogicalJoin::isMarkJoin));

        String bareIsNull = "select t1.id from t1"
                + " where (t1.score in (select score from t3)) is null";

        PlanChecker.from(connectContext)
                .analyze(bareIsNull)
                .rewrite()
                .matches(logicalJoin().when(LogicalJoin::isMarkJoin));
    }

    @Test
    void markSlotInDisjunction() {
        // FALSE and NULL marks are distinguishable inside OR: must keep the mark join
        String sql = "select t1.id from t1 where t1.id = 1"
                + " or t1.score in (select score from t3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin().when(LogicalJoin::isMarkJoin));
    }

    @Test
    void notInSubqueryInJoinOnCondition() {
        // NOT IN unnests to a null-aware anti join; the bare mark slot conjunct is
        // equivalent to the mark being true, so the mark slot is eliminated and the
        // result is a plain (non-mark) null-aware anti join
        String sql = "select t1.id from t1 join t2 on t1.id = t2.id"
                + " and t1.score not in (select score from t3)";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(logicalJoin().when(join -> !join.isMarkJoin()
                        && join.getJoinType() == JoinType.NULL_AWARE_LEFT_ANTI_JOIN));
    }
}