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

import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

class RewriteSetOperationToJoinWhenSpillTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");

        createTable("CREATE TABLE set_operation_spill_rewrite_t ("
                + "k1 int null,"
                + "k2 varchar(20) null"
                + ")\n"
                + "DISTRIBUTED BY HASH(k1)\n"
                + "BUCKETS 1\n"
                + "PROPERTIES(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ");");
        createTable("CREATE TABLE set_operation_spill_rewrite_complex_t ("
                + "k1 int null,"
                + "j jsonb null,"
                + "a array<int> null,"
                + "m map<string, int> null,"
                + "s struct<a:int> null"
                + ")\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1)\n"
                + "BUCKETS 1\n"
                + "PROPERTIES(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ");");
    }

    @Test
    void testRewriteIntersectWhenSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = true;
        String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                + "intersect select k1, k2 from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_SEMI_JOIN)));
    }

    @Test
    void testRewriteMixedPrimitiveOutputTypeAfterSetCoercionWhenSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = true;
        String sql = "select cast(k1 as smallint) from set_operation_spill_rewrite_t "
                + "intersect select cast(k1 as bigint) from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_SEMI_JOIN, 1)));
    }

    @Test
    void testRewriteIntersectWhenForceSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = false;
        connectContext.getSessionVariable().enableForceSpill = true;
        try {
            String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                    + "intersect select k1, k2 from set_operation_spill_rewrite_t";

            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                    .matches(logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_SEMI_JOIN)));
        } finally {
            connectContext.getSessionVariable().enableForceSpill = false;
        }
    }

    @Test
    void testRewriteExceptWhenSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = true;
        String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                + "except select k1, k2 from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_ANTI_JOIN)));
    }

    @Test
    void testRewriteThreeClauseIntersectWhenSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = true;
        String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                + "intersect select k1, k2 from set_operation_spill_rewrite_t "
                + "intersect select k1, k2 from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new MergeSetOperations())
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalJoin(
                        logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_SEMI_JOIN)),
                        any()).when(join -> isSpillHashJoin(join, JoinType.LEFT_SEMI_JOIN)));
    }

    @Test
    void testRewriteThreeClauseExceptWhenSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = true;
        String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                + "except select k1, k2 from set_operation_spill_rewrite_t "
                + "except select k1, k2 from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new MergeSetOperationsExcept())
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalJoin(
                        logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_ANTI_JOIN)),
                        any()).when(join -> isSpillHashJoin(join, JoinType.LEFT_ANTI_JOIN)));
    }

    @Test
    void testRewriteThreeClauseMinusWhenSpillEnabled() {
        connectContext.getSessionVariable().enableSpill = true;
        String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                + "minus select k1, k2 from set_operation_spill_rewrite_t "
                + "minus select k1, k2 from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new MergeSetOperationsExcept())
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalJoin(
                        logicalJoin().when(join -> isSpillHashJoin(join, JoinType.LEFT_ANTI_JOIN)),
                        any()).when(join -> isSpillHashJoin(join, JoinType.LEFT_ANTI_JOIN)));
    }

    @Test
    void testKeepSetOperatorWhenSpillDisabled() {
        connectContext.getSessionVariable().enableSpill = false;
        String sql = "select k1, k2 from set_operation_spill_rewrite_t "
                + "intersect select k1, k2 from set_operation_spill_rewrite_t";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalIntersect());
    }

    @Test
    void testKeepSetOperatorWhenOutputTypeCannotBeBothAggAndJoinKey() {
        assertKeepSetOperatorWhenSpillEnabled("select j from set_operation_spill_rewrite_complex_t "
                + "intersect select j from set_operation_spill_rewrite_complex_t");
        assertKeepSetOperatorWhenSpillEnabled("select a from set_operation_spill_rewrite_complex_t "
                + "intersect select a from set_operation_spill_rewrite_complex_t");
        assertKeepSetOperatorWhenSpillEnabled("select m from set_operation_spill_rewrite_complex_t "
                + "intersect select m from set_operation_spill_rewrite_complex_t");
        assertKeepSetOperatorWhenSpillEnabled("select s from set_operation_spill_rewrite_complex_t "
                + "intersect select s from set_operation_spill_rewrite_complex_t");
        assertKeepExceptWhenSpillEnabled("select j from set_operation_spill_rewrite_complex_t "
                + "except select j from set_operation_spill_rewrite_complex_t");
    }

    @Test
    void testKeepSetOperatorForUnsupportedOutputTypeAfterSetCoercion() {
        assertKeepSetOperatorWhenSpillEnabled("select cast(a as array<int>) from set_operation_spill_rewrite_complex_t "
                + "intersect select cast(a as array<bigint>) from set_operation_spill_rewrite_complex_t");
    }

    private void assertKeepSetOperatorWhenSpillEnabled(String sql) {
        connectContext.getSessionVariable().enableSpill = true;
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalIntersect());
    }

    private void assertKeepExceptWhenSpillEnabled(String sql) {
        connectContext.getSessionVariable().enableSpill = true;
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new RewriteSetOperationToJoinWhenSpill())
                .matches(logicalExcept());
    }

    private boolean isSpillHashJoin(LogicalJoin<?, ?> join, JoinType joinType) {
        return isSpillHashJoin(join, joinType, 2);
    }

    private boolean isSpillHashJoin(LogicalJoin<?, ?> join, JoinType joinType, int hashJoinConjunctSize) {
        return join.getJoinType() == joinType
                && join.getHashJoinConjuncts().size() == hashJoinConjunctSize
                && join.getHashJoinConjuncts().stream().allMatch(NullSafeEqual.class::isInstance)
                && join.getDistributeHint().distributeType == DistributeType.SHUFFLE_RIGHT;
    }
}
