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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.DistributeType;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Optional;

class EliminateNullAwareLeftAntiJoinTest extends TestWithFeService implements MemoPatternMatchSupported {
    private static final String DATABASE = "uniform_non_null_anti_join";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DATABASE);
        createTable("create table " + DATABASE + ".probe_nullable (k int null, k2 int null) "
                + "duplicate key(k) distributed by hash(k) buckets 3 properties('replication_num'='1')");
        createTable("create table " + DATABASE + ".build_nonnull (k int not null, k2 int not null, v int) "
                + "duplicate key(k) distributed by hash(k) buckets 3 properties('replication_num'='1')");
        createTable("create table " + DATABASE + ".build_nullable (k int null, k2 int null, v int) "
                + "duplicate key(k) distributed by hash(k) buckets 3 properties('replication_num'='1')");
        connectContext.setDatabase(DATABASE);
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
    }

    @Test
    void nonNullConstantProbeEliminatesNullAwareness() throws Exception {
        Plan left = filterKey(scan("probe_nullable"), 0, 7);
        Plan right = scan("build_nonnull");
        Assertions.assertTrue(left.getOutput().get(0).nullable());
        Assertions.assertTrue(left.getLogicalProperties().getTrait().isUniformAndNotNull(left.getOutput().get(0)));
        LogicalJoin<?, ?> join = antiJoin(left, right, ImmutableList.of(
                new EqualTo(left.getOutput().get(0), right.getOutput().get(0))));

        LogicalJoin<?, ?> rewritten = rewrite(join);
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, rewritten.getJoinType());
        Assertions.assertEquals(join.getOutput(), rewritten.getOutput());
    }

    @Test
    void nullableBuildNeedsItsOwnNonNullProof() throws Exception {
        Plan left = filterKey(scan("probe_nullable"), 0, 7);
        LogicalOlapScan right = scan("build_nullable");
        List<Expression> hash = ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0)));
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(left, right, hash)).getJoinType());

        Plan nullBuild = new LogicalFilter<>(ImmutableSet.of(new IsNull(right.getOutput().get(0))), right);
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN,
                rewrite(antiJoin(left, nullBuild, hash)).getJoinType());

        Plan nonNullBuild = filterKey(right, 0, 2);
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, rewrite(antiJoin(left, nonNullBuild, hash)).getJoinType());
    }

    @Test
    void nullableProbeIsNotProvedByAnUnrelatedConstant() throws Exception {
        Plan left = filterKey(scan("probe_nullable"), 1, 7);
        Plan right = scan("build_nonnull");
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(left, right,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))))).getJoinType());
    }

    @Test
    void emptyBuildDoesNotProveTheNullableProbeNonNull() throws Exception {
        Plan left = scan("probe_nullable");
        Plan right = new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(),
                ImmutableList.copyOf(scan("build_nonnull").getOutput()));
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(left, right,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))))).getJoinType());
    }

    @Test
    void everyCompositeHashKeyNeedsANonNullProof() throws Exception {
        Plan left = filterKey(scan("probe_nullable"), 0, 7);
        Plan right = scan("build_nonnull");
        List<Expression> hash = ImmutableList.of(
                new EqualTo(left.getOutput().get(0), right.getOutput().get(0)),
                new EqualTo(left.getOutput().get(1), right.getOutput().get(1)));
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(left, right, hash)).getJoinType());

        Plan bothKeysNonNull = filterKey(left, 1, 8);
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, rewrite(antiJoin(bothKeysNonNull, right, hash)).getJoinType());
    }

    @Test
    void aliasPreservesTheUniformProofWithCommutedHashKeys() throws Exception {
        Plan filtered = filterKey(scan("probe_nullable"), 0, 7);
        Plan left = new LogicalProject<>(ImmutableList.of(new Alias(filtered.getOutput().get(0), "alias_key")),
                filtered);
        Plan right = scan("build_nonnull");
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, rewrite(antiJoin(left, right,
                ImmutableList.of(new EqualTo(right.getOutput().get(0), left.getOutput().get(0))))).getJoinType());
    }

    @ParameterizedTest
    @EnumSource(value = JoinType.class, names = {"LEFT_OUTER_JOIN", "RIGHT_OUTER_JOIN", "FULL_OUTER_JOIN"})
    void outerJoinNullExtensionInvalidatesTheSourceProof(JoinType joinType) throws Exception {
        Plan first = filterKey(scan("build_nonnull"), 0, 7);
        Plan second = filterKey(scan("build_nonnull"), 0, 7);
        Plan outer = new LogicalJoin<>(joinType,
                ImmutableList.of(new EqualTo(first.getOutput().get(0), second.getOutput().get(0))),
                first, second, null);
        Slot extended = outer.getOutput().get(joinType.isRightOuterJoin() ? 0 : first.getOutput().size());
        Assertions.assertTrue(extended.nullable());
        Assertions.assertFalse(outer.getLogicalProperties().getTrait().isUniformAndNotNull(extended));
        Plan other = filterKey(scan("probe_nullable"), 0, 7);
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(outer, other,
                ImmutableList.of(new EqualTo(extended, other.getOutput().get(0))))).getJoinType());
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(other, outer,
                ImmutableList.of(new EqualTo(other.getOutput().get(0), extended)))).getJoinType());
    }

    @Test
    void outerJoinPreservedSideKeepsTheUniformProof() throws Exception {
        Plan preserved = filterKey(scan("probe_nullable"), 0, 7);
        Plan nullableSide = scan("build_nonnull");
        Plan left = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN,
                ImmutableList.of(new EqualTo(preserved.getOutput().get(0), nullableSide.getOutput().get(0))),
                preserved, nullableSide, null);
        Plan right = scan("build_nonnull");
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, rewrite(antiJoin(left, right,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))))).getJoinType());
    }

    @Test
    void nullableComplexHashAndExtraConditionsKeepTheOriginalChecks() throws Exception {
        Plan left = filterKey(scan("probe_nullable"), 0, 7);
        Plan right = scan("build_nonnull");
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(antiJoin(left, right,
                ImmutableList.of(new EqualTo(new Add(left.getOutput().get(0), new IntegerLiteral(1)),
                        right.getOutput().get(0))))).getJoinType());

        LogicalJoin<?, ?> extra = new LogicalJoin<>(JoinType.NULL_AWARE_LEFT_ANTI_JOIN,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))),
                ImmutableList.of(new GreaterThan(left.getOutput().get(1), new IntegerLiteral(0))), left, right, null);
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(extra).getJoinType());
    }

    @Test
    void markJoinDoesNotUseTheUniformProof() throws Exception {
        Plan left = filterKey(scan("probe_nullable"), 0, 7);
        Plan right = scan("build_nonnull");
        LogicalJoin<?, ?> mark = new LogicalJoin<>(JoinType.NULL_AWARE_LEFT_ANTI_JOIN,
                ImmutableList.of(new EqualTo(left.getOutput().get(0), right.getOutput().get(0))), ImmutableList.of(),
                new DistributeHint(DistributeType.NONE), Optional.of(new MarkJoinSlotReference("mark")),
                left, right, null);
        Assertions.assertEquals(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, rewrite(mark).getJoinType());
    }

    @Test
    void schemaNonNullExpressionsStillEliminate() throws Exception {
        Plan left = scan("build_nonnull");
        Plan right = scan("build_nonnull");
        Assertions.assertEquals(JoinType.LEFT_ANTI_JOIN, rewrite(antiJoin(left, right,
                ImmutableList.of(new EqualTo(new Add(left.getOutput().get(0), new IntegerLiteral(1)),
                        right.getOutput().get(0))))).getJoinType());
    }

    @Test
    void eliminationEnablesConstantInferenceThroughTheGroupedBuild() {
        String sql = "select p.k from probe_nullable p where p.k = 7 and p.k not in "
                + "(select b.k from build_nonnull b group by b.k having max_by(b.v, b.k2) = 1)";
        PlanChecker.from(connectContext).analyze(sql).rewrite()
                .nonMatch(logicalJoin().when(join -> join.getJoinType() == JoinType.NULL_AWARE_LEFT_ANTI_JOIN))
                .matches(logicalFilter(logicalOlapScan()
                        .when(scan -> scan.getTable().getName().equals("build_nonnull")))
                        .when(filter -> filter.getConjuncts().stream().anyMatch(this::isKeyEqualToSeven)));
    }

    @Test
    void sqlRewriteRequiresBothGroupedJoinKeysToBeNonNull() {
        PlanChecker.from(connectContext).analyze("select k from probe_nullable where k = 7 and k not in "
                        + "(select k from build_nonnull group by k having count(*) > 1)")
                .rewrite()
                .matches(logicalJoin().when(join -> join.getJoinType() == JoinType.LEFT_ANTI_JOIN));
        PlanChecker.from(connectContext).analyze("select k from probe_nullable where k = 7 and k not in "
                        + "(select k from build_nullable group by k having count(*) > 1)")
                .rewrite()
                .matches(logicalJoin().when(join -> join.getJoinType() == JoinType.NULL_AWARE_LEFT_ANTI_JOIN));
    }

    private boolean isKeyEqualToSeven(Expression expression) {
        if (!(expression instanceof EqualTo)) {
            return false;
        }
        for (int i = 0; i < 2; i++) {
            Expression key = expression.child(i);
            Expression value = expression.child(1 - i);
            if (key instanceof Slot && ((Slot) key).getName().equals("k") && value instanceof Literal
                    && ((Literal) value).getStringValue().equals("7")) {
                return true;
            }
        }
        return false;
    }

    private LogicalOlapScan scan(String name) throws Exception {
        OlapTable table = (OlapTable) Env.getCurrentInternalCatalog().getDbOrMetaException(DATABASE)
                .getTableOrMetaException(name);
        return new LogicalOlapScan(StatementScopeIdGenerator.newRelationId(), table, ImmutableList.of(DATABASE));
    }

    private Plan filterKey(Plan child, int keyIndex, int value) {
        return new LogicalFilter<>(ImmutableSet.of(new EqualTo(child.getOutput().get(keyIndex),
                new IntegerLiteral(value))), child);
    }

    private LogicalJoin<Plan, Plan> antiJoin(Plan left, Plan right, List<Expression> hashConjuncts) {
        return new LogicalJoin<>(JoinType.NULL_AWARE_LEFT_ANTI_JOIN, hashConjuncts, left, right, null);
    }

    private LogicalJoin<?, ?> rewrite(LogicalJoin<?, ?> join) {
        return (LogicalJoin<?, ?>) PlanChecker.from(connectContext, join)
                .applyBottomUp(new EliminateNullAwareLeftAntiJoin()).getPlan();
    }
}
