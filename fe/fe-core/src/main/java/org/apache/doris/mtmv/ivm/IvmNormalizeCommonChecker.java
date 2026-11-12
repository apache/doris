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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;

import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * The query shapes an IVM MV cannot maintain, checked once for every normalize.
 *
 * <p>Each shape here has to be rejected while a query is normalized, and the reason is the same
 * whichever model runs the normalize: a DISTINCT project folds rows together, a UNION DISTINCT or a
 * UNION ALL with constant arms reaches rows the MV does not hold as the query's own, and a column name
 * Doris reserves for a hidden one would leave the MV holding fewer columns than the query produced.
 * Keeping each such gate here rather than in a normalizer is what stops the two normalize paths -- the
 * unique-key one and the append-only detail one, plus the delta rewriter that accepts the same shapes the
 * detail normalize does -- from drifting apart on what they accept.
 *
 * <p>The join gate is the one gate that carries a per-model policy: how many join types a model can
 * maintain differs. An append-only detail MV maintains only {@link #DETAIL_JOIN_TYPES}, a unique-key MV
 * also maintains the outer joins ({@link #UNIQ_JOIN_TYPES}). The accepted set is therefore the caller's,
 * but the gate as a whole is shared, because a mark join's mark is orthogonal to its type: the apply
 * rewrites put one on a LEFT_SEMI, a LEFT_ANTI, a NULL_AWARE_LEFT_ANTI, or even a CROSS join, so a test
 * of the join type alone does not reject it. A mark join reaches rows the MV does not hold as the query's
 * own, exactly as an outer join does, so it must be rejected by name before the type is looked at.
 *
 * <p>The class holds no state: every gate is a pure check that throws when it rejects.
 */
final class IvmNormalizeCommonChecker {

    /** Join types an append-only detail (DUP) MV can maintain; see {@link IvmNormalizeDupKeysMTMV}. */
    static final Set<JoinType> DETAIL_JOIN_TYPES =
            ImmutableSet.of(JoinType.INNER_JOIN, JoinType.CROSS_JOIN);

    /** Join types a unique-key MV can maintain; see {@link IvmNormalizeUniqKeysMTMV}. */
    static final Set<JoinType> UNIQ_JOIN_TYPES =
            ImmutableSet.of(JoinType.INNER_JOIN, JoinType.CROSS_JOIN,
                    JoinType.LEFT_OUTER_JOIN, JoinType.RIGHT_OUTER_JOIN, JoinType.FULL_OUTER_JOIN);

    private IvmNormalizeCommonChecker() {
    }

    /**
     * The rejection every normalize's {@code visit} default throws: a node type no model accepts lands
     * there, and the caller throws what this returns.
     */
    static IvmException unsupportedNode(Plan plan) {
        return new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                "IVM does not support plan node: " + plan.getClass().getSimpleName());
    }

    static void checkProject(LogicalProject<? extends Plan> project) {
        if (project.isDistinct()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support DISTINCT project.");
        }
    }

    static void checkUnion(LogicalUnion union) {
        if (union.getQualifier() != Qualifier.ALL) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support UNION DISTINCT. Only UNION ALL is supported.");
        }
        if (!union.getConstantExprsList().isEmpty()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support UNION ALL with constant expressions.");
        }
    }

    /**
     * Rejects an MV output column whose name a Doris hidden column already owns. Both models lay hidden
     * columns on the MV they build -- the unique-key model its row-id, any model that enables changelog
     * its own commit tso and row lsn -- so a user column taking a reserved prefix would be filtered out of
     * the schema the user reads, leaving an MV that holds fewer columns than its query produced.
     */
    static void checkResultSinkOutputNames(LogicalResultSink<? extends Plan> sink) {
        for (NamedExpression output : sink.getOutputExprs()) {
            try {
                FeNameFormat.checkColumnName(output.getName());
            } catch (AnalysisException e) {
                throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, e.getMessage(), e);
            }
        }
    }

    /**
     * Rejects a join no model with {@code allowedJoinTypes} can maintain. A mark join is rejected before
     * its type is looked at: it passes a type test whenever its type happens to be an accepted one (a
     * CROSS join carrying an EXISTS, say), which is exactly the hole a type-only check leaves.
     */
    static void checkJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Set<JoinType> allowedJoinTypes) {
        if (join.isMarkJoin()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support a mark join (a subquery with disjunction): the mark can drop "
                            + "a row the MV already holds.");
        }
        if (!allowedJoinTypes.contains(join.getJoinType())) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "IVM does not support join type " + join.getJoinType() + ". Only "
                            + allowedJoinTypes.stream().map(JoinType::toString).collect(Collectors.joining(", "))
                            + " are supported.");
        }
    }
}
