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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.FloatType;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * ScalarFunction 'score': the relevance of a row for the MATCH and SEARCH predicates in the
 * WHERE clause of its SELECT.
 *
 * <p>CheckScoreUsage gives every SELECT that calls score() an id and marks its score() calls and
 * those predicates with it. BindScoreToOlapScan then replaces the calls with the score column of
 * the scan that the predicates filter.
 */
public class Score extends ScalarFunction
        implements LeafExpression, ExplicitlyCastableSignature, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(FloatType.INSTANCE).args());

    private final Optional<ExprId> scoreId;

    /**
     * constructor with 0 argument.
     */
    public Score() {
        this(Optional.empty());
    }

    private Score(Optional<ExprId> scoreId) {
        super("score");
        this.scoreId = scoreId;
    }

    /** The SELECT whose MATCH and SEARCH predicates define this score(). */
    public Optional<ExprId> getScoreId() {
        return scoreId;
    }

    public Score withScoreId(ExprId scoreId) {
        return new Score(Optional.of(scoreId));
    }

    /** The score column that BindScoreToOlapScan added among the virtual columns of a scan. */
    public static Optional<Alias> findScoreColumn(List<NamedExpression> virtualColumns) {
        for (NamedExpression column : virtualColumns) {
            if (column instanceof Alias && ((Alias) column).child() instanceof Score) {
                return Optional.of((Alias) column);
            }
        }
        return Optional.empty();
    }

    /**
     * The score column of a scan and the virtual columns computed from it, e.g. common
     * sub-expressions. The scan fills them only after it evaluated its own predicates, so any
     * predicate on them but the score range of the scan has to be evaluated above the scan.
     */
    public static Set<Slot> findScoreSlots(List<NamedExpression> virtualColumns) {
        Set<Slot> scoreSlots = new HashSet<>();
        findScoreColumn(virtualColumns).ifPresent(column -> scoreSlots.add(column.toSlot()));
        boolean added = !scoreSlots.isEmpty();
        while (added) {
            added = false;
            for (NamedExpression column : virtualColumns) {
                if (!scoreSlots.contains(column.toSlot())
                        && column.getInputSlots().stream().anyMatch(scoreSlots::contains)) {
                    scoreSlots.add(column.toSlot());
                    added = true;
                }
            }
        }
        return scoreSlots;
    }

    @Override
    protected boolean extraEquals(Expression that) {
        return super.extraEquals(that) && scoreId.equals(((Score) that).scoreId);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), scoreId);
    }

    @Override
    public boolean foldable() {
        return false;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScore(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
