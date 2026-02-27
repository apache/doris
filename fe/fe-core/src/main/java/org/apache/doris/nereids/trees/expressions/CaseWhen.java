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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * The internal representation of
 * CASE [expr] WHEN expr THEN expr [WHEN expr THEN expr ...] [ELSE expr] END
 * Each When/Then is stored as two consecutive children (whenExpr, thenExpr).
 * If a case expr is given, convert it to equalTo(caseExpr, whenExpr) and set it to whenExpr.
 * If an else expr is given then it is the last child.
 */
public class CaseWhen extends Expression implements NeedSessionVarGuard {

    private final List<WhenClause> whenClauses;
    private final Optional<Expression> defaultValue;
    private Supplier<List<DataType>> dataTypesForCoercion;

    public CaseWhen(List<WhenClause> whenClauses) {
        super((List) whenClauses);
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        defaultValue = Optional.empty();
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    /** CaseWhen */
    public CaseWhen(List<WhenClause> whenClauses, Expression defaultValue) {
        super(ImmutableList.<Expression>builderWithExpectedSize(whenClauses.size() + 1)
                .addAll(whenClauses)
                .add(defaultValue)
                .build());
        this.whenClauses = ImmutableList.copyOf(Objects.requireNonNull(whenClauses));
        this.defaultValue = Optional.of(Objects.requireNonNull(defaultValue));
        this.dataTypesForCoercion = computeDataTypesForCoercion();
    }

    public List<WhenClause> getWhenClauses() {
        return whenClauses;
    }

    public Optional<Expression> getDefaultValue() {
        return defaultValue;
    }

    /** dataTypesForCoercion */
    public List<DataType> dataTypesForCoercion() {
        return this.dataTypesForCoercion.get();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCaseWhen(this, context);
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }

    @Override
    public boolean nullable() {
        for (WhenClause whenClause : whenClauses) {
            if (whenClause.nullable()) {
                return true;
            }
        }
        return defaultValue.map(Expression::nullable).orElse(true);
    }

    @Override
    public String toString() {
        StringBuilder output = new StringBuilder("CASE");
        for (Expression child : children()) {
            if (child instanceof WhenClause) {
                output.append(child.toString());
            } else {
                output.append(" ELSE ").append(child.toString());
            }
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder("CASE");
        for (Expression child : children()) {
            if (child instanceof WhenClause) {
                sb.append(child.toDigest());
            } else {
                sb.append(" ELSE ").append(child.toDigest());
            }
        }
        sb.append(" END");
        return sb.toString();
    }

    @Override
    public String computeToSql() throws UnboundException {
        StringBuilder output = new StringBuilder("CASE");
        for (Expression child : children()) {
            if (child instanceof WhenClause) {
                output.append(child.toSql());
            } else {
                output.append(" ELSE ").append(child.toSql());
            }
        }
        output.append(" END");
        return output.toString();
    }

    @Override
    public CaseWhen withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty(), "case when should has at least 1 child");
        List<WhenClause> whenClauseList = new ArrayList<>();
        Expression defaultValue = null;
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i) instanceof WhenClause) {
                whenClauseList.add((WhenClause) children.get(i));
            } else if (children.size() - 1 == i) {
                defaultValue = children.get(i);
            } else {
                throw new AnalysisException("The children format needs to be [WhenClause+, DefaultValue?]");
            }
        }
        if (defaultValue == null) {
            return new CaseWhen(whenClauseList);
        }
        return new CaseWhen(whenClauseList, defaultValue);
    }

    private Supplier<List<DataType>> computeDataTypesForCoercion() {
        return Suppliers.memoize(() -> {
            Builder<DataType> dataTypes = ImmutableList.builderWithExpectedSize(
                    whenClauses.size() + (defaultValue.isPresent() ? 1 : 0));
            for (WhenClause whenClause : whenClauses) {
                dataTypes.add(whenClause.getDataType());
            }
            defaultValue.ifPresent(expression -> dataTypes.add(expression.getDataType()));
            return dataTypes.build();
        });
    }

    /**
     * Result class for extractSimpleCaseInfo() method.
     * Contains the case operand expression, the list of literal values, and the list of then expressions.
     */
    public static class SimpleCaseInfo {
        private final Expression caseOperand;
        private final List<Literal> literals;
        private final List<Expression> thenExprs;

        public SimpleCaseInfo(Expression caseOperand, List<Literal> literals, List<Expression> thenExprs) {
            this.caseOperand = caseOperand;
            this.literals = literals;
            this.thenExprs = thenExprs;
        }

        public Expression getCaseOperand() {
            return caseOperand;
        }

        public List<Literal> getLiterals() {
            return literals;
        }

        public List<Expression> getThenExprs() {
            return thenExprs;
        }
    }

    /**
     * Check if this CaseWhen is in "simple case" form and extract the necessary information.
     * Simple case form: CASE column WHEN 'O' THEN ... WHEN 'F' THEN ... END
     * which is represented as: CASE WHEN column = 'O' THEN ... WHEN column = 'F' THEN ... END
     *
     * Note: WhenClause's operand can be any expression, not just EqualTo. For example:
     * - EqualTo: CASE WHEN col = 'value' THEN ...  (from simple case syntax)
     * - IsNull: CASE WHEN col IS NULL THEN ...
     * - And/Or: CASE WHEN col > 1 AND col < 10 THEN ...
     * - etc.
     *
     * This method returns SimpleCaseInfo when ALL of the following conditions are met:
     * 1. All WhenClauses have operands in the form of EqualTo(sameExpr, literal)
     *    (the literal can be wrapped in Cast)
     * 2. All literals are non-NULL (NullLiteral is excluded)
     * 3. All literals are Integer or String type (IntegerLikeLiteral or StringLikeLiteral)
     * 4. All WHEN clauses compare the same expression
     *
     * When SimpleCaseInfo is returned, the caller can build children as:
     * [caseOperand, literal1, then1, literal2, then2, ..., else?]
     * This allows BE to optimize using techniques like hash lookup.
     *
     * This pattern is created by LogicalPlanBuilder.visitSimpleCase() when parsing
     * "CASE expr WHEN value THEN ..." syntax.
     *
     * @return Optional containing SimpleCaseInfo if this is a simple case form, or empty otherwise.
     */
    public Optional<SimpleCaseInfo> extractSimpleCaseInfo() {
        if (whenClauses.isEmpty()) {
            return Optional.empty();
        }

        Expression caseOperand = null;
        List<Literal> literals = new ArrayList<>();
        List<Expression> thenExprs = new ArrayList<>();

        for (WhenClause whenClause : whenClauses) {
            Expression operand = whenClause.getOperand();
            // WhenClause's operand can be any boolean expression (EqualTo, IsNull, And, Or, etc.)
            // We only handle the simple case pattern: EqualTo(column, literal)
            if (!(operand instanceof EqualTo)) {
                return Optional.empty();
            }

            EqualTo equalTo = (EqualTo) operand;
            Expression left = equalTo.left();
            Expression right = equalTo.right();

            // Unwrap Cast if present (type inference may add Cast around literals)
            Expression unwrappedLeft = unwrapCast(left);
            Expression unwrappedRight = unwrapCast(right);

            // Determine which side is the literal and which is the case expression
            Expression currentCaseOperand;
            Literal literal;
            if (unwrappedRight instanceof Literal && !(unwrappedLeft instanceof Literal)) {
                currentCaseOperand = left;  // Keep the original (possibly casted) expression
                literal = (Literal) unwrappedRight;
            } else if (unwrappedLeft instanceof Literal && !(unwrappedRight instanceof Literal)) {
                currentCaseOperand = right;  // Keep the original (possibly casted) expression
                literal = (Literal) unwrappedLeft;
            } else {
                // Both are literals or both are non-literals
                return Optional.empty();
            }

            // Exclude NULL literals - we can't optimize CASE column WHEN NULL THEN ...
            // because NULL comparison has special semantics (NULL = NULL is NULL, not TRUE)
            if (literal instanceof NullLiteral) {
                return Optional.empty();
            }

            // Only support Integer and String type literals for simple case optimization
            if (!(literal instanceof IntegerLikeLiteral) && !(literal instanceof StringLikeLiteral)) {
                return Optional.empty();
            }

            // Check that all WHEN clauses compare the same expression
            // Compare unwrapped expressions to handle cases where Cast is added
            Expression unwrappedCaseOperand = unwrapCast(currentCaseOperand);
            if (caseOperand == null) {
                caseOperand = currentCaseOperand;
            } else if (!unwrapCast(caseOperand).equals(unwrappedCaseOperand)) {
                // Different case expressions in different WHEN clauses
                return Optional.empty();
            }

            literals.add(literal);
            thenExprs.add(whenClause.getResult());
        }

        return Optional.of(new SimpleCaseInfo(caseOperand, literals, thenExprs));
    }

    /**
     * Unwrap Cast expression to get the underlying expression.
     * This is needed because type inference may add Cast around literals.
     */
    private Expression unwrapCast(Expression expr) {
        while (expr instanceof Cast) {
            expr = ((Cast) expr).child();
        }
        return expr;
    }
}
