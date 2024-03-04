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

import org.apache.doris.nereids.analyzer.Unbound;
import org.apache.doris.nereids.analyzer.UnboundVariable;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Nondeterministic;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.trees.expressions.typecoercion.TypeCheckResult;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.FractionalType;
import org.apache.doris.nereids.types.coercion.IntegralType;
import org.apache.doris.nereids.types.coercion.NumericType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract class for all Expression in Nereids.
 */
public abstract class Expression extends AbstractTreeNode<Expression> implements ExpressionTrait {

    protected Expression(Expression... children) {
        super(children);
    }

    protected Expression(List<Expression> children) {
        super(Optional.empty(), children);
    }

    public Alias alias(String alias) {
        return new Alias(this, alias);
    }

    /**
     * check input data types
     */
    public TypeCheckResult checkInputDataTypes() {
        if (this instanceof ExpectsInputTypes) {
            ExpectsInputTypes expectsInputTypes = (ExpectsInputTypes) this;
            return checkInputDataTypes(children, expectsInputTypes.expectedInputTypes());
        } else {
            List<String> errorMessages = Lists.newArrayList();
            // check all of its children recursively.
            for (int i = 0; i < this.children.size(); ++i) {
                Expression expression = this.children.get(i);
                TypeCheckResult childResult = expression.checkInputDataTypes();
                if (childResult != TypeCheckResult.SUCCESS) {
                    errorMessages.add(String.format("argument %d type check fail: %s",
                            i + 1, childResult.getMessage()));
                }
            }
            if (errorMessages.isEmpty()) {
                return TypeCheckResult.SUCCESS;
            } else {
                return new TypeCheckResult(false, StringUtils.join(errorMessages, ", "));
            }
        }
    }

    private TypeCheckResult checkInputDataTypes(List<Expression> inputs, List<AbstractDataType> inputTypes) {
        Preconditions.checkArgument(inputs.size() == inputTypes.size());
        List<String> errorMessages = Lists.newArrayList();
        for (int i = 0; i < inputs.size(); i++) {
            Expression input = inputs.get(i);
            AbstractDataType inputType = inputTypes.get(i);
            boolean legacyCastCompatible = inputType instanceof DataType
                    && !(inputType.getClass().equals(NumericType.class))
                    && !(inputType.getClass().equals(IntegralType.class))
                    && !(inputType.getClass().equals(FractionalType.class))
                    && !(inputType.getClass().equals(CharacterType.class))
                    && input.getDataType().toCatalogDataType().matchesType(inputType.toCatalogDataType());
            if (!legacyCastCompatible && !inputType.acceptsType(input.getDataType())) {
                errorMessages.add(String.format("argument %d requires %s type, however '%s' is of %s type",
                        i + 1, inputType.simpleString(), input.toSql(), input.getDataType().simpleString()));
            }
        }
        if (!errorMessages.isEmpty()) {
            return new TypeCheckResult(false, StringUtils.join(errorMessages, ", "));
        }
        return TypeCheckResult.SUCCESS;
    }

    public abstract <R, C> R accept(ExpressionVisitor<R, C> visitor, C context);

    @Override
    public List<Expression> children() {
        return children;
    }

    @Override
    public Expression child(int index) {
        return children.get(index);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        throw new RuntimeException();
    }

    public final Expression withChildren(Expression... children) {
        return withChildren(ImmutableList.copyOf(children));
    }

    /**
     * Whether the expression is a constant.
     */
    public boolean isConstant() {
        if (this instanceof AssertNumRowsElement
                || this instanceof AggregateFunction
                || this instanceof OrderExpression
                || this instanceof Properties
                || this instanceof SubqueryExpr
                || this instanceof UnboundVariable
                || this instanceof Variable
                || this instanceof VariableDesc
                || this instanceof WindowExpression
                || this instanceof WindowFrame) {
            // agg_fun(literal) is not constant, the result depends on the group by keys
            return false;
        }
        if (this instanceof LeafExpression) {
            return this instanceof Literal;
        } else {
            return !(this instanceof Nondeterministic) && children().stream().allMatch(Expression::isConstant);
        }
    }

    public final Expression castTo(DataType targetType) throws AnalysisException {
        return uncheckedCastTo(targetType);
    }

    public Expression checkedCastTo(DataType targetType) throws AnalysisException {
        return castTo(targetType);
    }

    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        throw new RuntimeException("Do not implement uncheckedCastTo");
    }

    /**
     * Get all the input slots of the expression.
     * <p>
     * Note that the input slots of subquery's inner plan is not included.
     */
    public final Set<Slot> getInputSlots() {
        return collect(Slot.class::isInstance);
    }

    public final Set<ExprId> getInputSlotExprIds() {
        return getInputSlots().stream().map(NamedExpression::getExprId).collect(Collectors.toSet());
    }

    public boolean isLiteral() {
        return this instanceof Literal;
    }

    public boolean isNullLiteral() {
        return this instanceof NullLiteral;
    }

    public boolean isSlot() {
        return this instanceof Slot;
    }

    public boolean isColumnFromTable() {
        return (this instanceof SlotReference) && ((SlotReference) this).getColumn().isPresent();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Expression that = (Expression) o;
        return Objects.equals(children(), that.children());
    }

    @Override
    public int hashCode() {
        return 0;
    }

    /**
     * This expression has unbound symbols or not.
     */
    public boolean hasUnbound() {
        if (this instanceof Unbound) {
            return true;
        }
        for (Expression child : children) {
            if (child.hasUnbound()) {
                return true;
            }
        }
        return false;
    }

    public String shapeInfo() {
        return toSql();
    }
}
