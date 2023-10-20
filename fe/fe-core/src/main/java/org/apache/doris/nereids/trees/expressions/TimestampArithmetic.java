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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullableOnDateLikeV2Args;
import org.apache.doris.nereids.trees.expressions.literal.Interval.TimeUnit;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 * Example: '1996-01-01' + INTERVAL '3' month;
 * TODO: we need to rethink this, and maybe need to add a new type of Interval then implement IntervalLiteral as others
 */
public class TimestampArithmetic extends Expression implements BinaryExpression, PropagateNullableOnDateLikeV2Args {

    private final String funcName;
    private final boolean intervalFirst;
    private final Operator op;
    private final TimeUnit timeUnit;

    public TimestampArithmetic(Operator op, Expression e1, Expression e2, TimeUnit timeUnit, boolean intervalFirst) {
        this(null, op, e1, e2, timeUnit, intervalFirst);
    }

    /**
     * Full parameter constructor.
     */
    public TimestampArithmetic(String funcName, Operator op, Expression e1, Expression e2, TimeUnit timeUnit,
            boolean intervalFirst) {
        super(ImmutableList.of(e1, e2));
        Preconditions.checkState(op == Operator.ADD || op == Operator.SUBTRACT);
        this.funcName = funcName;
        this.op = op;
        this.intervalFirst = intervalFirst;
        this.timeUnit = timeUnit;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimestampArithmetic(this, context);
    }

    @Override
    public TimestampArithmetic withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new TimestampArithmetic(this.funcName, this.op, children.get(0), children.get(1),
                this.timeUnit, this.intervalFirst);
    }

    public Expression withFuncName(String funcName) {
        return new TimestampArithmetic(funcName, this.op, children.get(0), children.get(1), this.timeUnit,
                this.intervalFirst);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        int dateChildIndex = 0;
        if (intervalFirst) {
            dateChildIndex = 1;
        }
        DataType childType = child(dateChildIndex).getDataType();
        if (childType instanceof DateTimeV2Type) {
            return childType;
        }
        if (childType instanceof DateV2Type) {
            if (timeUnit.isDateTimeUnit()) {
                return DateTimeV2Type.SYSTEM_DEFAULT;
            }
            return DateV2Type.INSTANCE;
        }
        if (childType instanceof DateTimeType || timeUnit.isDateTimeUnit()) {
            return DateTimeType.INSTANCE;
        } else {
            return DateType.INSTANCE;
        }
    }

    public String getFuncName() {
        return funcName;
    }

    public Operator getOp() {
        return op;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder strBuilder = new StringBuilder();
        if (funcName != null) {
            // Function-call like version.
            strBuilder.append(funcName).append("(");
            strBuilder.append(child(0).toSql()).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(child(1).toSql());
            strBuilder.append(" ").append(timeUnit);
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (intervalFirst) {
            // Non-function-call like version with interval as first operand.
            strBuilder.append("INTERVAL ");
            strBuilder.append(child(1).toSql()).append(" ");
            strBuilder.append(timeUnit);
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append(child(0).toSql());
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(child(0).toSql());
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(child(1).toSql()).append(" ");
            strBuilder.append(timeUnit);
        }
        return strBuilder.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TimestampArithmetic other = (TimestampArithmetic) o;
        return Objects.equals(funcName, other.funcName) && Objects.equals(timeUnit, other.timeUnit)
                && Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        children().forEach(c -> {
            if (c.getDataType().isObjectType()) {
                throw new AnalysisException("timestamp arithmetic could not contains object type: " + this.toSql());
            }
            if (c.getDataType().isComplexType()) {
                throw new AnalysisException("timestamp arithmetic could not contains complex type: " + this.toSql());
            }
        });
    }
}
