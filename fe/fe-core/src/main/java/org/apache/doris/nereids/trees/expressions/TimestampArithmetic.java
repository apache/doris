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
import org.apache.doris.nereids.trees.expressions.IntervalLiteral.TimeUnit;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Describes the addition and subtraction of time units from timestamps.
 * Arithmetic expressions on timestamps are syntactic sugar.
 * They are executed as function call exprs in the BE.
 */
public class TimestampArithmetic extends Expression implements BinaryExpression {
    private static final Logger LOG = LogManager.getLogger(TimestampArithmetic.class);
    private final String funcName;
    private final boolean intervalFirst;
    private Operator op;
    private TimeUnit timeUnit;

    public TimestampArithmetic(String funcName, Expression e1, Expression e2, TimeUnit timeUnit) {
        this(funcName, null, e1, e2, timeUnit, false);
    }

    public TimestampArithmetic(Operator op, Expression e1, Expression e2, TimeUnit timeUnit, boolean intervalFirst) {
        this(null, op, e1, e2, timeUnit, intervalFirst);

    }

    /**
     * Full parameter constructor.
     */
    public TimestampArithmetic(String funcName, Operator op, Expression e1, Expression e2, TimeUnit timeUnit,
            boolean intervalFirst) {
        super(e1, e2);
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
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new TimestampArithmetic(this.funcName, this.op, children.get(0), children.get(1),
                this.timeUnit, this.intervalFirst);
    }

    public Expression withFuncName(String funcName) {
        return new TimestampArithmetic(funcName, this.op, children.get(0), children.get(1), this.timeUnit,
                this.intervalFirst);
    }

    public String getFuncName() {
        return funcName;
    }

    public boolean isIntervalFirst() {
        return intervalFirst;
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
            strBuilder.append(child(1).toSql() + " ");
            strBuilder.append(timeUnit);
            strBuilder.append(" ").append(op.toString()).append(" ");
            strBuilder.append(child(0).toSql());
        } else {
            // Non-function-call like version with interval as second operand.
            strBuilder.append(child(0).toSql());
            strBuilder.append(" " + op.toString() + " ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(child(1).toSql() + " ");
            strBuilder.append(timeUnit);
        }
        return strBuilder.toString();
    }
}
