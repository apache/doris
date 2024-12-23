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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * represents window function. WindowFunction of this window is saved as Window's child,
 * which is an UnboundFunction at first and will be analyzed as relevant BoundFunction
 * (can be a WindowFunction or AggregateFunction) after BindFunction.
 */
public class WindowExpression extends Expression {

    private final Expression function;

    private final List<Expression> partitionKeys;

    private final List<OrderExpression> orderKeys;

    private final Optional<WindowFrame> windowFrame;

    /** constructor of Window*/
    public WindowExpression(Expression function, List<Expression> partitionKeys, List<OrderExpression> orderKeys) {
        super(new ImmutableList.Builder<Expression>()
                .add(function)
                .addAll(partitionKeys)
                .addAll(orderKeys)
                .build());
        this.function = function;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.orderKeys = ImmutableList.copyOf(orderKeys);
        this.windowFrame = Optional.empty();
    }

    /** constructor of Window*/
    public WindowExpression(Expression function, List<Expression> partitionKeys, List<OrderExpression> orderKeys,
                            WindowFrame windowFrame) {
        super(new ImmutableList.Builder<Expression>()
                .add(function)
                .addAll(partitionKeys)
                .addAll(orderKeys)
                .build());
        this.function = function;
        this.partitionKeys = ImmutableList.copyOf(partitionKeys);
        this.orderKeys = ImmutableList.copyOf(orderKeys);
        this.windowFrame = Optional.of(Objects.requireNonNull(windowFrame));
    }

    public Expression getFunction() {
        return function;
    }

    /**
     * extract expressions from function, partitionKeys and orderKeys
     * todo: expressions from WindowFrame
     */
    public List<Expression> getExpressionsInWindowSpec() {
        List<Expression> expressions = Lists.newArrayList();
        expressions.addAll(function.children());
        expressions.addAll(partitionKeys);
        expressions.addAll(orderKeys.stream()
                .map(UnaryNode::child)
                .collect(Collectors.toList()));
        return expressions;
    }

    public List<Expression> getPartitionKeys() {
        return partitionKeys;
    }

    public List<OrderExpression> getOrderKeys() {
        return orderKeys;
    }

    public Optional<WindowFrame> getWindowFrame() {
        return windowFrame;
    }

    public WindowExpression withWindowFrame(WindowFrame windowFrame) {
        return new WindowExpression(function, partitionKeys, orderKeys, windowFrame);
    }

    public WindowExpression withOrderKeys(List<OrderExpression> orderKeys) {
        return windowFrame.map(frame -> new WindowExpression(function, partitionKeys, orderKeys, frame))
                .orElseGet(() -> new WindowExpression(function, partitionKeys, orderKeys));
    }

    public WindowExpression withPartitionKeysOrderKeys(
            List<Expression> partitionKeys, List<OrderExpression> orderKeys) {
        return windowFrame.map(frame -> new WindowExpression(function, partitionKeys, orderKeys, frame))
                .orElseGet(() -> new WindowExpression(function, partitionKeys, orderKeys));
    }

    public WindowExpression withFunction(Expression function) {
        return windowFrame.map(frame -> new WindowExpression(function, partitionKeys, orderKeys, frame))
                .orElseGet(() -> new WindowExpression(function, partitionKeys, orderKeys));
    }

    public WindowExpression withFunctionPartitionKeysOrderKeys(Expression function,
            List<Expression> partitionKeys, List<OrderExpression> orderKeys) {
        return windowFrame.map(frame -> new WindowExpression(function, partitionKeys, orderKeys, frame))
                .orElseGet(() -> new WindowExpression(function, partitionKeys, orderKeys));
    }

    @Override
    public boolean nullable() {
        return function.nullable();
    }

    @Override
    public WindowExpression withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        int index = 0;
        Expression func = children.get(index);
        index += 1;

        List<Expression> partitionKeys = children.subList(index, index + this.partitionKeys.size());
        index += this.partitionKeys.size();

        List<OrderExpression> orderKeys = children.subList(index, index + this.orderKeys.size()).stream()
                .map(OrderExpression.class::cast)
                .collect(Collectors.toList());
        index += this.orderKeys.size();

        if (index < children.size()) {
            return new WindowExpression(func, partitionKeys, orderKeys, (WindowFrame) children.get(index));
        }
        if (windowFrame.isPresent()) {
            return new WindowExpression(func, partitionKeys, orderKeys, windowFrame.get());
        }
        return new WindowExpression(func, partitionKeys, orderKeys);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WindowExpression window = (WindowExpression) o;
        return Objects.equals(function, window.function)
            && Objects.equals(partitionKeys, window.partitionKeys)
            && Objects.equals(orderKeys, window.orderKeys)
            && Objects.equals(windowFrame, window.windowFrame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(function, partitionKeys, orderKeys, windowFrame);
    }

    @Override
    public String computeToSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(function.toSql()).append(" OVER(");
        if (!partitionKeys.isEmpty()) {
            sb.append("PARTITION BY ").append(partitionKeys.stream()
                    .map(Expression::toSql)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        if (!orderKeys.isEmpty()) {
            sb.append("ORDER BY ").append(orderKeys.stream()
                    .map(OrderExpression::toSql)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        windowFrame.ifPresent(wf -> sb.append(wf.toSql()));
        return sb.toString().trim() + ")";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(function).append(" WindowSpec(");
        if (!partitionKeys.isEmpty()) {
            sb.append("PARTITION BY ").append(partitionKeys.stream()
                    .map(Expression::toString)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        if (!orderKeys.isEmpty()) {
            sb.append("ORDER BY ").append(orderKeys.stream()
                    .map(OrderExpression::toString)
                    .collect(Collectors.joining(", ", "", " ")));
        }
        windowFrame.ifPresent(wf -> sb.append(wf.toSql()));
        return sb.toString().trim() + ")";
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitWindow(this, context);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return function.getDataType();
    }
}
