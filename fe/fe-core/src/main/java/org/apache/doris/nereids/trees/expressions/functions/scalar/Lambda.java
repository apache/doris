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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LambdaType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Lambda includes lambda arguments and function body
 * Before bind, x -> x : arguments("x") -> children: Expression(x)
 * After bind, x -> x : arguments("x") -> children: Expression(x) ArrayItemReference(x)
 */
public class Lambda extends Expression {

    private final List<String> argumentNames;

    /**
     * constructor
     */
    public Lambda(List<String> argumentNames, Expression lambdaFunction) {
        this(argumentNames, ImmutableList.of(lambdaFunction));
    }

    public Lambda(List<String> argumentNames, Expression lambdaFunction, List<ArrayItemReference> arguments) {
        this(argumentNames, ImmutableList.<Expression>builder().add(lambdaFunction).addAll(arguments).build());
    }

    public Lambda(List<String> argumentNames, List<Expression> children) {
        super(children);
        this.argumentNames = ImmutableList.copyOf(Objects.requireNonNull(
                argumentNames, "argumentNames should not null"));
    }

    /**
     * make slot according array expression
     * @param arrays array expression
     * @return item slots of array expression
     */
    public ImmutableList<ArrayItemReference> makeArguments(List<Expression> arrays) {
        Builder<ArrayItemReference> builder = new ImmutableList.Builder<>();
        if (arrays.size() != argumentNames.size()) {
            throw new AnalysisException(String.format("lambda %s arguments' size is not equal parameters' size",
                    toSql()));
        }
        for (int i = 0; i < arrays.size(); i++) {
            Expression array = arrays.get(i);
            if (!(array.getDataType() instanceof ArrayType)) {
                throw new AnalysisException(String.format("lambda argument must be array but is %s", array));
            }
            String name = argumentNames.get(i);
            builder.add(new ArrayItemReference(name, array));
        }
        return builder.build();
    }

    public String getLambdaArgumentName(int i) {
        return argumentNames.get(i);
    }

    public ArrayItemReference getLambdaArgument(int i) {
        return (ArrayItemReference) children.get(i + 1);
    }

    public List<ArrayItemReference> getLambdaArguments() {
        return children.stream()
                .skip(1)
                .map(e -> (ArrayItemReference) e)
                .collect(Collectors.toList());
    }

    public List<String> getLambdaArgumentNames() {
        return argumentNames;
    }

    public Expression getLambdaFunction() {
        return child(0);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLambda(this, context);
    }

    public Lambda withLambdaFunctionArguments(Expression lambdaFunction, List<ArrayItemReference> arguments) {
        return new Lambda(argumentNames, lambdaFunction, arguments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Lambda that = (Lambda) o;
        return argumentNames.equals(that.argumentNames)
                && Objects.equals(children(), that.children());
    }

    @Override
    public String computeToSql() {
        StringBuilder builder = new StringBuilder();
        String argStr = argumentNames.get(0);
        if (argumentNames.size() > 1) {
            argStr = argumentNames.stream().collect(Collectors.joining(", ", "(", ")"));
        }
        builder.append(String.format("%s -> %s", argStr, getLambdaFunction().toSql()));
        for (int i = 1; i < getArguments().size(); i++) {
            builder.append(", ").append(getArgument(i).toSql());
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        String argStr = argumentNames.get(0);
        if (argumentNames.size() > 1) {
            argStr = argumentNames.stream().collect(Collectors.joining(", ", "(", ")"));
        }
        builder.append(String.format("%s -> %s", argStr, getLambdaFunction().toString()));
        for (int i = 1; i < getArguments().size(); i++) {
            builder.append(", ").append(getArgument(i).toString());
        }
        return builder.toString();
    }

    @Override
    public Lambda withChildren(List<Expression> children) {
        return new Lambda(argumentNames, children);
    }

    @Override
    public boolean nullable() {
        return getLambdaArguments().stream().anyMatch(ArrayItemReference::nullable);
    }

    @Override
    public DataType getDataType() {
        return new LambdaType();
    }

    public DataType getRetType() {
        return getLambdaFunction().getDataType();
    }
}
