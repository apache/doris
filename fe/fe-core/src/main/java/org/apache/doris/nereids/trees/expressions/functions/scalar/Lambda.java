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

import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.LambdaType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;
import java.util.Objects;

/**
 * Lambda includes lambda arguments and function body
 */
public class Lambda extends Expression implements UnaryExpression {
    final List<String> argumentNames;
    ImmutableList<Slot> arguments;

    /**
     * constructor
     */
    public Lambda(List<String> argumentNames, Expression lambdaFunction) {
        super(lambdaFunction);
        this.argumentNames = argumentNames;
    }

    public Lambda(List<String> argumentNames, ImmutableList<Slot> arguments, List<Expression> lambdaFunction) {
        super(lambdaFunction);
        this.arguments = arguments;
        this.argumentNames = argumentNames;
    }

    /**
     * make slot according array expression
     * @param arrays array expression
     * @return item slots of array expression
     */
    public ImmutableList<Slot> makeArguments(List<Expression> arrays) {
        Builder<Slot> builder = new ImmutableList.Builder<>();
        for (int i = 0; i < arrays.size(); i++) {
            Expression array = arrays.get(i);
            String name = argumentNames.get(i);
            Preconditions.checkArgument(array.getDataType() instanceof ArrayType, "lambda must receive array");
            ArrayType arrayType = (ArrayType) array.getDataType();
            builder.add(new ArrayItemReference(name, arrayType.getItemType(), arrayType.isNullType(), array));
        }
        arguments = builder.build();
        return arguments;
    }

    public ArrayItemReference getLambdaArgument(int i) {
        return (ArrayItemReference) arguments.get(i);
    }

    public List<Slot> getLambdaArguments() {
        return arguments;
    }

    public String getLambdaArgumentName(int i) {
        return argumentNames.get(i);
    }

    public List<String> getLambdaArgumentNames() {
        return argumentNames;
    }

    public Expression getLambdaFunction() {
        return children.get(0);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLambda(this, context);
    }

    @Override
    public Lambda withChildren(List<Expression> children) {
        return new Lambda(argumentNames, arguments, children);
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
        return that.argumentNames.equals(argumentNames) && Objects.equals(children(), that.children());
    }

    @Override
    public String toSql() {
        return String.format("%s -> %s",
                argumentNames,
                child(0).toSql());
    }

    @Override
    public String toString() {
        return String.format("%s -> %s",
                argumentNames,
                child(0).toSql());
    }

    @Override
    public boolean nullable() {
        return children.get(0).nullable();
    }

    @Override
    public DataType getDataType() {
        return new LambdaType();
    }

    public DataType getRetType() {
        return children.get(0).getDataType();
    }
}
