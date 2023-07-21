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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** count_by_enum agg function. */
public class CountByEnum extends AggregateFunction implements CustomSignature, AlwaysNotNullable {

    public CountByEnum() {
        super("count_by_enum");
    }

    public CountByEnum(AggregateParam aggregateParam) {
        super("count_by_enum", aggregateParam);
    }

    public CountByEnum(Expression child) {
        super("count_by_enum", child);
    }

    public CountByEnum(AggregateParam aggregateParam, Expression... varArgs) {
        super("count_by_enum", aggregateParam, varArgs);
    }

    @Override
    public AggregateFunction withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 0 || children.size() == 1);
        if (children.size() == 0) {
            return this;
        }
        return new CountByEnum(getAggregateParam(), children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCountByEnum(this, context);
    }

    @Override
    public AggregateFunction withAggregateParam(AggregateParam aggregateParam) {
        if (arity() == 0) {
            return new CountByEnum(aggregateParam);
        } else {
            return new CountByEnum(aggregateParam, child(0));
        }
    }

    @Override
    protected List<DataType> intermediateTypes(List<DataType> argumentTypes, List<Expression> arguments) {
        return ImmutableList.of(StringType.INSTANCE);
    }

    @Override
    public FunctionSignature customSignature(List<DataType> argumentTypes, List<Expression> arguments) {
        return FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE);
    }
}
