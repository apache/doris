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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.ForbiddenMetricTypeArguments;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** count agg function. */
public class Count extends AggregateFunction implements AlwaysNotNullable, CustomSignature,
        ForbiddenMetricTypeArguments {

    private final boolean isStar;

    public Count() {
        super("count");
        this.isStar = true;
    }

    public Count(Expression child) {
        super("count", child);
        this.isStar = false;
    }

    public Count(boolean isDistinct, Expression arg0, Expression... varArgs) {
        super("count", isDistinct, ExpressionUtils.mergeArguments(arg0, varArgs));
        this.isStar = false;
        if (!isDistinct && arity() > 1) {
            throw new AnalysisException("COUNT must have DISTINCT for multiple arguments" + this.toSql());
        }
    }

    public boolean isStar() {
        return isStar;
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(BigIntType.INSTANCE, (List) getArgumentsTypes());
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(BigIntType.INSTANCE);
    }

    @Override
    public Count withDistinctAndChildren(boolean isDistinct, List<Expression> children) {
        if (children.size() == 0) {
            if (isDistinct) {
                throw new AnalysisException("Can not count distinct empty arguments");
            }
            return new Count();
        } else if (children.size() == 1) {
            return new Count(isDistinct, children.get(0));
        } else {
            return new Count(isDistinct, children.get(0),
                    children.subList(1, children.size()).toArray(new Expression[0]));
        }
    }

    @Override
    public Count withChildren(List<Expression> children) {
        if (children.size() == 0) {
            return new Count();
        }
        if (children.size() == 1) {
            return new Count(isDistinct, children.get(0));
        } else {
            return new Count(isDistinct, children.get(0),
                    children.subList(1, children.size()).toArray(new Expression[0]));
        }
    }

    @Override
    public String toSql() {
        if (isStar) {
            return "count(*)";
        }
        return super.toSql();
    }

    @Override
    public String toString() {
        if (isStar) {
            return "count(*)";
        }
        return super.toString();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCount(this, context);
    }
}
