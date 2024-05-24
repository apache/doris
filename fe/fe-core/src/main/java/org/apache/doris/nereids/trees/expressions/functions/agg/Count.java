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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.List;

/** count agg function. */
public class Count extends AggregateFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable, SupportWindowAnalytic, RollUpTrait {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            // count(*)
            FunctionSignature.ret(BigIntType.INSTANCE).args(),
            FunctionSignature.ret(BigIntType.INSTANCE).varArgs(AnyDataType.INSTANCE_WITHOUT_INDEX)
    );

    private final boolean isStar;

    public Count() {
        super("count");
        this.isStar = true;
    }

    /**
     * this constructor use for COUNT(c1, c2) to get correct error msg.
     */
    public Count(Expression child, Expression... varArgs) {
        this(false, child, varArgs);
    }

    public Count(boolean distinct, Expression arg0, Expression... varArgs) {
        super("count", distinct, ExpressionUtils.mergeArguments(arg0, varArgs));
        this.isStar = false;
    }

    public boolean isCountStar() {
        return isStar
                || children.isEmpty()
                || (children.size() == 1 && child(0) instanceof Literal);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        // for multiple exprs count must be qualified with distinct
        if (arity() > 1 && !distinct) {
            throw new AnalysisException("COUNT must have DISTINCT for multiple arguments: " + this.toSql());
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        // after rewrite, count(distinct bitmap_column) should be rewritten to bitmap_union_count(bitmap_column)
        for (Expression argument : getArguments()) {
            if (distinct && (argument.getDataType().isComplexType()
                    || argument.getDataType().isObjectType() || argument.getDataType().isJsonType())) {
                throw new AnalysisException("COUNT DISTINCT could not process type " + this.toSql());
            }
        }
    }

    public boolean isStar() {
        return isStar;
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
    public Count withDistinctAndChildren(boolean distinct, List<Expression> children) {
        if (children.size() == 0) {
            if (distinct) {
                throw new AnalysisException("Can not count distinct empty arguments");
            }
            return new Count();
        } else if (children.size() == 1) {
            return new Count(distinct, children.get(0));
        } else {
            return new Count(distinct, children.get(0),
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

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        if (this.isDistinct()) {
            return new BitmapUnionCount(param);
        } else {
            return new Sum(param);
        }
    }

    @Override
    public boolean canRollUp() {
        return true;
    }
}
