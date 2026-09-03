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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionTrait;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** datasketches_hll_union_agg agg function. */
public class DataSketchesHllUnionAgg extends NotNullableAggregateFunction
        implements ExplicitlyCastableSignature, FunctionTrait, RollUpTrait {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DoubleType.INSTANCE).args(StringType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(DoubleType.INSTANCE).args(VarBinaryType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(StringType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE),
            FunctionSignature.ret(DoubleType.INSTANCE).args(VarBinaryType.INSTANCE, IntegerType.INSTANCE)
    );

    private static final int MIN_LG_MAX_K = 7;
    private static final int MAX_LG_MAX_K = 21;

    /**
     * constructor with 1 argument.
     */
    public DataSketchesHllUnionAgg(Expression arg) {
        super("datasketches_hll_union_agg", arg);
    }

    /** constructor with 2 arguments. */
    public DataSketchesHllUnionAgg(Expression arg0, Expression arg1) {
        super("datasketches_hll_union_agg", arg0, arg1);
    }

    /**
     * constructor with 1 argument.
     */
    public DataSketchesHllUnionAgg(boolean distinct, Expression arg) {
        this(arg);
    }

    /** constructor with 2 arguments. */
    public DataSketchesHllUnionAgg(boolean distinct, Expression arg0, Expression arg1) {
        this(arg0, arg1);
    }

    /** constructor for withChildren and reuse signature */
    protected DataSketchesHllUnionAgg(AggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType inputType = getArgumentType(0);
        if (!(inputType.isStringType() || inputType.isVarcharType() || inputType.isVarBinaryType()
                || inputType.isNullType())) {
            throw new AnalysisException(getName()
                + " function's argument should be of STRING/VARCHAR/VARBINARY type, but was " + inputType);
        }
        if (arity() == 2
                && (!getArgument(1).isConstant() || !getArgumentType(1).isIntegralType())) {
            throw new AnalysisException(getName()
                    + " requires lg_max_k to be a constant integer: " + this.toSql());
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (arity() == 1) {
            return;
        }
        Expression lgMaxK = getArgument(1);
        if (!(lgMaxK instanceof IntegerLikeLiteral)) {
            throw new AnalysisException(getName() + " requires lg_max_k to be a constant integer: " + this.toSql());
        }
        long value = ((IntegerLikeLiteral) lgMaxK).getLongValue();
        if (value < MIN_LG_MAX_K || value > MAX_LG_MAX_K) {
            throw new AnalysisException(getName() + " requires lg_max_k to be between "
                    + MIN_LG_MAX_K + " and " + MAX_LG_MAX_K + ", but was " + value);
        }
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(StringType.INSTANCE);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public DataSketchesHllUnionAgg withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2);
        return new DataSketchesHllUnionAgg(getFunctionParams(distinct, children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDataSketchesHllUnionAgg(this, context);
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        return arity() == 1
                ? new DataSketchesHllUnionAgg(getFunctionParams(ImmutableList.of(param)))
                : new DataSketchesHllUnionAgg(getFunctionParams(ImmutableList.of(param, getArgument(1))));
    }

    @Override
    public boolean canRollUp() {
        return false;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new DoubleLiteral(0);
    }
}
