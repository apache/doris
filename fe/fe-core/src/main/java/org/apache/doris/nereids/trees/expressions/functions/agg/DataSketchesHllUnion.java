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
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarBinaryType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AggregateFunction 'datasketches_hll_union'.
 *
 * <p>datasketches_hll_union(sketch [, lg_max_k [, tgt_type]]) unions serialized Apache DataSketches
 * HLL sketches and returns a serialized compact sketch matching the first argument's type.
 */
public class DataSketchesHllUnion extends NullableAggregateFunction
        implements ExplicitlyCastableSignature, RollUpTrait, NullIgnoringAggregateFunction {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(VarBinaryType.INSTANCE).args(VarBinaryType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE),
            FunctionSignature.ret(VarBinaryType.INSTANCE).args(VarBinaryType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT)
                    .args(VarcharType.SYSTEM_DEFAULT, IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(VarBinaryType.INSTANCE)
                    .args(VarBinaryType.INSTANCE, IntegerType.INSTANCE, VarcharType.SYSTEM_DEFAULT)
    );

    private static final int MIN_LG_MAX_K = 7;
    private static final int MAX_LG_MAX_K = 21;
    private static final Set<String> TGT_TYPES = ImmutableSet.of("HLL_4", "HLL_6", "HLL_8");

    /** constructor with 1 argument. */
    public DataSketchesHllUnion(Expression arg) {
        this(false, false, arg);
    }

    /** constructor with 2 arguments. */
    public DataSketchesHllUnion(Expression arg0, Expression arg1) {
        this(false, false, arg0, arg1);
    }

    /** constructor with 3 arguments. */
    public DataSketchesHllUnion(Expression arg0, Expression arg1, Expression arg2) {
        this(false, false, arg0, arg1, arg2);
    }

    /** constructor with 1 argument and distinct flag. */
    public DataSketchesHllUnion(boolean distinct, Expression arg) {
        this(false, false, arg);
    }

    /** constructor with 2 arguments and distinct flag. */
    public DataSketchesHllUnion(boolean distinct, Expression arg0, Expression arg1) {
        this(false, false, arg0, arg1);
    }

    /** constructor with 3 arguments and distinct flag. */
    public DataSketchesHllUnion(boolean distinct, Expression arg0, Expression arg1, Expression arg2) {
        this(false, false, arg0, arg1, arg2);
    }

    /** constructor with 1 argument, distinct, and alwaysNullable flag. */
    public DataSketchesHllUnion(boolean distinct, boolean alwaysNullable, Expression arg) {
        super("datasketches_hll_union", false, alwaysNullable, arg);
    }

    /** constructor with 2 arguments, distinct, and alwaysNullable flag. */
    public DataSketchesHllUnion(boolean distinct, boolean alwaysNullable, Expression arg0, Expression arg1) {
        super("datasketches_hll_union", false, alwaysNullable, arg0, arg1);
    }

    /** constructor with 3 arguments, distinct, and alwaysNullable flag. */
    public DataSketchesHllUnion(
            boolean distinct, boolean alwaysNullable, Expression arg0, Expression arg1, Expression arg2) {
        super("datasketches_hll_union", false, alwaysNullable, arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature. */
    private DataSketchesHllUnion(NullableAggregateFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType inputType = getArgumentType(0);
        if (!(inputType.isStringLikeType() || inputType.isVarBinaryType() || inputType.isNullType())) {
            throw new AnalysisException(getName()
                    + " function's argument should be of STRING/VARCHAR/VARBINARY type, but was " + inputType);
        }
        if (arity() >= 2) {
            checkLgMaxK(ExpressionUtils.foldConstantArgument(getArgument(1)));
        }
        if (arity() == 3) {
            checkTgtType(ExpressionUtils.foldConstantArgument(getArgument(2)));
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (arity() >= 2) {
            checkLgMaxK(getArgument(1));
        }
        if (arity() == 3) {
            checkTgtType(getArgument(2));
        }
    }

    private void checkLgMaxK(Expression lgMaxK) {
        if (!lgMaxK.isConstant() || !lgMaxK.getDataType().isIntegralType()) {
            throw new AnalysisException(getName() + " requires lg_max_k to be a constant integer: " + toSql());
        }
        if (!(lgMaxK instanceof Literal)) {
            return;
        }
        if (!(lgMaxK instanceof IntegerLikeLiteral)) {
            throw new AnalysisException(getName() + " requires lg_max_k to be a constant integer: " + toSql());
        }
        BigDecimal value = ((IntegerLikeLiteral) lgMaxK).getBigDecimalValue();
        if (value.compareTo(BigDecimal.valueOf(MIN_LG_MAX_K)) < 0
                || value.compareTo(BigDecimal.valueOf(MAX_LG_MAX_K)) > 0) {
            throw new AnalysisException(getName() + " requires lg_max_k to be between "
                    + MIN_LG_MAX_K + " and " + MAX_LG_MAX_K + ", but was " + value);
        }
    }

    private void checkTgtType(Expression tgtType) {
        if (!tgtType.isConstant() || !tgtType.getDataType().isStringLikeType()) {
            throw new AnalysisException(getName() + " requires tgt_type to be a constant string: " + toSql());
        }
        if (!(tgtType instanceof Literal)) {
            return;
        }
        if (!(tgtType instanceof StringLikeLiteral)) {
            throw new AnalysisException(getName() + " requires tgt_type to be a constant string: " + toSql());
        }
        String value = ((StringLikeLiteral) tgtType).getStringValue();
        if (!TGT_TYPES.contains(value.toUpperCase(Locale.ROOT))) {
            throw new AnalysisException(getName() + " requires tgt_type to be one of HLL_4, HLL_6, HLL_8, but was '"
                    + value + "'");
        }
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return ImmutableList.of(StringType.INSTANCE);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        DataType input = getArgumentType(0);
        DataType output = input.isVarBinaryType() ? VarBinaryType.INSTANCE
                : input.isVarcharType() ? VarcharType.SYSTEM_DEFAULT : StringType.INSTANCE;
        return SIGNATURES.stream()
                .filter(signature -> signature.returnType.equals(output))
                .collect(Collectors.toList());
    }

    @Override
    public DataSketchesHllUnion withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 3);
        return new DataSketchesHllUnion(getFunctionParams(false, children));
    }

    @Override
    public DataSketchesHllUnion withAlwaysNullable(boolean alwaysNullable) {
        return new DataSketchesHllUnion(getAlwaysNullableFunctionParams(alwaysNullable));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDataSketchesHllUnion(this, context);
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        ImmutableList.Builder<Expression> children = ImmutableList.builder();
        children.add(param);
        for (int i = 1; i < arity(); i++) {
            children.add(getArgument(i));
        }
        return new DataSketchesHllUnion(getFunctionParams(false, children.build()));
    }

    @Override
    public boolean canRollUp() {
        return true;
    }
}
