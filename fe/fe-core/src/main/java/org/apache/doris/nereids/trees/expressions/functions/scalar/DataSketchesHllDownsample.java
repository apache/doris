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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
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
 * ScalarFunction 'datasketches_hll_downsample'.
 *
 * <p>datasketches_hll_downsample(sketch, lg_k [, tgt_type]) rewrites a serialized Apache DataSketches HLL
 * sketch so that its lgK is min(lg_k, source lgK) and its HLL type is tgt_type (HLL_4 by default). A sketch
 * that already satisfies both is returned byte for byte.
 */
public class DataSketchesHllDownsample extends ScalarFunction
        implements ExplicitlyCastableSignature, PropagateNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
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

    // the bounds the DataSketches library accepts for a sketch's lgK
    private static final int MIN_LG_K = 4;
    private static final int MAX_LG_K = 21;
    private static final Set<String> TGT_TYPES = ImmutableSet.of("HLL_4", "HLL_6", "HLL_8");

    /**
     * constructor with 2 arguments.
     */
    public DataSketchesHllDownsample(Expression arg0, Expression arg1) {
        super("datasketches_hll_downsample", arg0, arg1);
    }

    /**
     * constructor with 3 arguments.
     */
    public DataSketchesHllDownsample(Expression arg0, Expression arg1, Expression arg2) {
        super("datasketches_hll_downsample", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private DataSketchesHllDownsample(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        DataType sketchType = getArgumentType(0);
        if (!(sketchType.isStringLikeType() || sketchType.isVarBinaryType() || sketchType.isNullType())) {
            throw new AnalysisException(getName()
                    + " function's first argument should be of STRING/VARCHAR/VARBINARY type, but was "
                    + sketchType);
        }
        // validate the value FE can evaluate here, because constant folding, e.g. of
        // datasketches_hll_downsample(null, 30), may remove this function before checkLegalityAfterRewrite;
        // the types are checked before the type coercion casts the arguments to the signature
        checkLgK(ExpressionUtils.foldConstantArgument(getArgument(1)));
        if (arity() == 3) {
            checkTgtType(ExpressionUtils.foldConstantArgument(getArgument(2)));
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLgK(getArgument(1));
        if (arity() == 3) {
            checkTgtType(getArgument(2));
        }
    }

    private void checkLgK(Expression lgK) {
        if (!lgK.isConstant() || !lgK.getDataType().isIntegralType()) {
            throw new AnalysisException(getName() + " requires lg_k to be a constant integer: " + toSql());
        }
        // the value of a constant FE cannot fold is validated by BE when it is evaluated
        if (!(lgK instanceof Literal)) {
            return;
        }
        if (!(lgK instanceof IntegerLikeLiteral)) {
            throw new AnalysisException(getName() + " requires lg_k to be a constant integer: " + toSql());
        }
        // compare the full value: narrowing a wide literal to int would wrap it, e.g. 4294967308 into 12
        BigDecimal value = ((IntegerLikeLiteral) lgK).getBigDecimalValue();
        if (value.compareTo(BigDecimal.valueOf(MIN_LG_K)) < 0 || value.compareTo(BigDecimal.valueOf(MAX_LG_K)) > 0) {
            throw new AnalysisException(getName() + " requires lg_k to be between " + MIN_LG_K + " and "
                    + MAX_LG_K + ", but was " + value);
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

    /**
     * withChildren.
     */
    @Override
    public DataSketchesHllDownsample withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        return new DataSketchesHllDownsample(getFunctionParams(children));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        // Catalog matching treats VARCHAR and STRING as interchangeable. Select the input family
        // first so the earlier STRING overload cannot change a VARCHAR result into STRING.
        DataType input = getArgumentType(0);
        DataType output = input.isVarBinaryType() ? VarBinaryType.INSTANCE
                : input.isVarcharType() ? VarcharType.SYSTEM_DEFAULT : StringType.INSTANCE;
        return SIGNATURES.stream().filter(signature -> signature.returnType.equals(output))
                .collect(Collectors.toList());
    }

    @Override
    public boolean foldable() {
        // Keep parameter validation on BE even for a NULL sketch when, for example, lg_k contains
        // crc32() and cannot be evaluated on FE. Binary results also cannot become FE strings.
        return false;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDataSketchesHllDownsample(this, context);
    }
}
