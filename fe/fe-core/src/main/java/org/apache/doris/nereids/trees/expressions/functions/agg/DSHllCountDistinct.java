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
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.coercion.AnyDataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Use Apache datasketches to estimate the number of distinct values.
 * see <a href="https://datasketches.apache.org/">Apache datasketches</a>
 */
public class DSHllCountDistinct extends NotNullableAggregateFunction implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE).args(AnyDataType.INSTANCE_WITHOUT_INDEX),
            FunctionSignature.ret(BigIntType.INSTANCE).args(AnyDataType.INSTANCE_WITHOUT_INDEX, TinyIntType.INSTANCE),
            FunctionSignature.ret(BigIntType.INSTANCE).args(AnyDataType.INSTANCE_WITHOUT_INDEX, TinyIntType.INSTANCE,
                    CharType.SYSTEM_DEFAULT)
    );

    private static final Set<String> SUPPORTED_HLL_TYPE = new HashSet<>(
            Arrays.asList("HLL_4", "HLL_6", "HLL_8"));

    private static final int MIN_LOG_K = 4;

    private static final int MAX_LOG_K = 21;

    /**
     * constructor
     * @param arg 0: input column, 1: logK, 2: tgtType
     */
    public DSHllCountDistinct(Expression ...arg) {
        super("ds_hll_count_distinct", arg);
        if (arg.length > 1) {
            Expression logK = arg[1];
            if (logK instanceof TinyIntLiteral) {
                TinyIntLiteral logKLitteral = (TinyIntLiteral) logK;
                int logKInt = logKLitteral.getIntValue();
                if (logKInt < MIN_LOG_K || logKInt > MAX_LOG_K) {
                    throw new AnalysisException("logK must be in range [4, 21]");
                }
            } else {
                throw new AnalysisException("logK must be a tinyint");
            }
            if (arg.length > 2) {
                Expression tgtType = arg[2];
                if (tgtType instanceof VarcharLiteral) {
                    VarcharLiteral tgtTypeLiteral = (VarcharLiteral) tgtType;
                    String tgtTypeString = tgtTypeLiteral.getStringValue();
                    if (!SUPPORTED_HLL_TYPE.contains(tgtTypeString)) {
                        throw new AnalysisException("tgtType must be one of HLL_4, HLL_6, HLL_8");
                    }
                } else {
                    throw new AnalysisException("hll type must be a varchar");
                }
            }
        }
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (getArgumentType(0).isOnlyMetricType()) {
            throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
        }
    }

    @Override
    public DSHllCountDistinct withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty() && children.size() <= 3);
        return new DSHllCountDistinct(children.toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDSHllCountDistinct(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new BigIntLiteral(0);
    }
}
