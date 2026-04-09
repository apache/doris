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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'timestamp_spark'.
 *
 * Spark-compatible timestamp conversion. Accepts string, integer, float, double,
 * and decimal inputs.
 *
 * Key differences from Doris's standard timestamp():
 * - Accepts string input directly (no implicit cast to DateTimeV2)
 * - Only accepts '-' as date separator (rejects '/', '_', '.')
 * - Truncates sub-microsecond fractional seconds instead of rounding
 * - Numeric inputs are treated as Unix epoch seconds
 * - Returns NULL for negative numeric values and invalid string formats
 *
 * For literal inputs, computeSignature derives the return type scale from the
 * literal value (e.g., string fractional digits, decimal scale). For non-literal
 * inputs, the return type uses MAX scale (6) to avoid data loss.
 */
public class TimestampSpark extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullLiteral, AlwaysNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(TinyIntType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(SmallIntType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(IntegerType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(BigIntType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.MAX).args(DecimalV3Type.createDecimalV3Type(18, 6)),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(FloatType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.SYSTEM_DEFAULT).args(DoubleType.INSTANCE),
            FunctionSignature.ret(DateTimeV2Type.MAX).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(DateTimeV2Type.MAX).args(StringType.INSTANCE)
    );

    public TimestampSpark(Expression arg) {
        super("timestamp_spark", arg);
    }

    private TimestampSpark(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public TimestampSpark withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new TimestampSpark(getFunctionParams(children));
    }

    @Override
    public FunctionSignature computeSignature(FunctionSignature signature) {
        signature = super.computeSignature(signature);
        if (child() instanceof StringLikeLiteral) {
            String value = ((StringLikeLiteral) child()).getStringValue();
            int scale = computeSparkTimestampScale(value);
            return signature.withReturnType(DateTimeV2Type.of(scale));
        }
        if (child() instanceof DecimalV3Literal) {
            int scale = Math.max(0, ((DecimalV3Literal) child()).getValue().scale());
            scale = Math.min(scale, DateTimeV2Type.MAX_SCALE);
            return signature.withReturnType(DateTimeV2Type.of(scale));
        }
        // Non-literal input: use max precision since we can't determine scale at planning time
        return signature;
    }

    /** Determine the DateTimeV2 scale from a timestamp string's fractional digits.
     *  Counts non-trailing-zero digits after '.', capped at 6 (Spark truncation). */
    private static int computeSparkTimestampScale(String s) {
        int dotPos = s.indexOf('.');
        if (dotPos < 0) {
            return 0;
        }
        int digitCount = 0;
        for (int i = dotPos + 1; i < s.length() && Character.isDigit(s.charAt(i)); i++) {
            digitCount++;
        }
        if (digitCount == 0) {
            return 0;
        }
        // Spark truncates to 6 digits, so only consider the first 6
        int truncated = Math.min(digitCount, DateTimeV2Type.MAX_SCALE);
        // Trim trailing zeros from the truncated portion
        int scale = truncated;
        for (int i = dotPos + truncated; i > dotPos; i--) {
            if (s.charAt(i) != '0') {
                break;
            }
            scale--;
        }
        return scale;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitTimestampSpark(this, context);
    }
}
