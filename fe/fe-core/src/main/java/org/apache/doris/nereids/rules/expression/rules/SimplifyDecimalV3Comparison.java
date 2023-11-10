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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.rules.expression.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.types.DecimalV3Type;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;

/**
 * if we have a column with decimalv3 type and set enable_decimal_conversion = false.
 * we have a column named col1 with type decimalv3(15, 2)
 * and we have a comparison like col1 > 0.5 + 0.1
 * then the result type of 0.5 + 0.1 is decimalv2(27, 9)
 * and the col1 need to convert to decimalv3(27, 9) to match the precision of right hand
 * this rule simplify it from cast(col1 as decimalv3(27, 9)) > 0.6 to col1 > 0.6
 */
public class SimplifyDecimalV3Comparison extends AbstractExpressionRewriteRule {

    public static SimplifyDecimalV3Comparison INSTANCE = new SimplifyDecimalV3Comparison();

    @Override
    public Expression visitComparisonPredicate(ComparisonPredicate cp, ExpressionRewriteContext context) {
        Expression left = rewrite(cp.left(), context);
        Expression right = rewrite(cp.right(), context);

        if (left.getDataType() instanceof DecimalV3Type
                && left instanceof Cast
                && ((Cast) left).child().getDataType() instanceof DecimalV3Type
                && right instanceof DecimalV3Literal) {
            return doProcess(cp, (Cast) left, (DecimalV3Literal) right);
        }

        if (left != cp.left() || right != cp.right()) {
            return cp.withChildren(left, right);
        } else {
            return cp;
        }
    }

    private Expression doProcess(ComparisonPredicate cp, Cast left, DecimalV3Literal right) {
        BigDecimal trailingZerosValue = right.getValue().stripTrailingZeros();
        int scale = org.apache.doris.analysis.DecimalLiteral.getBigDecimalScale(trailingZerosValue);
        int precision = org.apache.doris.analysis.DecimalLiteral.getBigDecimalPrecision(trailingZerosValue);
        Expression castChild = left.child();
        Preconditions.checkState(castChild.getDataType() instanceof DecimalV3Type);
        DecimalV3Type leftType = (DecimalV3Type) castChild.getDataType();

        if (scale <= leftType.getScale() && precision - scale <= leftType.getPrecision() - leftType.getScale()) {
            // precision and scale of literal all smaller than left, we don't need the cast
            DecimalV3Literal newRight = new DecimalV3Literal(
                    DecimalV3Type.createDecimalV3TypeLooseCheck(leftType.getPrecision(), leftType.getScale()),
                    trailingZerosValue);
            return cp.withChildren(castChild, newRight);
        } else {
            return cp;
        }
    }
}
