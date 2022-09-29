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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.IntegralType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

/**
 * coercion character literal to another side
 */
public class CharacterLiteralTypeCoercion extends AbstractExpressionRewriteRule {

    public static final CharacterLiteralTypeCoercion INSTANCE = new CharacterLiteralTypeCoercion();

    @Override
    public Expression visitBinaryOperator(BinaryOperator binaryOperator, ExpressionRewriteContext context) {
        Expression left = rewrite(binaryOperator.left(), context);
        Expression right = rewrite(binaryOperator.right(), context);
        if (!(left instanceof Literal) && !(right instanceof Literal)) {
            return binaryOperator.withChildren(left, right);
        }
        if (left instanceof Literal && right instanceof Literal) {
            // process by constant folding
            return binaryOperator.withChildren(left, right);
        }
        if (left instanceof Literal && ((Literal) left).isCharacterLiteral()) {
            left = characterLiteralTypeCoercion(((Literal) left).getStringValue(), right.getDataType()).orElse(left);
        }
        if (right instanceof Literal && ((Literal) right).isCharacterLiteral()) {
            right = characterLiteralTypeCoercion(((Literal) right).getStringValue(), left.getDataType()).orElse(right);

        }
        return binaryOperator.withChildren(left, right);
    }

    private Optional<Expression> characterLiteralTypeCoercion(String value, DataType dataType) {
        Expression ret = null;
        try {
            if (dataType instanceof BooleanType) {
                if ("true".equalsIgnoreCase(value)) {
                    ret = BooleanLiteral.TRUE;
                }
                if ("false".equalsIgnoreCase(value)) {
                    ret = BooleanLiteral.FALSE;
                }
            } else if (dataType instanceof IntegralType) {
                BigInteger bigInt = new BigInteger(value);
                if (BigInteger.valueOf(bigInt.byteValue()).equals(bigInt)) {
                    ret = new TinyIntLiteral(bigInt.byteValue());
                } else if (BigInteger.valueOf(bigInt.shortValue()).equals(bigInt)) {
                    ret = new SmallIntLiteral(bigInt.shortValue());
                } else if (BigInteger.valueOf(bigInt.intValue()).equals(bigInt)) {
                    ret = new IntegerLiteral(bigInt.intValue());
                } else if (BigInteger.valueOf(bigInt.longValue()).equals(bigInt)) {
                    ret = new BigIntLiteral(bigInt.longValueExact());
                } else {
                    ret = new LargeIntLiteral(bigInt);
                }
            } else if (dataType instanceof FloatType) {
                ret = new FloatLiteral(Float.parseFloat(value));
            } else if (dataType instanceof DoubleType) {
                ret = new DoubleLiteral(Double.parseDouble(value));
            } else if (dataType instanceof DecimalType) {
                ret = new DecimalLiteral(new BigDecimal(value));
            } else if (dataType instanceof CharType) {
                ret = new CharLiteral(value, value.length());
            } else if (dataType instanceof VarcharType) {
                ret = new VarcharLiteral(value, value.length());
            } else if (dataType instanceof StringType) {
                ret = new StringLiteral(value);
            } else if (dataType instanceof DateType) {
                ret = new DateLiteral(value);
            } else if (dataType instanceof DateTimeType) {
                ret = new DateTimeLiteral(value);
            }
        } catch (Exception e) {
            // ignore
        }
        return Optional.ofNullable(ret);

    }
}
