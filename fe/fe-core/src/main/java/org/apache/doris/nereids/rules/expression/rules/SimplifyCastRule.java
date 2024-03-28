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

import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrite rule of simplify CAST expression.
 * Remove redundant cast like
 * - cast(1 as int) -> 1.
 * Merge cast like
 * - cast(cast(1 as bigint) as string) -> cast(1 as string).
 */
public class SimplifyCastRule implements ExpressionPatternRuleFactory {
    public static SimplifyCastRule INSTANCE = new SimplifyCastRule();

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                matchesType(Cast.class).then(SimplifyCastRule::simplifyCast)
        );
    }

    /** simplifyCast */
    public static Expression simplifyCast(Cast cast) {
        Expression child = cast.child();

        // remove redundant cast
        // CAST(value as type), value is type
        if (cast.getDataType().equals(child.getDataType())) {
            return child;
        }

        if (child instanceof Literal) {
            try {
                DataType castType = cast.getDataType();
                if (castType instanceof StringType) {
                    if (child instanceof VarcharLiteral) {
                        return new StringLiteral(((VarcharLiteral) child).getValue());
                    } else if (child instanceof CharLiteral) {
                        return new StringLiteral(((CharLiteral) child).getValue());
                    }
                } else if (castType instanceof VarcharType) {
                    if (child instanceof VarcharLiteral) {
                        return new VarcharLiteral(((VarcharLiteral) child).getValue(),
                                ((VarcharType) castType).getLen());
                    } else if (child instanceof CharLiteral) {
                        return new VarcharLiteral(((CharLiteral) child).getValue(),
                                ((VarcharType) castType).getLen());
                    }
                } else if (castType instanceof DecimalV2Type) {
                    DecimalV2Type decimalV2Type = (DecimalV2Type) castType;
                    if (child instanceof TinyIntLiteral) {
                        return new DecimalLiteral(decimalV2Type, new BigDecimal(((TinyIntLiteral) child).getValue()));
                    } else if (child instanceof SmallIntLiteral) {
                        return new DecimalLiteral(decimalV2Type, new BigDecimal(((SmallIntLiteral) child).getValue()));
                    } else if (child instanceof IntegerLiteral) {
                        return new DecimalLiteral(decimalV2Type, new BigDecimal(((IntegerLiteral) child).getValue()));
                    } else if (child instanceof BigIntLiteral) {
                        return new DecimalLiteral(decimalV2Type, new BigDecimal(((BigIntLiteral) child).getValue()));
                    }
                } else if (castType instanceof DecimalV3Type) {
                    DecimalV3Type decimalV3Type = (DecimalV3Type) castType;
                    if (child instanceof TinyIntLiteral) {
                        return new DecimalV3Literal(decimalV3Type,
                                new BigDecimal(((TinyIntLiteral) child).getValue()));
                    } else if (child instanceof SmallIntLiteral) {
                        return new DecimalV3Literal(decimalV3Type,
                                new BigDecimal(((SmallIntLiteral) child).getValue()));
                    } else if (child instanceof IntegerLiteral) {
                        return new DecimalV3Literal(decimalV3Type,
                                new BigDecimal(((IntegerLiteral) child).getValue()));
                    } else if (child instanceof BigIntLiteral) {
                        return new DecimalV3Literal(decimalV3Type,
                                new BigDecimal(((BigIntLiteral) child).getValue()));
                    } else if (child instanceof DecimalV3Literal) {
                        return new DecimalV3Literal(decimalV3Type,
                                ((DecimalV3Literal) child).getValue());
                    }
                }
            } catch (Throwable t) {
                return cast;
            }
        }

        if (child != cast.child()) {
            return new Cast(child, cast.getDataType());
        }
        return cast;
    }
}
