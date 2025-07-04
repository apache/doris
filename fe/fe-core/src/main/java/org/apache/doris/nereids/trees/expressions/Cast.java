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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.DateLikeType;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * cast function.
 */
public class Cast extends Expression implements UnaryExpression, Monotonic {

    // CAST can be from SQL Query or Type Coercion.
    private final boolean isExplicitType;

    private final DataType targetType;

    public Cast(Expression child, DataType targetType, boolean isExplicitType) {
        super(ImmutableList.of(child));
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = isExplicitType;
    }

    public Cast(Expression child, DataType targetType) {
        super(ImmutableList.of(child));
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = false;
    }

    private Cast(List<Expression> child, DataType targetType, boolean isExplicitType) {
        super(child);
        this.targetType = Objects.requireNonNull(targetType, "targetType can not be null");
        this.isExplicitType = isExplicitType;
    }

    public boolean isExplicitType() {
        return isExplicitType;
    }

    @Override
    public DataType getDataType() {
        return targetType;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public boolean nullable() {
        if (ConnectContext.get().getSessionVariable().enableStrictCast()) {
            if (targetType.isNumericType() || targetType.isDateLikeType() || targetType.isBooleanType()) {
                return child().nullable();
            }
            DataType childDataType = child().getDataType();
            if (childDataType.isStringLikeType() && !targetType.isStringLikeType()) {
                return true;
            } else if (!childDataType.isDateLikeType() && targetType.isDateLikeType()) {
                return true;
            } else if (!childDataType.isTimeType() && targetType.isTimeType()) {
                return true;
            } else if (childDataType.isJsonType() || targetType.isJsonType()) {
                return true;
            } else if (childDataType.isVariantType() || targetType.isVariantType()) {
                return true;
            } else {
                return child().nullable();
            }
        } else {
            return unStrictCastNullable();
        }
    }

    protected boolean unStrictCastNullable() {
        if (child().nullable()) {
            return true;
        }
        DataType childDataType = child().getDataType();
        if (childDataType.isStringLikeType() && !targetType.isStringLikeType()) {
            return true;
        } else if (targetType.isDateLikeType() || targetType.isTimeType()) {
            // for date/time types, parsing or converting from numbers may generate null.
            // datetime scale reduction may also generate null. that's all for them.
            if (childDataType.isStringLikeType() || childDataType.isNumericType()) {
                return true;
            } else if (childDataType.isDateTimeV2Type() && targetType.isDateTimeV2Type()) {
                return true;
            }
            return false;
        } else if (childDataType.isJsonType() || targetType.isJsonType()) {
            return true;
        } else if (childDataType.isVariantType() || targetType.isVariantType()) {
            return true;
        } else if (childDataType.isIntegralType()) {
            // integral to integral
            if (targetType.isIntegralType()) {
                if (childDataType.isLargeIntType() && !targetType.isLargeIntType()) {
                    return true;
                }
                if (childDataType.isBigIntType() && !targetType.isLargeIntType() && !targetType.isBigIntType()) {
                    return true;
                }
                if (childDataType.isIntegerType() && (targetType.isSmallIntType() || targetType.isTinyIntType())) {
                    return true;
                }
                return childDataType.isSmallIntType() && targetType.isTinyIntType();
            } else if (targetType.isDecimalLikeType()) {
                // Integral to decimal
                int range = targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getRange()
                        : ((DecimalV3Type) targetType).getRange();
                if (childDataType.isTinyIntType() && range < 3) {
                    return true;
                } else if (childDataType.isTinyIntType() && range < 3) {
                    return true;
                } else if (childDataType.isSmallIntType() && range < 5) {
                    return true;
                } else if (childDataType.isIntegerType() && range < 10) {
                    return true;
                } else if (childDataType.isBigIntType() && range < 19) {
                    return true;
                } else {
                    return childDataType.isLargeIntType() && range < 39;
                }
            }
        } else if (childDataType.isFloatType() || childDataType.isDoubleType()) {
            // Double/Float to integral or decimal
            return targetType.isIntegralType() || targetType.isDecimalLikeType();
        } else if (childDataType.isDecimalLikeType()) {
            // Decimal to integral
            if (targetType.isIntegralType()) {
                int range = 0;
                if (childDataType.isDecimalV2Type()) {
                    range = ((DecimalV2Type) childDataType).getRange();
                } else {
                    range = ((DecimalV3Type) childDataType).getRange();
                }
                if (range >= 39) {
                    return true;
                }
                if (targetType.isTinyIntType() && range >= 3) {
                    return true;
                }
                if (targetType.isSmallIntType() && range >= 5) {
                    return true;
                }
                if (targetType.isIntegerType() && range >= 10) {
                    return true;
                }
                return targetType.isBigIntType() && range >= 19;
            } else if (targetType.isDecimalLikeType()) {
                // Decimal to decimal
                int targetRange = targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getRange()
                        : ((DecimalV3Type) targetType).getRange();
                int sourceRange = childDataType.isDecimalV2Type() ? ((DecimalV2Type) childDataType).getRange()
                        : ((DecimalV3Type) childDataType).getRange();
                return sourceRange > targetRange;
            }
        } else if (childDataType.isTimeType() && targetType.isIntegralType()) {
            // Time to integral
            return targetType.isTinyIntType() || targetType.isSmallIntType() || targetType.isIntegerType();
        } else if (childDataType.isBooleanType() && targetType.isDecimalLikeType()) {
            // Boolean to decimal
            return (targetType.isDecimalV2Type() ? ((DecimalV2Type) targetType).getRange()
                    : ((DecimalV3Type) targetType).getRange()) <= 1;
        }
        return false;
    }

    @Override
    public Cast withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Cast(children, targetType, isExplicitType);
    }

    @Override
    public String computeToSql() throws UnboundException {
        return "cast(" + child().toSql() + " as " + targetType.toSql() + ")";
    }

    @Override
    public String toString() {
        return "cast(" + child() + " as " + targetType + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        Cast cast = (Cast) o;
        return Objects.equals(targetType, cast.targetType);
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), targetType);
    }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    public Expression withConstantArgs(Expression literal) {
        return new Cast(literal, targetType, isExplicitType);
    }

    @Override
    public boolean isMonotonic(Literal lower, Literal upper) {
        // Both upward and downward casting of date types satisfy monotonicity.
        if (child().getDataType() instanceof DateLikeType && targetType instanceof DateLikeType) {
            return true;
        }
        return false;
    }
}
