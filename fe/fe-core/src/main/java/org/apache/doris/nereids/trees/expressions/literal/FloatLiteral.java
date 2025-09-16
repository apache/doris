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

package org.apache.doris.nereids.trees.expressions.literal;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.VarcharType;

import java.math.BigDecimal;

/**
 * float type literal
 */
public class FloatLiteral extends FractionalLiteral {

    private final float value;

    public FloatLiteral(float value) {
        super(FloatType.INSTANCE);
        this.value = value;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(String.valueOf(value));
    }

    @Override
    public Float getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitFloatLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new org.apache.doris.analysis.FloatLiteral(getDouble(), Type.FLOAT);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isDoubleType()) {
            return new DoubleLiteral(Double.parseDouble(String.valueOf(value)));
        } else if (targetType.isStringType()) {
            return new StringLiteral(castToString());
        } else if (targetType.isCharType()) {
            String desc = castToString();
            if (((CharType) targetType).getLen() >= desc.length()) {
                return new CharLiteral(desc, ((CharType) targetType).getLen());
            }
        } else if (targetType.isVarcharType()) {
            String desc = castToString();
            return new VarcharLiteral(desc, ((VarcharType) targetType).getLen());
        } else if (targetType.isDecimalV2Type() || targetType.isDecimalV3Type()) {
            if (Float.isInfinite(value) || Float.isNaN(value)) {
                throw new CastException(String.format("%s can't cast to %s in strict mode.", getValue(), targetType));
            }
            BigDecimal bigDecimal = new BigDecimal(Float.toString(value));
            return getDecimalLiteral(bigDecimal, targetType);
        }
        return super.uncheckedCastTo(targetType);
    }
}
