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

import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.CastException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.CharType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Double literal
 */
public class DoubleLiteral extends FractionalLiteral {

    public static Set<String> POS_INF_NAME = Sets.newHashSet();
    public static Set<String> NEG_INF_NAME = Sets.newHashSet();
    public static Set<String> NAN_NAME = Sets.newHashSet();

    static {
        POS_INF_NAME.add("infinity");
        POS_INF_NAME.add("inf");
        POS_INF_NAME.add("+infinity");
        POS_INF_NAME.add("+inf");
        NEG_INF_NAME.add("-inf");
        NEG_INF_NAME.add("-infinity");
        NAN_NAME.add("nan");
        NAN_NAME.add("+nan");
        NAN_NAME.add("-nan");
    }

    private final double value;

    public DoubleLiteral(double value) {
        super(DoubleType.INSTANCE);
        this.value = value;
    }

    @Override
    public BigDecimal getBigDecimalValue() {
        return new BigDecimal(String.valueOf(value));
    }

    @Override
    public Double getValue() {
        return value;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDoubleLiteral(this, context);
    }

    @Override
    public LiteralExpr toLegacyLiteral() {
        return new FloatLiteral(value, Type.DOUBLE);
    }

    @Override
    protected Expression uncheckedCastTo(DataType targetType) throws AnalysisException {
        if (this.dataType.equals(targetType)) {
            return this;
        }
        if (targetType.isFloatType()) {
            return new org.apache.doris.nereids.trees.expressions.literal.FloatLiteral(
                    Float.parseFloat(String.valueOf(value)));
        } else if (targetType.isStringType()) {
            return new StringLiteral(getStringValue());
        } else if (targetType.isCharType()) {
            String desc = getStringValue();
            if (((CharType) targetType).getLen() >= desc.length()) {
                return new CharLiteral(desc, ((CharType) targetType).getLen());
            }
        } else if (targetType.isVarcharType()) {
            String desc = getStringValue();
            return new VarcharLiteral(desc, ((VarcharType) targetType).getLen());
        } else if (targetType.isDecimalV2Type() || targetType.isDecimalV3Type()) {
            if (Double.isInfinite(value) || Double.isNaN(value)) {
                throw new CastException(String.format("%s can't cast to %s in strict mode.", getValue(), targetType));
            }
            BigDecimal bigDecimal = new BigDecimal(Double.toString(value));
            return getDecimalLiteral(bigDecimal, targetType);
        }
        return super.uncheckedCastTo(targetType);
    }
}
