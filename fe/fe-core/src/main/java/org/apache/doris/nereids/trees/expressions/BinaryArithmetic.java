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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.NumericType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.List;

/**
 * binary arithmetic operator. Such as +, -, *, /.
 */
public abstract class BinaryArithmetic extends BinaryOperator implements PropagateNullable {

    private final Operator legacyOperator;

    public BinaryArithmetic(List<Expression> children, Operator legacyOperator) {
        super(children, legacyOperator.toString());
        this.legacyOperator = legacyOperator;
    }

    public Operator getLegacyOperator() {
        return legacyOperator;
    }

    @Override
    public DataType inputType() {
        return NumericType.INSTANCE;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        DataType t1 = left().getDataType();
        DataType t2 = right().getDataType();
        if (t1.isDecimalV2Type() && t2.isDecimalV2Type()) {
            return getDataTypeForDecimalV2((DecimalV2Type) t1, (DecimalV2Type) t2);
        }
        if (t1.isDecimalV3Type() && t2.isDecimalV3Type()) {
            return getDataTypeForDecimalV3((DecimalV3Type) t1, (DecimalV3Type) t2);
        }
        return getDataTypeForOthers(t1, t2);
    }

    public DecimalV3Type getDataTypeForDecimalV3(DecimalV3Type t1, DecimalV3Type t2) {
        return t1;
    }

    public DecimalV2Type getDataTypeForDecimalV2(DecimalV2Type t1, DecimalV2Type t2) {
        return DecimalV2Type.SYSTEM_DEFAULT;
    }

    /**
     * get return type if both t1 and t2 are not Decimal Type
     */
    public DataType getDataTypeForOthers(DataType t1, DataType t2) {
        for (DataType dataType : TypeCoercionUtils.NUMERIC_PRECEDENCE) {
            if (t1.equals(dataType) || t2.equals(dataType)) {
                return dataType;
            }
        }
        // should not come here
        throw new AnalysisException("Both side of binary arithmetic is not numeric."
                + " left type is " + left().getDataType() + " and right type is " + right().getDataType());
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBinaryArithmetic(this, context);
    }
}
