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

package org.apache.doris.nereids.trees.expressions.functions.window;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.shape.TernaryExpression;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.DecimalV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import java.util.List;

/**
 * types used for specific window functions.
 * This list is equal with Type.trivialTypes used in legacy planner
 */
public abstract class LeadOrLag extends WindowFunction implements TernaryExpression, CustomSignature {

    public LeadOrLag(String name, Expression child, Expression offset, Expression defaultValue) {
        super(name, child, offset, defaultValue);
    }

    protected LeadOrLag(String name, List<Expression> children) {
        super(name, children);
    }

    public Expression getOffset() {
        if (children().size() <= 1) {
            throw new AnalysisException("Not set offset of " + getName() + "(): " + this.toSql());
        }
        return child(1);
    }

    public Expression getDefaultValue() {
        if (children.size() <= 2) {
            throw new AnalysisException("Not set default value of " + getName() + "(): " + this.toSql());
        }
        return child(2);
    }

    @Override
    public boolean nullable() {
        if (children.size() == 3 && child(2).nullable()) {
            return true;
        }
        return child(0).nullable();
    }

    @Override
    public DataType getDataType() {
        return child(0).getDataType();
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (children().size() == 1) {
            return;
        }
        if (children().size() >= 2) {
            checkValidParams(getOffset(), true);
            if (getOffset() instanceof Literal) {
                if (((Literal) getOffset()).getDouble() < 0) {
                    throw new AnalysisException(
                            "The offset parameter of " + getName()
                                    + " must be a constant positive integer: " + this.toSql());
                }
            } else {
                throw new AnalysisException(
                        "The offset parameter of " + getName()
                                + " must be a constant positive integer: " + this.toSql());
            }
            if (children().size() >= 3) {
                checkValidParams(getDefaultValue(), false);
            }
        }
    }

    @Override
    public FunctionSignature customSignature() {
        DataType inputType = getArgument(0).getDataType();
        DataType defaultType = getDefaultValue().getDataType();
        DataType commonType = inputType;
        if (inputType.isNumericType() && defaultType.isNumericType()) {
            for (DataType dataType : TypeCoercionUtils.NUMERIC_PRECEDENCE) {
                if (inputType.equals(dataType) || defaultType.equals(dataType)) {
                    commonType = dataType;
                    break;
                }
            }
            if (commonType.isFloatLikeType() && (inputType.isDecimalV3Type() || defaultType.isDecimalV3Type())) {
                commonType = DoubleType.INSTANCE;
            }
            if (inputType.isDecimalV2Type() || defaultType.isDecimalV2Type()) {
                commonType = DecimalV2Type.SYSTEM_DEFAULT;
            }
            if (inputType.isDecimalV3Type() || defaultType.isDecimalV3Type()) {
                commonType = DecimalV3Type.widerDecimalV3Type(
                        DecimalV3Type.forType(inputType), DecimalV3Type.forType(defaultType), true);
            }
        } else if (inputType.isDateLikeType() && inputType.isDateLikeType()) {
            if (inputType.isDateTimeV2Type() || defaultType.isDateTimeV2Type()) {
                commonType = DateTimeV2Type.getWiderDatetimeV2Type(
                        DateTimeV2Type.forType(inputType), DateTimeV2Type.forType(defaultType));
            } else if (inputType.isDateTimeType() || defaultType.isDateTimeType()) {
                commonType = DateTimeType.INSTANCE;
            } else if (inputType.isDateV2Type() || defaultType.isDateV2Type()) {
                commonType = DateV2Type.INSTANCE;
            } else {
                commonType = DateType.INSTANCE;
            }
        } else if (!defaultType.isNullType()) {
            commonType = StringType.INSTANCE;
        }
        return FunctionSignature.ret(commonType).args(commonType, BigIntType.INSTANCE, commonType);
    }
}
