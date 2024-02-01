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
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Multiply Expression.
 */
public class Multiply extends BinaryArithmetic implements PropagateNullable {

    public Multiply(Expression left, Expression right) {
        super(ImmutableList.of(left, right), Operator.MULTIPLY);
    }

    public Multiply(List<Expression> children) {
        super(children, Operator.MULTIPLY);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new Multiply(children);
    }

    @Override
    public DecimalV3Type getDataTypeForDecimalV3(DecimalV3Type t1, DecimalV3Type t2) {
        int retPrecision = t1.getPrecision() + t2.getPrecision();
        int retScale = t1.getScale() + t2.getScale();
        boolean enableDecimal256 = false;
        int defaultScale = 6;
        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null) {
            enableDecimal256 = connectContext.getSessionVariable().isEnableDecimal256();
            defaultScale = connectContext.getSessionVariable().decimalOverflowScale;
        }
        if (!enableDecimal256 && retPrecision > DecimalV3Type.MAX_DECIMAL128_PRECISION) {
            int integralPartBoundary = DecimalV3Type.MAX_DECIMAL128_PRECISION - defaultScale;
            if (retPrecision - retScale < integralPartBoundary) {
                // retains more int part
                retScale = DecimalV3Type.MAX_DECIMAL128_PRECISION - (retPrecision - retScale);
            } else if (retPrecision - retScale > integralPartBoundary && retScale < defaultScale) {
                // retScale not change, retains more scale part
            } else {
                retScale = defaultScale;
            }
            retPrecision = DecimalV3Type.MAX_DECIMAL128_PRECISION;
        } else if (enableDecimal256 && retPrecision > DecimalV3Type.MAX_DECIMAL256_PRECISION) {
            int integralPartBoundary = DecimalV3Type.MAX_DECIMAL256_PRECISION - defaultScale;
            if (retPrecision - retScale < integralPartBoundary) {
                // retains more int part
                retScale = DecimalV3Type.MAX_DECIMAL256_PRECISION - (retPrecision - retScale);
            } else if (retPrecision - retScale > integralPartBoundary && retScale < defaultScale) {
                // retScale not change, retains more scale part
            } else {
                retScale = defaultScale;
            }
            retPrecision = DecimalV3Type.MAX_DECIMAL256_PRECISION;
        }
        Preconditions.checkState(retPrecision >= retScale,
                "scale " + retScale + " larger than precision " + retPrecision
                        + " in Multiply return type");
        return DecimalV3Type.createDecimalV3Type(retPrecision, retScale);
    }

    @Override
    public DataType getDataTypeForOthers(DataType t1, DataType t2) {
        return super.getDataTypeForOthers(t1, t2).promotion();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMultiply(this, context);
    }
}
