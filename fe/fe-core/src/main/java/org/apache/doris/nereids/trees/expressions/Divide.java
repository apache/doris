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
import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.DoubleType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Divide Expression.
 */
public class Divide extends BinaryArithmetic implements AlwaysNullable {

    public Divide(Expression left, Expression right) {
        super(ImmutableList.of(left, right), Operator.DIVIDE);
    }

    private Divide(List<Expression> children) {
        super(children, Operator.DIVIDE);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new Divide(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDivide(this, context);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        if (left().getDataType().isDecimalV3Type()) {
            DecimalV3Type dt1 = (DecimalV3Type) left().getDataType();
            DecimalV3Type dt2 = (DecimalV3Type) right().getDataType();
            return DecimalV3Type.createDecimalV3Type(dt1.getPrecision(), dt1.getScale() - dt2.getScale());
        }
        return super.getDataType();
    }

    @Override
    public DecimalV3Type getDataTypeForDecimalV3(DecimalV3Type t1, DecimalV3Type t2) {
        int retPercision = t1.getPrecision() + t2.getScale() + Config.div_precision_increment;
        Preconditions.checkState(retPercision <= DecimalV3Type.MAX_DECIMAL256_PRECISION,
                "target precision " + retPercision + " larger than precision "
                        + DecimalV3Type.MAX_DECIMAL256_PRECISION + " in Divide return type");
        int retScale = t1.getScale() + t2.getScale()
                + Config.div_precision_increment;
        int targetPercision = retPercision;
        int targetScale = t1.getScale() + t2.getScale();
        Preconditions.checkState(targetPercision >= targetScale,
                "target scale " + targetScale + " larger than precision " + retPercision
                        + " in Divide return type");
        Preconditions.checkState(retPercision >= retScale,
                "scale " + retScale + " larger than precision " + retPercision
                        + " in Divide return type");
        return DecimalV3Type.createDecimalV3Type(retPercision, retScale);
    }

    @Override
    public DataType getDataTypeForOthers(DataType t1, DataType t2) {
        return DoubleType.INSTANCE;
    }

    // Divide is implemented as a scalar function which return type is always nullable.
    @Override
    public boolean nullable() throws UnboundException {
        return true;
    }
}
