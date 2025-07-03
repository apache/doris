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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.coercion.Int32OrLessType;

/** ComputePrecisionForRound */
public interface ComputePrecisionForRound extends ComputePrecision {
    @Override
    default FunctionSignature computePrecision(FunctionSignature signature) {
        if (arity() == 1 && signature.getArgType(0) instanceof DecimalV3Type) {
            DecimalV3Type decimalV3Type = DecimalV3Type.forType(getArgumentType(0));
            return signature.withArgumentType(0, decimalV3Type)
                    .withReturnType(DecimalV3Type.createDecimalV3Type(decimalV3Type.getPrecision(), 0));
        } else if (arity() == 2 && signature.getArgType(0) instanceof DecimalV3Type) {
            DecimalV3Type decimalV3Type = DecimalV3Type.forType(getArgumentType(0));
            Expression floatLength = getArgument(1);
            int scale;

            // If scale arg is an integer literal, or it is a cast(Integer as Integer)
            // then we will try to use its value as result scale
            // In any other cases, we will make sure result decimal has same scale with input.
            if ((floatLength.isLiteral() && !floatLength.isNullLiteral()
                    && floatLength.getDataType() instanceof Int32OrLessType)
                    || (floatLength instanceof Cast && floatLength.child(0).isLiteral()
                    && !floatLength.child(0).isNullLiteral()
                    && floatLength.child(0).getDataType() instanceof Int32OrLessType)) {
                if (floatLength instanceof Cast) {
                    scale = ((IntegerLikeLiteral) floatLength.child(0)).getIntValue();
                } else {
                    scale = ((IntegerLikeLiteral) floatLength).getIntValue();
                }
                scale = Math.min(Math.max(scale, 0), decimalV3Type.getScale());
            } else {
                // Func could use Column as its scale argument.
                // Result scale will always same with input Decimal in this situation.
                scale = decimalV3Type.getScale();
            }

            return signature.withArgumentType(0, decimalV3Type)
                    .withReturnType(DecimalV3Type.createDecimalV3Type(decimalV3Type.getPrecision(), scale));
        } else {
            return signature;
        }
    }
}
