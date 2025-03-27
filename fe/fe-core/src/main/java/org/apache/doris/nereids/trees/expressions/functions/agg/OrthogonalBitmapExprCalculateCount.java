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

package org.apache.doris.nereids.trees.expressions.functions.agg;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.scalar.BitmapEmpty;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/** OrthogonalBitmapExprCalculateCount */
public class OrthogonalBitmapExprCalculateCount extends NotNullableAggregateFunction
        implements OrthogonalBitmapFunction, ExplicitlyCastableSignature {

    static final List<FunctionSignature> FUNCTION_SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BigIntType.INSTANCE)
                    .varArgs(BitmapType.INSTANCE, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 3 arguments.
     */
    public OrthogonalBitmapExprCalculateCount(
            Expression bitmap, Expression filterColumn, VarcharLiteral inputString) {
        super("orthogonal_bitmap_expr_calculate_count",
                ExpressionUtils.mergeArguments(bitmap, filterColumn, inputString));
    }

    /**
     * constructor with 3 arguments.
     */
    public OrthogonalBitmapExprCalculateCount(boolean distinct,
            Expression bitmap, Expression filterColumn, VarcharLiteral inputString) {
        super("orthogonal_bitmap_expr_calculate_count", distinct,
                ExpressionUtils.mergeArguments(bitmap, filterColumn, inputString));
    }

    @Override
    public boolean supportAggregatePhase(AggregatePhase aggregatePhase) {
        return aggregatePhase == AggregatePhase.TWO;
    }

    @Override
    public Expression resultForEmptyInput() {
        return new BitmapEmpty();
    }

    @Override
    public OrthogonalBitmapExprCalculateCount withDistinctAndChildren(boolean distinct, List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3
                && children.get(2).getDataType() instanceof CharacterType
                && children.get(2).getDataType() instanceof VarcharType);
        return new OrthogonalBitmapExprCalculateCount(
                distinct, children.get(0), children.get(1), (VarcharLiteral) children.get(2));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return FUNCTION_SIGNATURES;
    }
}
