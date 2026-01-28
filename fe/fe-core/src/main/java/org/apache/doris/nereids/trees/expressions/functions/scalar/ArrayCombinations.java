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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;

/**
 * ScalarFunction 'combinations'
 */
public class ArrayCombinations extends ScalarFunction
        implements CustomSignature, PropagateNullable {

    /**
     * constructor with 2 arguments.
     */
    public ArrayCombinations(Expression arg0, Expression arg1) {
        super("array_combinations", arg0, arg1);
    }

    @Override
    public FunctionSignature customSignature() {
        DataType arg0Type = getArgument(0).getDataType();
        Preconditions.checkArgument(arg0Type instanceof ArrayType,
                "array_combinations first argument must be Array");
        DataType itemType = ((ArrayType) arg0Type).getItemType();
        return FunctionSignature.ret(ArrayType.of(ArrayType.of(itemType)))
            .args(getArgument(0).getDataType(), getArgument(1).getDataType());
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayCombinations withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new ArrayCombinations(children.get(0), children.get(1));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayCombinations(this, context);
    }
}
