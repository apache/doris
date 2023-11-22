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
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'array_zip'.
 */
public class ArrayZip extends ScalarFunction implements ExplicitlyCastableSignature,
        BinaryExpression, PropagateNullable {

    /**
     * constructor with more than 0 arguments.
     */
    public ArrayZip(Expression arg, Expression ...varArgs) {
        super("array_zip", ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /**
     * withChildren.
     */
    @Override
    public ArrayZip withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new ArrayZip(children.get(0), children.subList(1, children.size()).toArray(new Expression[0]));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitArrayZip(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (arity() == 0) {
            SearchSignature.throwCanNotFoundFunctionException(getName(), getArguments());
        }
        if (!children.stream()
                .map(ExpressionTrait::getDataType)
                .filter(dt -> !dt.isNullType())
                .allMatch(ArrayType.class::isInstance)) {
            SearchSignature.throwCanNotFoundFunctionException(getName(), getArguments());
        }
        ImmutableList.Builder<StructField> structFieldBuilder = ImmutableList.builder();
        for (int i = 0; i < children.size(); i++) {
            DataType itemType = TinyIntType.INSTANCE;
            DataType childType = children.get(i).getDataType();
            if (childType instanceof ArrayType) {
                itemType = ((ArrayType) childType).getItemType();
            }
            structFieldBuilder.add(new StructField(String.valueOf(i + 1), itemType, true, ""));
        }

        return ImmutableList.of(FunctionSignature.ret(ArrayType.of(new StructType(structFieldBuilder.build())))
                .args(children.stream().map(ExpressionTrait::getDataType).toArray(DataType[]::new)));
    }
}
