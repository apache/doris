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

package org.apache.doris.nereids.trees.expressions.functions.generator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * explode([1, 2, 3]), generate 3 lines include 1, 2 and 3.
 */
public class ExplodeOuter extends TableGeneratingFunction implements ExplicitlyCastableSignature, AlwaysNullable {

    /**
     * constructor with one or more argument.
     */
    public ExplodeOuter(Expression[] args) {
        super("explode_outer", args);
    }

    /**
     * withChildren.
     */
    @Override
    public ExplodeOuter withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new ExplodeOuter(children.toArray(new Expression[0]));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        for (Expression child : children) {
            if (!child.isNullLiteral() && !(child.getDataType() instanceof ArrayType)) {
                throw new AnalysisException("only support array type for explode_outer function but got "
                    + child.getDataType());
            }
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        List<DataType> arguments = new ArrayList<>();
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).isNullLiteral()) {
                arguments.add(ArrayType.of(NullType.INSTANCE));
                structFields.add(
                    new StructField("col" + (i + 1), ArrayType.of(NullType.INSTANCE).getItemType(), true, ""));
            } else {
                structFields.add(
                    new StructField("col" + (i + 1),
                        ((ArrayType) (children.get(i)).getDataType()).getItemType(), true, ""));
                arguments.add(children.get(i).getDataType());
            }
        }
        return ImmutableList.of(FunctionSignature.of(new StructType(structFields.build()), arguments));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplodeOuter(this, context);
    }
}
