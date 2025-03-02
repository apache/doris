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
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * explode([1, 2, 3]), generate 3 lines include 1, 2 and 3.
 */
public class Explode extends TableGeneratingFunction implements CustomSignature, AlwaysNullable {

    /**
     * constructor with 1 argument.
     */
    public Explode(Expression[] args) {
        super("explode", args);
    }

    /**
     * withChildren.
     */
    @Override
    public Explode withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1);
        return new Explode(children.toArray(new Expression[0]));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        for (Expression child : children) {
            if (!(child.getDataType() instanceof ArrayType)) {
                throw new AnalysisException("only support array type for explode function but got "
                        + child.getDataType());
            }
        }
    }

    @Override
    public FunctionSignature customSignature() {
        List<DataType> fieldTypes = children.stream().map(ExpressionTrait::getDataType).collect(Collectors.toList());
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            structFields.add(
                    new StructField("col" + (i + 1), ((ArrayType) (fieldTypes.get(i))).getItemType(), true, ""));
        }
        return FunctionSignature.of(new StructType(structFields.build()), fieldTypes);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplode(this, context);
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return super.searchSignature(signatures);
    }
}
