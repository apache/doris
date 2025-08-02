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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * explode_variant_array(variant([1, 2, 3])), generate three rows include 1, 2 and 3.
 * explode_variant_array(variant([1, 2, 3]), variant([4, 5, 6])) generates two columns and three rows
 * where the first column contains 1, 2, 3, and the second column contains 4, 5, 6.
 */
public class ExplodeVariantArray extends TableGeneratingFunction implements
        CustomSignature, ComputePrecision, AlwaysNullable {

    /**
     * constructor with one or more argument.
     */
    public ExplodeVariantArray(Expression[] args) {
        super("explode_variant_array", args);
    }

    /** constructor for withChildren and reuse signature */
    private ExplodeVariantArray(GeneratorFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public ExplodeVariantArray withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new ExplodeVariantArray(getFunctionParams(children));
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        return signature;
    }

    @Override
    public FunctionSignature customSignature() {
        List<DataType> arguments = new ArrayList<>();
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).getDataType() instanceof VariantType) {
                structFields.add(
                    new StructField("col" + (i + 1), VariantType.INSTANCE, true, ""));
                arguments.add(VariantType.INSTANCE);
            } else {
                SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            }
        }
        return FunctionSignature.of(new StructType(structFields.build()), arguments);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplodeVariant(this, context);
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return super.searchSignature(signatures);
    }
}
