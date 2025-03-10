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
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.types.VariantType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * explode_variant_array(variant([1, 2, 3])), generate 3 lines include 1, 2 and 3.
 */
public class ExplodeVariantArray extends TableGeneratingFunction
        implements ExplicitlyCastableSignature, AlwaysNullable {

    /**
     * constructor with 1 argument.
     */
    public ExplodeVariantArray(Expression[] args) {
        super("explode_variant_array", args);
    }

    /**
     * withChildren.
     */
    @Override
    public ExplodeVariantArray withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new ExplodeVariantArray(children.toArray(new Expression[0]));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        for (Expression child : children) {
            if (!(child.getDataType() instanceof VariantType)) {
                throw new AnalysisException("only support variant type for explode_variant_array function but got "
                        + child.getDataType());
            }
        }
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        List<DataType> arguments = new ArrayList<>();
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        for (int i = 0; i < children.size(); i++) {
            structFields.add(
                new StructField("col" + (i + 1), VariantType.INSTANCE, true, ""));
            arguments.add(VariantType.INSTANCE);
        }
        return ImmutableList.of(FunctionSignature.of(new StructType(structFields.build()), arguments));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExplodeVariant(this, context);
    }
}
