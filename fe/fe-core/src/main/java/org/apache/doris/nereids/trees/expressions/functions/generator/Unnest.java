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
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.BitmapType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * unnest([1, 2, 3]), generate three rows include 1, 2 and 3.
 * unnest([1, 2, 3], [4, 5, 6]) generates two columns and three rows
 * where the first column contains 1, 2, 3, and the second column contains 4, 5, 6.
 */
public class Unnest extends TableGeneratingFunction implements CustomSignature, ComputePrecision, AlwaysNullable {

    private boolean isOuter;
    private boolean needOrdinality;

    /**
     * constructor with one argument.
     */
    public Unnest(Expression argument) {
        this(false, false, ImmutableList.of(argument));
    }

    /**
     * constructor with more argument.
     */
    public Unnest(boolean isOuter, boolean needOrdinality, List<Expression> arguments) {
        super("unnest", arguments);
        this.isOuter = isOuter;
        this.needOrdinality = needOrdinality;
    }

    /** constructor for withChildren and reuse signature */
    private Unnest(boolean isOuter, boolean needOrdinality, GeneratorFunctionParams functionParams) {
        super(functionParams);
        this.isOuter = isOuter;
        this.needOrdinality = needOrdinality;
    }

    /**
     * withChildren.
     */
    @Override
    public Unnest withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new Unnest(isOuter, needOrdinality, getFunctionParams(children));
    }

    public Unnest withOuter(boolean isOuter) {
        return new Unnest(isOuter, needOrdinality, getFunctionParams(children));
    }

    public boolean isOuter() {
        return isOuter;
    }

    public boolean needOrdinality() {
        return needOrdinality;
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        return signature;
    }

    @Override
    public FunctionSignature customSignature() {
        DataType childDataType = child(0).getDataType();
        if (childDataType.isArrayType() || childDataType.isNullType()) {
            List<DataType> arguments = new ArrayList<>(children.size());
            ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
            for (int i = 0; i < children.size(); i++) {
                if (children.get(i).getDataType().isNullType()) {
                    arguments.add(ArrayType.of(NullType.INSTANCE));
                    structFields.add(
                            new StructField("col" + (i + 1), NullType.INSTANCE, true, ""));
                } else if (children.get(i).getDataType().isArrayType()) {
                    structFields.add(
                            new StructField("col" + (i + 1),
                                    ((ArrayType) (children.get(i)).getDataType()).getItemType(), true, ""));
                    arguments.add(children.get(i).getDataType());
                } else {
                    SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
                }
            }

            StructType structType = new StructType(structFields.build());
            if (arguments.size() == 1) {
                return FunctionSignature.of(structType.getFields().get(0).getDataType(), arguments);
            }
            return FunctionSignature.of(structType, arguments);
        } else if (childDataType.isBitmapType()) {
            return FunctionSignature.ret(BigIntType.INSTANCE).args(BitmapType.INSTANCE);
        } else if (childDataType.isMapType()) {
            return FunctionSignature.ret(new StructType(ImmutableList.of(
                            new StructField("col1", ((MapType) child(0).getDataType()).getKeyType(), true, ""),
                            new StructField("col2", ((MapType) child(0).getDataType()).getValueType(), true, ""))))
                    .args(child(0).getDataType());
        } else {
            SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            return null;
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnnest(this, context);
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return super.searchSignature(signatures);
    }
}
