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
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.functions.SearchSignature;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * PosExplode(array('a','b','c')) generate two columns and three rows with:
 * pose column: 0, 1, 2
 * value column: 'a', 'b', 'c'
 */
public class PosExplodeOuter extends TableGeneratingFunction implements
        CustomSignature, ComputePrecision, AlwaysNullable {

    /**
     * constructor with one or more arguments.
     */
    public PosExplodeOuter(Expression arg, Expression... others) {
        super("posexplode_outer", ExpressionUtils.mergeArguments(arg, others));
    }

    /** constructor for withChildren and reuse signature */
    private PosExplodeOuter(GeneratorFunctionParams functionParams) {
        super(functionParams);
    }

    /**
     * withChildren.
     */
    @Override
    public PosExplodeOuter withChildren(List<Expression> children) {
        Preconditions.checkArgument(!children.isEmpty());
        return new PosExplodeOuter(getFunctionParams(children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        for (Expression c : children) {
            if (!(c.getDataType() instanceof ArrayType)) {
                throw new AnalysisException("only support array type for posexplode_outer function but got "
                        + c.getDataType());
            }
        }
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        return signature;
    }

    @Override
    public FunctionSignature customSignature() {
        List<DataType> arguments = new ArrayList<>();
        ImmutableList.Builder<StructField> structFields = ImmutableList.builder();
        structFields.add(new StructField(PosExplode.POS_COLUMN, IntegerType.INSTANCE, false, ""));
        for (int i = 0; i < children.size(); i++) {
            if (children.get(i).getDataType().isArrayType()) {
                structFields.add(
                    new StructField(StructLiteral.COL_PREFIX + (i + 1),
                        ((ArrayType) (children.get(i)).getDataType()).getItemType(), true, ""));
                arguments.add(children.get(i).getDataType());
            } else {
                SearchSignature.throwCanNotFoundFunctionException(this.getName(), getArguments());
            }
        }
        StructType structType = new StructType(structFields.build());
        return FunctionSignature.of(structType, arguments);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPosExplodeOuter(this, context);
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return super.searchSignature(signatures);
    }
}
