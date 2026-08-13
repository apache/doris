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
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ComputePrecision;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * stack(n, expr1, ..., exprk) separates the expressions into n rows in row-major order.
 * Missing values in the last row are padded with nulls.
 */
public class Stack extends TableGeneratingFunction implements CustomSignature, ComputePrecision, AlwaysNullable {

    /** constructor with two or more arguments. */
    public Stack(Expression numRows, Expression argument, Expression... otherArguments) {
        super("stack", ExpressionUtils.mergeArguments(numRows, argument, otherArguments));
    }

    /** constructor for withChildren and reuse signature. */
    private Stack(GeneratorFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public Stack withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2);
        return new Stack(getFunctionParams(children));
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        getColumnTypes();
    }

    @Override
    public FunctionSignature computePrecision(FunctionSignature signature) {
        return signature;
    }

    @Override
    public FunctionSignature searchSignature(List<FunctionSignature> signatures) {
        return super.searchSignature(signatures);
    }

    @Override
    public FunctionSignature customSignature() {
        List<DataType> columnTypes = getColumnTypes();
        List<DataType> argumentTypes = new ArrayList<>(arity());
        argumentTypes.add(IntegerType.INSTANCE);
        for (int i = 1; i < arity(); i++) {
            argumentTypes.add(columnTypes.get((i - 1) % columnTypes.size()));
        }

        if (columnTypes.size() == 1) {
            return FunctionSignature.of(columnTypes.get(0), argumentTypes);
        }
        ImmutableList.Builder<StructField> fields = ImmutableList.builder();
        for (int i = 0; i < columnTypes.size(); i++) {
            fields.add(new StructField("col" + i, columnTypes.get(i), true, ""));
        }
        return FunctionSignature.of(new StructType(fields.build()), argumentTypes);
    }

    private int getNumRows() {
        Expression numRowsArgument = getArgument(0);
        if (!numRowsArgument.isConstant()) {
            throw new AnalysisException("The first argument of stack must be a positive constant integer, but got: "
                    + numRowsArgument.toSql());
        }
        Expression evaluated = FoldConstantRuleOnFE.evaluateWithoutContext(numRowsArgument);
        if (!(evaluated instanceof IntegerLikeLiteral)) {
            throw new AnalysisException("The first argument of stack must be a positive constant integer, but got: "
                    + numRowsArgument.toSql());
        }
        long numRows = ((IntegerLikeLiteral) evaluated).getLongValue();
        if (numRows <= 0 || numRows > Integer.MAX_VALUE) {
            throw new AnalysisException("The first argument of stack must be in (0, " + Integer.MAX_VALUE
                    + "], but got: " + numRows);
        }
        return (int) numRows;
    }

    /** Return the number of logical output columns derived from the row count and value arguments. */
    public int getOutputColumnCount() {
        int numRows = getNumRows();
        return (arity() - 2) / numRows + 1;
    }

    private List<DataType> getColumnTypes() {
        int numFields = getOutputColumnCount();
        List<DataType> columnTypes = new ArrayList<>(numFields);
        for (int columnIndex = 0; columnIndex < numFields; columnIndex++) {
            DataType referenceType = NullType.INSTANCE;
            int referenceArgumentIndex = -1;
            for (int argumentIndex = columnIndex + 1; argumentIndex < arity(); argumentIndex += numFields) {
                DataType fieldType = getArgument(argumentIndex).getDataType();
                if (fieldType.isNullType()) {
                    continue;
                }
                if (referenceType.isNullType()) {
                    referenceType = fieldType;
                    referenceArgumentIndex = argumentIndex;
                    continue;
                }
                if (!referenceType.equals(fieldType)) {
                    throw new AnalysisException("The expressions for stack output column " + columnIndex
                            + " must have compatible types, but argument " + referenceArgumentIndex + " is "
                            + referenceType.toSql() + " while argument " + argumentIndex + " is "
                            + fieldType.toSql());
                }
            }
            columnTypes.add(referenceType);
        }
        return columnTypes;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStack(this, context);
    }
}
