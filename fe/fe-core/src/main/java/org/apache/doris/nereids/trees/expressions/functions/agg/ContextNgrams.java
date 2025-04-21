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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.NullType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/**
 * AggregateFunction 'context_ngrams'.
 */
public class ContextNgrams extends NullableAggregateFunction implements ExplicitlyCastableSignature {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(ArrayType.of(StringType.INSTANCE))).args(
                ArrayType.of(ArrayType.of(StringType.INSTANCE)),
                IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(ArrayType.of(StringType.INSTANCE))).args(
                ArrayType.of(ArrayType.of(StringType.INSTANCE)),
                IntegerType.INSTANCE, IntegerType.INSTANCE, IntegerType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(ArrayType.of(StringType.INSTANCE))).args(
                ArrayType.of(ArrayType.of(StringType.INSTANCE)),
                ArrayType.of(StringType.INSTANCE), IntegerType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(ArrayType.of(StringType.INSTANCE))).args(
                ArrayType.of(ArrayType.of(StringType.INSTANCE)),
                ArrayType.of(StringType.INSTANCE), IntegerType.INSTANCE, IntegerType.INSTANCE)
    );

    /**
     * Constructor for context_ngrams with 3 arguments:
     */
    public ContextNgrams(Expression sequences, Expression secondArg, Expression k) {
        super("context_ngrams", false, true, Arrays.asList(sequences, secondArg, k));
    }

    /**
     * Constructor for context_ngrams with 4 arguments:
     */
    public ContextNgrams(Expression sequences, Expression secondArg, Expression k, Expression pf) {
        super("context_ngrams", false, true, Arrays.asList(sequences, secondArg, k, pf));
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        if (children.size() == 3) {
            return new ContextNgrams(children.get(0), children.get(1), children.get(2));
        } else if (children.size() == 4) {
            return new ContextNgrams(children.get(0), children.get(1), children.get(2), children.get(3));
        }
        throw new AnalysisException("Invalid number of arguments for context_ngrams: " + children.size());
    }

    @Override
    public ContextNgrams withDistinctAndChildren(boolean distinct, List<Expression> children) {
        if (children.size() == 3) {
            return new ContextNgrams(children.get(0), children.get(1), children.get(2));
        } else if (children.size() == 4) {
            return new ContextNgrams(children.get(0), children.get(1), children.get(2), children.get(3));
        }
        throw new AnalysisException("Invalid number of arguments for context_ngrams: " + children.size());
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() throws AnalysisException {
        DataType contextType = children.get(0).getDataType();
        if (!(contextType instanceof ArrayType)
                || !(((ArrayType) contextType).getItemType() instanceof ArrayType)) {
            throw new AnalysisException(
                "context_ngrams requires array<array<string>> for first parameter: " + toSql());
        }

        if (children.size() >= 3) {
            DataType secondType = children.get(1).getDataType();
            if (secondType instanceof ArrayType) {
                // Check array<string> for core words
                DataType itemType = ((ArrayType) secondType).getItemType();
                if (!(itemType instanceof StringType || itemType instanceof NullType)) {
                    throw new AnalysisException(
                        "context_ngrams requires array<string> for second parameter when using core words: " + toSql());
                }
            } else if (!secondType.isIntegralType()) {
                throw new AnalysisException(
                    "context_ngrams requires integer for n parameter: " + toSql());
            }
        }

        // Check remaining integer parameters
        for (int i = 2; i < children.size(); i++) {
            if (!children.get(i).getDataType().isIntegralType()) {
                throw new AnalysisException(
                    "context_ngrams requires integer for parameter " + (i + 1) + ": " + toSql());
            }
        }
    }

    @Override
    public ContextNgrams withChildren(List<Expression> children) {
        return withDistinctAndChildren(isDistinct(), children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitContextNgrams(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
