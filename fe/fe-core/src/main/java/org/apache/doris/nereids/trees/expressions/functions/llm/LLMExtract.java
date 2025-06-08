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

package org.apache.doris.nereids.trees.expressions.functions.llm;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * LLM function 'LLM_Extract'
 */
public class LLMExtract extends LLMFunction {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, ArrayType.of(StringType.INSTANCE)),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, ArrayType.of(StringType.INSTANCE)),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, ArrayType.of(VarcharType.SYSTEM_DEFAULT)),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, ArrayType.of(VarcharType.SYSTEM_DEFAULT))
    );

    /**
     * Constructor with 2 arguments
     */
    public LLMExtract(Expression arg0, Expression arg1) {
        super("llm_extract", arg0, arg1);
        if (children.size() != 2) {
            throw new AnalysisException("LLM_Extract requires exactly 2 arguments: text and fields array, "
                    + "but got " + children.size() + " arguments");
        }
    }

    @Override
    public LLMExtract withChildren(List<Expression> newChildren) {
        Preconditions.checkArgument(newChildren.size() == 2,
                "LLM_Extract requires exactly 2 arguments");
        return new LLMExtract(newChildren.get(0), newChildren.get(1));
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLLMExtract(this, context);
    }
}
