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

package org.apache.doris.nereids.trees.expressions.functions.ai;

import org.apache.doris.catalog.AIResource;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.Resource;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.FloatType;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AI function 'Embed'
 */
public class Embed extends AIFunction {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE)).args(StringType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE)).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE))
                    .args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE))
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE))
                    .args(StringType.INSTANCE, JsonType.INSTANCE),
            FunctionSignature.ret(ArrayType.of(FloatType.INSTANCE))
                    .args(VarcharType.SYSTEM_DEFAULT, JsonType.INSTANCE)
    );

    /**
     * constructor with 1 argument.
     */
    public Embed(Expression arg) {
        this(new StringLiteral(getResourceName()), arg);
    }

    /**
     * constructor with 2 argument.
     */
    public Embed(Expression arg0, Expression arg1) {
        super("embed", arg0, arg1);
    }

    @Override
    public Embed withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 1 && children.size() <= 2,
                "Function EMBED only accepts 1 or 2 arguments");
        if (children.size() == 1) {
            return new Embed(new StringLiteral(getResourceName()),
                    children.get(0));
        }
        return new Embed(children.get(0), children.get(1));
    }

    @Override
    public int getMaxArgsNum() {
        return 2;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitEmbed(this, context);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() == 1) {
            return;
        }
        if (arity() == 2) {
            String aiResourceName = requireStringLiteral(child(0), "resource name",
                    "AI Function must accept literal for the resource name.");
            validateAIResource(aiResourceName);
            return;
        }
        throw new AnalysisException("Function EMBED only accepts 1 or 2 arguments");
    }

    private static String requireStringLiteral(Expression arg, String argName, String errorMsg) {
        if (!(arg instanceof StringLikeLiteral)) {
            throw new AnalysisException(errorMsg);
        }
        String value = ((StringLikeLiteral) arg).getStringValue();
        if (value == null || value.isEmpty()) {
            throw new AnalysisException("EMBED " + argName + " can not be empty.");
        }
        return value;
    }

    private static void validateAIResource(String resourceName) {
        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
        if (!(resource instanceof AIResource)) {
            throw new AnalysisException("AI resource '" + resourceName + "' does not exist");
        }
        Resource.registerUsedAIResourceName(resourceName);
    }

}
