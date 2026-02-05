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

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * AI function 'AI_Translate'
 */
public class AITranslate extends AIFunction {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 2 argument.
     */
    public AITranslate(Expression arg0, Expression arg1) {
        this(new StringLiteral(getResourceName()), arg0, arg1);
    }

    /**
     * constructor with 3 argument.
     */
    public AITranslate(Expression arg0, Expression arg1, Expression arg2) {
        super("ai_translate", arg0, arg1, arg2);
    }

    @Override
    public AITranslate withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3,
                "AI_TRANSLATE only accepts 2 or 3 arguments");
        if (children.size() == 2) {
            return new AITranslate(new StringLiteral(getResourceName()),
                    children.get(0), children.get(1));
        }
        return new AITranslate(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public int getMaxArgsNum() {
        return 3;
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAITranslate(this, context);
    }
}
