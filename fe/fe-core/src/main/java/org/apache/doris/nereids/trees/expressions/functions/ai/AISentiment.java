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
 * AI function 'AI_Sentiment'
 */
public class AISentiment extends AIFunction {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(StringType.INSTANCE, StringType.INSTANCE),
            FunctionSignature.ret(StringType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
    );

    /**
     * constructor with 1 argument.
     */
    public AISentiment(Expression arg) {
        this(new StringLiteral(getResourceName()), arg);
    }

    /**
     * constructor with 2 argument.
     */
    public AISentiment(Expression arg0, Expression arg1) {
        super("ai_sentiment", arg0, arg1);
    }

    @Override
    public AISentiment withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1 || children.size() == 2,
                "AI_SENTIMENT only accepts 1 or 2 arguments");
        if (children.size() == 1) {
            return new AISentiment(new StringLiteral(getResourceName()),
                    children.get(0));
        }
        return new AISentiment(children.get(0), children.get(1));
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
        return visitor.visitAISentiment(this, context);
    }
}
