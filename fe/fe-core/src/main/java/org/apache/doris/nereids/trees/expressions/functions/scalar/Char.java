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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'char'.
 */
public class Char extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNullable {
    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE, IntegerType.INSTANCE));

    public Char(List<Expression> varArgs) {
        super("char", varArgs);
    }

    public Char(Expression... varArgs) {
        super("char", varArgs);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (!(child(0) instanceof StringLikeLiteral)) {
            throw new AnalysisException("char charset name must be a constant: " + child(0).toSql());
        }
        StringLikeLiteral stringLiteral = (StringLikeLiteral) child(0);
        if (!"utf8".equalsIgnoreCase(stringLiteral.getStringValue())) {
            throw new AnalysisException(
                    "char function currently only support charset name 'utf8': " + child(0).toSql());
        }
    }

    /**
     * withChildren.
     */
    @Override
    public Char withChildren(List<Expression> children) {
        return new Char(children);
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitChar(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }
}
