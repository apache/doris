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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * ScalarFunction 'EncryptKeyRef'.
 */
public class EncryptKeyRef extends ScalarFunction
        implements BinaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(StringType.INSTANCE).args(StringType.INSTANCE, StringType.INSTANCE)
    );

    /**
     * constructor with 2 arguments.
     */
    public EncryptKeyRef(Expression arg0, Expression arg1) {
        super("encryptKeyRef", Lists.newArrayList(arg0, arg1));
    }

    public EncryptKeyRef(List<Expression> args) {
        super("encryptKeyRef", args);
    }

    public String getDbName() {
        Preconditions.checkArgument(children.get(0) instanceof StringLikeLiteral);
        return ((StringLikeLiteral) children.get(0)).value;
    }

    public String getEncryptKeyName() {
        Preconditions.checkArgument(children.get(1) instanceof StringLikeLiteral);
        return ((StringLikeLiteral) children.get(1)).value;
    }

    /**
     * withChildren.
     */
    @Override
    public EncryptKeyRef withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new EncryptKeyRef(children);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitEncryptKeyRef(this, context);
    }
}
