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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * like expression: a like 'xxx%'.
 */
public class Like extends StringRegexPredicate {
    public Like(Expression left, Expression right) {
        this(ImmutableList.of(left, right));
    }

    public Like(Expression left, Expression right, Expression escape) {
        this(ImmutableList.of(left, right, escape));
    }

    private Like(List<Expression> children) {
        this(children, false);
    }

    private Like(List<Expression> children, boolean inferred) {
        super("like", children, inferred);
    }

    @Override
    public Like withChildren(List<Expression> children) {
        return new Like(children);
    }

    public Optional<Expression> getEscape() {
        List<Expression> children = super.children;
        if (children.size() == 3) {
            return Optional.of(children.get(2));
        }
        return Optional.empty();
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        if (super.children.size() == 3) {
            return ImmutableList.of(
                FunctionSignature.ret(BooleanType.INSTANCE)
                    .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT)
            );
        }
        return super.getSignatures();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLike(this, context);
    }

    @Override
    public Expression withInferred(boolean inferred) {
        return new Like(this.children, inferred);
    }
}
