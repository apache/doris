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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * like expression: a like 'xxx%'.
 */
public class Like extends StringRegexPredicate {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(BooleanType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(BooleanType.INSTANCE).args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT,
                    VarcharType.SYSTEM_DEFAULT));

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
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public String computeToSql() {
        if (arity() == 2) {
            return super.computeToSql();
        }
        return '(' + left().toSql() + ' ' + getName() + ' ' + right().toSql() + " escape " + child(2).toSql()
                + ')';
    }

    @Override
    public String toString() {
        if (arity() == 2) {
            return super.computeToSql();
        }
        return "(" + left() + " " + getName() + " " + right() + " escape " + child(2)
                + ")";
    }

    @Override
    public Like withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2 || children.size() == 3);
        return new Like(children);
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitLike(this, context);
    }

    @Override
    public Expression withInferred(boolean inferred) {
        return new Like(this.children, inferred);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (arity() == 3) {
            if (child(2) instanceof StringLikeLiteral) {
                String escapeChar = ((StringLikeLiteral) child(2)).getStringValue();
                if (escapeChar.getBytes().length != 1) {
                    throw new AnalysisException(
                            "like escape character must be a single ascii character: " + escapeChar);
                }
            } else {
                throw new AnalysisException("like escape character must be a string literal: " + this.toSql());
            }
        }
    }
}
