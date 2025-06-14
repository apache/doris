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
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.JsonType;
import org.apache.doris.nereids.types.VarcharType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * JsonSearch returns the json path pointing to a json string witch contains the search string.
 */
public class JsonSearch extends ScalarFunction implements ExplicitlyCastableSignature, AlwaysNullable {
    public static final List<FunctionSignature> SIGNATURES =
            ImmutableList.of(
                    FunctionSignature.ret(JsonType.INSTANCE)
                            .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT,
                                    VarcharType.SYSTEM_DEFAULT),
                    FunctionSignature.ret(JsonType.INSTANCE)
                            .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT,
                                    VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT),
                    FunctionSignature.ret(JsonType.INSTANCE)
                            .args(VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT,
                                    VarcharType.SYSTEM_DEFAULT, VarcharType.SYSTEM_DEFAULT,
                                    VarcharType.SYSTEM_DEFAULT));

    public JsonSearch(Expression arg0, Expression arg1, Expression arg2) {
        super("json_search", arg0, arg1, arg2);
    }

    public JsonSearch(Expression arg0, Expression arg1, Expression arg2, Expression arg3) {
        super("json_search", arg0, arg1, arg2, arg3);
    }

    public JsonSearch(Expression arg0, Expression arg1, Expression arg2, Expression arg3,
            Expression arg4) {
        super("json_search", arg0, arg1, arg2, arg3, arg4);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public JsonSearch withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3 || children.size() == 4 || children.size() == 5);
        if (children.size() == 3) {
            return new JsonSearch(children.get(0), children.get(1), children.get(2));
        } else if (children.size() == 4) {
            return new JsonSearch(children.get(0), children.get(1), children.get(2),
                    children.get(3));
        } else {
            return new JsonSearch(children.get(0), children.get(1), children.get(2),
                    children.get(3), children.get(4));
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (arity() < 3) {
            throw new AnalysisException(
                    "function json_search must contains at least three arguments");
        }
        if (children.size() > 3 && !child(3).isConstant()) {
            throw new AnalysisException(
                    "function json_search must accept constant arguments at 3rd argument(escape_char)");
        } else {
            final String escape_char = children.get(3).toString();
            // escape_char is \'escape\', so we need sub 2
            if (escape_char.length() - 2 > 1) {
                throw new AnalysisException(
                        String.format(
                                "function json_search's escape char [%s] with [%d] must contain at most one character",
                                escape_char, escape_char.length()));
            }
        }
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkLegalityAfterRewrite();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitJsonSearch(this, context);
    }
}
