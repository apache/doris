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

package org.apache.doris.nereids.rules.expression.rewrite.rules;

import org.apache.doris.nereids.rules.expression.rewrite.AbstractExpressionRewriteRule;
import org.apache.doris.nereids.rules.expression.rewrite.ExpressionRewriteContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DecimalType;
import org.apache.doris.nereids.types.NumericType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.CharacterType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * a rule to add implicit cast for expressions.
 */
public class TypeCoercion extends AbstractExpressionRewriteRule {

    public static final TypeCoercion INSTANCE = new TypeCoercion();

    @Override
    public Expression rewrite(Expression expr, ExpressionRewriteContext ctx) {
        if (expr instanceof ImplicitCastInputTypes) {
            return visitImplicitCastInputTypes(expr, ctx);
        } else {
            return super.rewrite(expr, ctx);
        }
    }

    // TODO: add other expression visitor function to do type coercion.

    /**
     * Do implicit cast for expression's children.
     */
    private Expression visitImplicitCastInputTypes(Expression expr, ExpressionRewriteContext ctx) {
        ImplicitCastInputTypes implicitCastInputTypes = (ImplicitCastInputTypes) expr;
        List<Expression> newChildren = Lists.newArrayListWithCapacity(expr.arity());
        AtomicInteger changed = new AtomicInteger(0);
        for (int i = 0; i < implicitCastInputTypes.expectedInputTypes().size(); i++) {
            newChildren.add(implicitCast(expr.child(i), implicitCastInputTypes.expectedInputTypes().get(i), ctx)
                    .map(e -> {
                        changed.incrementAndGet();
                        return e;
                    })
                    .orElse(expr.child(0))
            );
        }
        if (changed.get() != 0) {
            return expr.withChildren(newChildren);
        } else {
            return expr;
        }
    }

    /**
     * Return Optional.empty() if we cannot do or do not need to do implicit cast.
     */
    private Optional<Expression> implicitCast(Expression input, AbstractDataType expected,
            ExpressionRewriteContext ctx) {
        Expression rewrittenInput = rewrite(input, ctx);
        Optional<DataType> castDataType = implicitCast(rewrittenInput.getDataType(), expected);
        if (castDataType.isPresent() && !castDataType.get().equals(rewrittenInput.getDataType())) {
            return Optional.of(new Cast(rewrittenInput, castDataType.get()));
        } else {
            // TODO: there maybe has performance problem, we need use ctx to save whether children is changed.
            if (rewrittenInput.equals(input)) {
                return Optional.empty();
            } else {
                return Optional.of(rewrittenInput);
            }
        }
    }

    /**
     * Return Optional.empty() if cannot do implicit cast.
     * TODO: datetime and date type
     */
    private Optional<DataType> implicitCast(DataType input, AbstractDataType expected) {
        DataType returnType = null;
        if (expected.acceptsType(input)) {
            // If the expected type is already a parent of the input type, no need to cast.
            return Optional.of(input);
        }
        if (expected instanceof TypeCollection) {
            TypeCollection typeCollection = (TypeCollection) expected;
            return typeCollection.getTypes().stream()
                    .map(e -> implicitCast(input, e))
                    .findFirst()
                    .orElse(Optional.empty());
        }
        if (input.isNullType()) {
            // Cast null type (usually from null literals) into target types
            returnType = expected.defaultConcreteType();
        } else if (input.isNumericType() && expected instanceof DecimalType) {
            // If input is a numeric type but not decimal, and we expect a decimal type,
            // cast the input to decimal.
            returnType = DecimalType.forType(input);
        } else if (input.isNumericType() && expected instanceof NumericType) {
            // For any other numeric types, implicitly cast to each other, e.g. bigint -> int, int -> bigint
            returnType = (DataType) expected;
        } else if (input.isStringType()) {
            if (expected instanceof DecimalType) {
                returnType = DecimalType.SYSTEM_DEFAULT;
            } else if (expected instanceof NumericType) {
                returnType = expected.defaultConcreteType();
            }
        } else if (input.isPrimitive() && !input.isStringType() && expected instanceof CharacterType) {
            returnType = StringType.INSTANCE;
        }

        // could not do implicit cast, just return null. Throw exception in check analysis.
        return Optional.ofNullable(returnType);
    }
}
