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

package org.apache.doris.nereids.rules.expression;

import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/** ExpressionPatternRuleFactory */
public interface ExpressionPatternRuleFactory {
    List<ExpressionPatternMatcher<? extends Expression>> buildRules();

    default <E extends Expression> ExpressionPatternDescriptor<E> matchesType(Class<E> clazz) {
        return new ExpressionPatternDescriptor<>(clazz);
    }

    default <E extends Expression> ExpressionPatternDescriptor<E> root(Class<E> clazz) {
        return new ExpressionPatternDescriptor<>(clazz)
                .whenCtx(ctx -> ctx.isRoot());
    }

    default <E extends Expression> ExpressionPatternDescriptor<E> matchesTopType(Class<E> clazz) {
        return new ExpressionPatternDescriptor<>(clazz)
                .whenCtx(ctx -> ctx.isRoot() || !clazz.isInstance(ctx.parent.get()));
    }

    /** ExpressionPatternDescriptor */
    class ExpressionPatternDescriptor<E extends Expression> {
        private final Class<E> typePattern;
        private final ImmutableList<Predicate<ExpressionMatchingContext<E>>> predicates;

        public ExpressionPatternDescriptor(Class<E> typePattern) {
            this(typePattern, ImmutableList.of());
        }

        public ExpressionPatternDescriptor(
                Class<E> typePattern, ImmutableList<Predicate<ExpressionMatchingContext<E>>> predicates) {
            this.typePattern = Objects.requireNonNull(typePattern, "typePattern can not be null");
            this.predicates = Objects.requireNonNull(predicates, "predicates can not be null");
        }

        public ExpressionPatternDescriptor<E> when(Predicate<E> predicate) {
            return whenCtx(ctx -> predicate.test(ctx.expr));
        }

        public ExpressionPatternDescriptor<E> whenCtx(Predicate<ExpressionMatchingContext<E>> predicate) {
            ImmutableList.Builder<Predicate<ExpressionMatchingContext<E>>> newPredicates
                    = ImmutableList.builderWithExpectedSize(predicates.size() + 1);
            newPredicates.addAll(predicates);
            newPredicates.add(predicate);
            return new ExpressionPatternDescriptor<>(typePattern, newPredicates.build());
        }

        /** then */
        public ExpressionPatternMatcher<E> then(Function<E, Expression> rewriter) {
            return new ExpressionPatternMatcher<>(
                    typePattern, predicates, (context) -> rewriter.apply(context.expr));
        }

        public ExpressionPatternMatcher<E> thenApply(ExpressionMatchingAction<E> action) {
            return new ExpressionPatternMatcher<>(typePattern, predicates, action);
        }
    }
}
