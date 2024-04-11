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
import java.util.function.Predicate;

/** ExpressionPattern */
public class ExpressionPatternMatcher<E extends Expression> {
    public final Class<E> typePattern;
    public final List<Predicate<ExpressionMatchingContext<E>>> predicates;
    public final ExpressionMatchingAction<E> matchingAction;

    public ExpressionPatternMatcher(Class<E> typePattern,
            List<Predicate<ExpressionMatchingContext<E>>> predicates,
            ExpressionMatchingAction<E> matchingAction) {
        this.typePattern = Objects.requireNonNull(typePattern, "typePattern can not be null");
        this.predicates = predicates == null ? ImmutableList.of() : predicates;
        this.matchingAction = Objects.requireNonNull(matchingAction, "matchingAction can not be null");
    }
}
