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

import org.apache.doris.nereids.pattern.TypeMappings.TypeMapping;
import org.apache.doris.nereids.trees.expressions.Expression;

import java.util.List;
import java.util.function.Predicate;

/** ExpressionTraverseListener */
public class ExpressionTraverseListenerMapping implements TypeMapping<Expression> {
    public final Class<? extends Expression> typePattern;
    public final List<Predicate<ExpressionMatchingContext<Expression>>> predicates;
    public final ExpressionTraverseListener<Expression> listener;

    public ExpressionTraverseListenerMapping(ExpressionListenerMatcher listenerMatcher) {
        this.typePattern = listenerMatcher.typePattern;
        this.predicates = listenerMatcher.predicates;
        this.listener = listenerMatcher.listener;
    }

    @Override
    public Class<? extends Expression> getType() {
        return typePattern;
    }

    /** matches */
    public boolean matchesTypeAndPredicates(ExpressionMatchingContext<Expression> context) {
        return typePattern.isInstance(context.expr) && matchesPredicates(context);
    }

    /** matchesPredicates */
    public boolean matchesPredicates(ExpressionMatchingContext<Expression> context) {
        if (!predicates.isEmpty()) {
            for (Predicate<ExpressionMatchingContext<Expression>> predicate : predicates) {
                if (!predicate.test(context)) {
                    return false;
                }
            }
        }
        return true;
    }
}
