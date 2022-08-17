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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Define a descriptor to wrap a pattern tree to define a pattern shape.
 * It can support pattern generic type to MatchedAction.
 */
public class PatternDescriptor<INPUT_TYPE extends Plan> {
    public final Pattern<INPUT_TYPE> pattern;
    public final RulePromise defaultPromise;

    public PatternDescriptor(Pattern<INPUT_TYPE> pattern, RulePromise defaultPromise) {
        this.pattern = Objects.requireNonNull(pattern, "pattern can not be null");
        this.defaultPromise = Objects.requireNonNull(defaultPromise, "defaultPromise can not be null");
    }

    public PatternDescriptor<INPUT_TYPE> when(Predicate<INPUT_TYPE> predicate) {
        List<Predicate<INPUT_TYPE>> predicates = ImmutableList.<Predicate<INPUT_TYPE>>builder()
                .addAll(pattern.getPredicates())
                .add(predicate)
                .build();
        return new PatternDescriptor<>(pattern.withPredicates(predicates), defaultPromise);
    }

    public PatternDescriptor<INPUT_TYPE> whenNot(Predicate<INPUT_TYPE> predicate) {
        return when(predicate.negate());
    }

    public <OUTPUT_TYPE extends Plan> PatternMatcher<INPUT_TYPE, OUTPUT_TYPE> then(
            Function<INPUT_TYPE, OUTPUT_TYPE> matchedAction) {
        return new PatternMatcher<>(pattern, defaultPromise, ctx -> matchedAction.apply(ctx.root));
    }

    public <OUTPUT_TYPE extends Plan> PatternMatcher<INPUT_TYPE, OUTPUT_TYPE> thenApply(
            MatchedAction<INPUT_TYPE, OUTPUT_TYPE> matchedAction) {
        return new PatternMatcher<>(pattern, defaultPromise, matchedAction);
    }
}
