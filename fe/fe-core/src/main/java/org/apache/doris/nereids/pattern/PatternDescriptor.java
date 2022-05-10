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
import org.apache.doris.nereids.trees.TreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Define a descriptor to wrap a pattern tree to define a pattern shape.
 * It can support pattern generic type to MatchedAction.
 */
public class PatternDescriptor<INPUT_TYPE extends RULE_TYPE, RULE_TYPE extends TreeNode> {
    public final Pattern<INPUT_TYPE> pattern;
    public final RulePromise defaultPromise;
    public final List<Predicate<INPUT_TYPE>> predicates = new ArrayList<>();

    public PatternDescriptor(Pattern<INPUT_TYPE> pattern, RulePromise defaultPromise) {
        this.pattern = Objects.requireNonNull(pattern, "pattern can not be null");
        this.defaultPromise = Objects.requireNonNull(defaultPromise, "defaultPromise can not be null");
    }

    public PatternDescriptor<INPUT_TYPE, RULE_TYPE> when(Predicate<INPUT_TYPE> predicate) {
        predicates.add(predicate);
        return this;
    }

    public <OUTPUT_TYPE extends RULE_TYPE> PatternMatcher<INPUT_TYPE, OUTPUT_TYPE, RULE_TYPE> then(
            Function<INPUT_TYPE, OUTPUT_TYPE> matchedAction) {
        return new PatternMatcher<>(patternWithPredicates(), defaultPromise, ctx -> matchedAction.apply(ctx.root));
    }

    public <OUTPUT_TYPE extends RULE_TYPE> PatternMatcher<INPUT_TYPE, OUTPUT_TYPE, RULE_TYPE> thenApply(
            MatchedAction<INPUT_TYPE, OUTPUT_TYPE> matchedAction) {
        return new PatternMatcher<>(patternWithPredicates(), defaultPromise, matchedAction);
    }

    public Pattern<INPUT_TYPE> patternWithPredicates() {
        Pattern[] children = pattern.children().toArray(new Pattern[0]);
        return new Pattern<>(pattern.getNodeType(), predicates, children);
    }
}
