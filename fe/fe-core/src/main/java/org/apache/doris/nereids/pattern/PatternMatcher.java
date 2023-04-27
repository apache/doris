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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RulePromise;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Define a class combine Pattern and MatchedAction.
 * It also Provided a function to convert to a rule.
 */
public class PatternMatcher<INPUT_TYPE extends Plan, OUTPUT_TYPE extends Plan> {

    public final Pattern<INPUT_TYPE> pattern;
    public final RulePromise defaultRulePromise;
    public final MatchedAction<INPUT_TYPE, OUTPUT_TYPE> matchedAction;
    public final MatchedMultiAction<INPUT_TYPE, OUTPUT_TYPE> matchedMultiAction;

    /**
     * PatternMatcher wrap a pattern, defaultRulePromise and matchedAction.
     *
     * @param pattern pattern
     * @param defaultRulePromise defaultRulePromise
     * @param matchedAction matched callback function
     */
    public PatternMatcher(Pattern<INPUT_TYPE> pattern, RulePromise defaultRulePromise,
            MatchedAction<INPUT_TYPE, OUTPUT_TYPE> matchedAction) {
        this.pattern = Objects.requireNonNull(pattern, "pattern can not be null");
        this.defaultRulePromise = Objects.requireNonNull(
                defaultRulePromise, "defaultRulePromise can not be null");
        this.matchedAction = Objects.requireNonNull(matchedAction, "matchedAction can not be null");
        this.matchedMultiAction = null;
    }

    public PatternMatcher(Pattern<INPUT_TYPE> pattern, RulePromise defaultRulePromise,
            MatchedMultiAction<INPUT_TYPE, OUTPUT_TYPE> matchedAction) {
        this.pattern = Objects.requireNonNull(pattern, "pattern can not be null");
        this.defaultRulePromise = Objects.requireNonNull(
                defaultRulePromise, "defaultRulePromise can not be null");
        this.matchedMultiAction = Objects.requireNonNull(matchedAction, "matchedMultiAction can not be null");
        this.matchedAction = null;
    }

    public Rule toRule(RuleType ruleType) {
        return toRule(ruleType, defaultRulePromise);
    }

    /**
     * convert current PatternMatcher to a rule.
     *
     * @param ruleType what type of the new rule?
     * @param rulePromise what priority of the new rule?
     * @return Rule
     */
    public Rule toRule(RuleType ruleType, RulePromise rulePromise) {
        return new Rule(ruleType, pattern, rulePromise) {
            @Override
            public List<Plan> transform(Plan originPlan, CascadesContext context) {
                if (matchedMultiAction != null) {
                    MatchingContext<INPUT_TYPE> matchingContext =
                            new MatchingContext<>((INPUT_TYPE) originPlan, pattern, context);
                    List<OUTPUT_TYPE> replacePlans = matchedMultiAction.apply(matchingContext);
                    return replacePlans == null || replacePlans.isEmpty()
                            ? ImmutableList.of(originPlan)
                            : ImmutableList.copyOf(replacePlans);
                } else {
                    MatchingContext<INPUT_TYPE> matchingContext =
                            new MatchingContext<>((INPUT_TYPE) originPlan, pattern, context);
                    OUTPUT_TYPE replacePlan = matchedAction.apply(matchingContext);
                    return ImmutableList.of(replacePlan == null ? originPlan : replacePlan);
                }
            }
        };
    }
}
