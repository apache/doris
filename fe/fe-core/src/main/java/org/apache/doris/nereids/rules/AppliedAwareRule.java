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

package org.apache.doris.nereids.rules;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.Pattern;
import org.apache.doris.nereids.pattern.ProxyPattern;
import org.apache.doris.nereids.trees.plans.Plan;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** AppliedAwareRule */
public class AppliedAwareRule extends Rule {
    private static final String APPLIED_RULES_KEY = "applied_rules";

    private static final Supplier<BitSet> CREATE_APPLIED_RULES = () -> new BitSet(RuleType.SENTINEL.ordinal());

    protected final Rule rule;
    protected final RuleType ruleType;
    protected int ruleTypeIndex;

    private AppliedAwareRule(Rule rule, BiPredicate<Rule, Plan> matchRootPredicate) {
        super(rule.getRuleType(),
                new ExtendPattern(rule.getPattern(), (Predicate<Plan>) (plan -> matchRootPredicate.test(rule, plan))),
                rule.getRulePromise());
        this.rule = rule;
        this.ruleType = rule.getRuleType();
        this.ruleTypeIndex = rule.getRuleType().ordinal();
    }

    @Override
    public List<Plan> transform(Plan plan, CascadesContext context) {
        return rule.transform(plan, context);
    }

    @Override
    public void acceptPlan(Plan plan) {
        BitSet appliedRules = plan.getOrInitMutableState(APPLIED_RULES_KEY, CREATE_APPLIED_RULES);
        appliedRules.set(ruleTypeIndex);
    }

    /**
     * AppliedAwareRuleCondition: convert one rule to AppliedAwareRule, so that the rule can add
     * some condition depends on whether this rule is applied to some plan
     */
    public static class AppliedAwareRuleCondition implements Function<Rule, Rule> {
        public Rule apply(Rule rule) {
            return new AppliedAwareRule(rule, this::condition);
        }

        /** provide this method for the child class get the applied state */
        public final boolean isAppliedRule(Rule rule, Plan plan) {
            Optional<BitSet> appliedRules = plan.getMutableState("applied_rules");
            return appliedRules.map(bitSet -> bitSet.get(rule.getRuleType().ordinal())).orElse(false);
        }

        /**
         * the default condition is whether this rule already applied to a plan,
         * this means one plan only apply for a rule only once. child class can
         * override this method.
         */
        protected boolean condition(Rule rule, Plan plan) {
            return isAppliedRule(rule, plan);
        }
    }

    private static class ExtendPattern<TYPE extends Plan> extends ProxyPattern<TYPE> {
        private final Predicate<Plan> matchRootPredicate;

        public ExtendPattern(Pattern pattern, Predicate<Plan> matchRootPredicate) {
            super(pattern);
            this.matchRootPredicate = Objects.requireNonNull(matchRootPredicate, "matchRootPredicate cannot be null");
        }

        @Override
        public boolean matchPlanTree(Plan plan) {
            return matchRootPredicate.test(plan) && super.matchPlanTree(plan);
        }

        @Override
        public boolean matchRoot(Plan plan) {
            return matchRootPredicate.test(plan) && super.matchRoot(plan);
        }
    }
}
