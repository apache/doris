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

import org.apache.doris.nereids.pattern.PatternMatcher;
import org.apache.doris.nereids.trees.TreeNode;

/**
 * Type of rules, each rule has its unique type.
 */
public enum RuleType {
    // binding rules
    BINDING_RELATION(RuleTypeClass.REWRITE),
    BINDING_PROJECT_SLOT(RuleTypeClass.REWRITE),
    BINDING_FILTER_SLOT(RuleTypeClass.REWRITE),
    BINDING_JOIN_SLOT(RuleTypeClass.REWRITE),
    BINDING_AGGREGATE_SLOT(RuleTypeClass.REWRITE),
    BINDING_SORT_SLOT(RuleTypeClass.REWRITE),
    BINDING_PROJECT_FUNCTION(RuleTypeClass.REWRITE),
    BINDING_AGGREGATE_FUNCTION(RuleTypeClass.REWRITE),
    RESOLVE_PROJECT_ALIAS(RuleTypeClass.REWRITE),
    RESOLVE_AGGREGATE_ALIAS(RuleTypeClass.REWRITE),
    PROJECT_TO_GLOBAL_AGGREGATE(RuleTypeClass.REWRITE),
    // rewrite rules
    AGGREGATE_DISASSEMBLE(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_PROJECTION(RuleTypeClass.REWRITE),
    // predicate push down rules
    PUSH_DOWN_PREDICATE_THROUGH_JOIN(RuleTypeClass.REWRITE),
    // column prune rules,
    COLUMN_PRUNE_AGGREGATION_CHILD(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_FILTER_CHILD(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_SORT_CHILD(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_JOIN_CHILD(RuleTypeClass.REWRITE),

    REWRITE_SENTINEL(RuleTypeClass.REWRITE),

    // exploration rules
    LOGICAL_JOIN_COMMUTATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_LEFT_JOIN_ASSOCIATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_L_ASSCOM(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_EXCHANGE(RuleTypeClass.EXPLORATION),

    // implementation rules
    LOGICAL_AGG_TO_PHYSICAL_HASH_AGG_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_JOIN_TO_HASH_JOIN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_PROJECT_TO_PHYSICAL_PROJECT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_FILTER_TO_PHYSICAL_FILTER_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_SORT_TO_PHYSICAL_HEAP_SORT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_OLAP_SCAN_TO_PHYSICAL_OLAP_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    IMPLEMENTATION_SENTINEL(RuleTypeClass.IMPLEMENTATION),

    // sentinel, use to count rules
    SENTINEL(RuleTypeClass.SENTINEL),
    ;

    private final RuleTypeClass ruleTypeClass;

    RuleType(RuleTypeClass ruleTypeClass) {
        this.ruleTypeClass = ruleTypeClass;
    }

    public int type() {
        return ordinal();
    }

    public RuleTypeClass getRuleTypeClass() {
        return ruleTypeClass;
    }

    public <INPUT_TYPE extends RULE_TYPE, OUTPUT_TYPE extends RULE_TYPE, RULE_TYPE extends TreeNode<RULE_TYPE>>
            Rule<RULE_TYPE> build(PatternMatcher<INPUT_TYPE, OUTPUT_TYPE, RULE_TYPE> patternMatcher) {
        return patternMatcher.toRule(this);
    }

    enum RuleTypeClass {
        REWRITE,
        EXPLORATION,
        IMPLEMENTATION,
        SENTINEL,
        ;
    }
}
