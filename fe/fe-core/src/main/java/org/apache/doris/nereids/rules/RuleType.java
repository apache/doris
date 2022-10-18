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
import org.apache.doris.nereids.trees.plans.Plan;

/**
 * Type of rules, each rule has its unique type.
 */
public enum RuleType {
    // just for UT
    TEST_REWRITE(RuleTypeClass.REWRITE),
    // binding rules

    // **** make sure BINDING_UNBOUND_LOGICAL_PLAN is the lowest priority in the rewrite rules. ****
    BINDING_NON_LEAF_LOGICAL_PLAN(RuleTypeClass.REWRITE),
    BINDING_ONE_ROW_RELATION_SLOT(RuleTypeClass.REWRITE),
    BINDING_RELATION(RuleTypeClass.REWRITE),
    BINDING_PROJECT_SLOT(RuleTypeClass.REWRITE),
    BINDING_FILTER_SLOT(RuleTypeClass.REWRITE),
    BINDING_JOIN_SLOT(RuleTypeClass.REWRITE),
    BINDING_AGGREGATE_SLOT(RuleTypeClass.REWRITE),
    BINDING_SORT_SLOT(RuleTypeClass.REWRITE),
    BINDING_LIMIT_SLOT(RuleTypeClass.REWRITE),
    BINDING_ONE_ROW_RELATION_FUNCTION(RuleTypeClass.REWRITE),
    BINDING_PROJECT_FUNCTION(RuleTypeClass.REWRITE),
    BINDING_AGGREGATE_FUNCTION(RuleTypeClass.REWRITE),
    BINDING_SUBQUERY_ALIAS_SLOT(RuleTypeClass.REWRITE),
    BINDING_FILTER_FUNCTION(RuleTypeClass.REWRITE),
    BINDING_HAVING_SLOT(RuleTypeClass.REWRITE),
    BINDING_HAVING_FUNCTION(RuleTypeClass.REWRITE),
    RESOLVE_HAVING(RuleTypeClass.REWRITE),

    RESOLVE_PROJECT_ALIAS(RuleTypeClass.REWRITE),
    RESOLVE_AGGREGATE_ALIAS(RuleTypeClass.REWRITE),
    PROJECT_TO_GLOBAL_AGGREGATE(RuleTypeClass.REWRITE),

    // check analysis rule
    CHECK_ANALYSIS(RuleTypeClass.CHECK),

    // rewrite rules
    NORMALIZE_AGGREGATE(RuleTypeClass.REWRITE),
    AGGREGATE_DISASSEMBLE(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_PROJECTION(RuleTypeClass.REWRITE),
    ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),

    PROJECT_ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),
    FILTER_ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),
    JOIN_ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),
    JOIN_LEFT_CHILD_ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),
    JOIN_RIGHT_CHILD_ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),
    AGGREGATE_ELIMINATE_ALIAS_NODE(RuleTypeClass.REWRITE),

    // subquery analyze
    ANALYZE_FILTER_SUBQUERY(RuleTypeClass.REWRITE),
    // subquery rewrite rule
    PUSH_APPLY_UNDER_PROJECT(RuleTypeClass.REWRITE),
    PUSH_APPLY_UNDER_FILTER(RuleTypeClass.REWRITE),
    APPLY_PULL_FILTER_ON_AGG(RuleTypeClass.REWRITE),
    APPLY_PULL_FILTER_ON_PROJECT_UNDER_AGG(RuleTypeClass.REWRITE),
    SCALAR_APPLY_TO_JOIN(RuleTypeClass.REWRITE),
    IN_APPLY_TO_JOIN(RuleTypeClass.REWRITE),
    EXISTS_APPLY_TO_JOIN(RuleTypeClass.REWRITE),
    // predicate push down rules
    PUSHDOWN_JOIN_OTHER_CONDITION(RuleTypeClass.REWRITE),
    PUSHDOWN_PREDICATE_THROUGH_AGGREGATION(RuleTypeClass.REWRITE),
    PUSHDOWN_EXPRESSIONS_IN_HASH_CONDITIONS(RuleTypeClass.REWRITE),
    // Pushdown filter
    PUSHDOWN_FILTER_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSHDOWN_FILTER_THROUGH_LEFT_SEMI_JOIN(RuleTypeClass.REWRITE),
    PUSH_FILTER_INSIDE_JOIN(RuleTypeClass.REWRITE),
    PUSHDOWN_FILTER_THROUGH_PROJET(RuleTypeClass.REWRITE),
    PUSHDOWN_PROJECT_THROUGHT_LIMIT(RuleTypeClass.REWRITE),
    // column prune rules,
    COLUMN_PRUNE_AGGREGATION_CHILD(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_FILTER_CHILD(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_SORT_CHILD(RuleTypeClass.REWRITE),
    COLUMN_PRUNE_JOIN_CHILD(RuleTypeClass.REWRITE),
    // expression of plan rewrite
    REWRITE_ONE_ROW_RELATION_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_PROJECT_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_AGG_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_FILTER_EXPRESSION(RuleTypeClass.REWRITE),
    REWRITE_JOIN_EXPRESSION(RuleTypeClass.REWRITE),
    REORDER_JOIN(RuleTypeClass.REWRITE),
    // Merge Consecutive plan
    MERGE_FILTERS(RuleTypeClass.REWRITE),
    MERGE_PROJECTS(RuleTypeClass.REWRITE),
    MERGE_LIMITS(RuleTypeClass.REWRITE),
    // Eliminate plan
    ELIMINATE_LIMIT(RuleTypeClass.REWRITE),
    ELIMINATE_FILTER(RuleTypeClass.REWRITE),
    ELIMINATE_OUTER(RuleTypeClass.REWRITE),
    FIND_HASH_CONDITION_FOR_JOIN(RuleTypeClass.REWRITE),
    ROLLUP_AGG_SCAN(RuleTypeClass.REWRITE),
    ROLLUP_AGG_FILTER_SCAN(RuleTypeClass.REWRITE),
    ROLLUP_AGG_PROJECT_SCAN(RuleTypeClass.REWRITE),
    ROLLUP_AGG_PROJECT_FILTER_SCAN(RuleTypeClass.REWRITE),
    ROLLUP_AGG_FILTER_PROJECT_SCAN(RuleTypeClass.REWRITE),
    ROLLUP_WITH_OUT_AGG(RuleTypeClass.REWRITE),
    OLAP_SCAN_PARTITION_PRUNE(RuleTypeClass.REWRITE),
    EXTRACT_SINGLE_TABLE_EXPRESSION_FROM_DISJUNCTION(RuleTypeClass.REWRITE),
    REWRITE_SENTINEL(RuleTypeClass.REWRITE),

    // limit push down
    PUSH_LIMIT_THROUGH_JOIN(RuleTypeClass.REWRITE),
    PUSH_LIMIT_THROUGH_PROJECT_JOIN(RuleTypeClass.REWRITE),

    // exploration rules
    TEST_EXPLORATION(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_COMMUTATE(RuleTypeClass.EXPLORATION),
    LOGICAL_LEFT_JOIN_ASSOCIATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_LASSCOM(RuleTypeClass.EXPLORATION),
    LOGICAL_INNER_JOIN_LASSCOM_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_OUTER_JOIN_LASSCOM(RuleTypeClass.EXPLORATION),
    LOGICAL_OUTER_JOIN_LASSCOM_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE(RuleTypeClass.EXPLORATION),
    LOGICAL_SEMI_JOIN_LOGICAL_JOIN_TRANSPOSE_PROJECT(RuleTypeClass.EXPLORATION),
    LOGICAL_SEMI_JOIN_SEMI_JOIN_TRANPOSE(RuleTypeClass.EXPLORATION),

    // implementation rules
    LOGICAL_ONE_ROW_RELATION_TO_PHYSICAL_ONE_ROW_RELATION(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_AGG_TO_PHYSICAL_HASH_AGG_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_JOIN_TO_HASH_JOIN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_JOIN_TO_NESTED_LOOP_JOIN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_PROJECT_TO_PHYSICAL_PROJECT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_FILTER_TO_PHYSICAL_FILTER_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_SORT_TO_PHYSICAL_QUICK_SORT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_TOP_N_TO_PHYSICAL_TOP_N_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_EMPTY_RELATION_TO_PHYSICAL_EMPTY_RELATION_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_LIMIT_TO_PHYSICAL_LIMIT_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_OLAP_SCAN_TO_PHYSICAL_OLAP_SCAN_RULE(RuleTypeClass.IMPLEMENTATION),
    LOGICAL_ASSERT_NUM_ROWS_TO_PHYSICAL_ASSERT_NUM_ROWS(RuleTypeClass.IMPLEMENTATION),
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

    public <INPUT_TYPE extends Plan, OUTPUT_TYPE extends Plan>
            Rule build(PatternMatcher<INPUT_TYPE, OUTPUT_TYPE> patternMatcher) {
        return patternMatcher.toRule(this);
    }

    enum RuleTypeClass {
        REWRITE,
        EXPLORATION,
        CHECK,
        IMPLEMENTATION,
        SENTINEL,
        ;
    }
}
