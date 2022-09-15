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

import org.apache.doris.nereids.rules.exploration.join.JoinCommute;
import org.apache.doris.nereids.rules.exploration.join.JoinCommuteProject;
import org.apache.doris.nereids.rules.exploration.join.JoinLAsscom;
import org.apache.doris.nereids.rules.exploration.join.JoinLAsscomProject;
import org.apache.doris.nereids.rules.implementation.LogicalAggToPhysicalHashAgg;
import org.apache.doris.nereids.rules.implementation.LogicalAssertNumRowsToPhysicalAssertNumRows;
import org.apache.doris.nereids.rules.implementation.LogicalEmptyRelationToPhysicalEmptyRelation;
import org.apache.doris.nereids.rules.implementation.LogicalFilterToPhysicalFilter;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToHashJoin;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToNestedLoopJoin;
import org.apache.doris.nereids.rules.implementation.LogicalLimitToPhysicalLimit;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalOneRowRelationToPhysicalOneRowRelation;
import org.apache.doris.nereids.rules.implementation.LogicalProjectToPhysicalProject;
import org.apache.doris.nereids.rules.implementation.LogicalSortToPhysicalQuickSort;
import org.apache.doris.nereids.rules.implementation.LogicalTopNToPhysicalTopN;
import org.apache.doris.nereids.rules.rewrite.logical.MergeConsecutiveFilters;
import org.apache.doris.nereids.rules.rewrite.logical.MergeConsecutiveLimits;
import org.apache.doris.nereids.rules.rewrite.logical.MergeConsecutiveProjects;
import org.apache.doris.nereids.rules.rewrite.logical.PushDownJoinOtherCondition;
import org.apache.doris.nereids.rules.rewrite.logical.PushPredicatesThroughJoin;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownProjectThroughLimit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/**
 * Containers for set of different type rules.
 */
public class RuleSet {
    public static final List<Rule> EXPLORATION_RULES = planRuleFactories()
            .add(JoinCommute.LEFT_DEEP)
            .add(JoinCommuteProject.LEFT_DEEP)
            .add(JoinLAsscom.INNER)
            .add(JoinLAsscomProject.INNER)
            .add(new PushdownFilterThroughProject())
            .add(new MergeConsecutiveProjects())
            .build();

    public static final List<RuleFactory> PUSH_DOWN_JOIN_CONDITION_RULES = ImmutableList.of(
            new PushDownJoinOtherCondition(),
            new PushPredicatesThroughJoin(),
            new PushdownProjectThroughLimit(),
            new PushdownFilterThroughProject(),
            new MergeConsecutiveProjects(),
            new MergeConsecutiveFilters(),
            new MergeConsecutiveLimits());

    public static final List<Rule> IMPLEMENTATION_RULES = planRuleFactories()
            .add(new LogicalAggToPhysicalHashAgg())
            .add(new LogicalFilterToPhysicalFilter())
            .add(new LogicalJoinToHashJoin())
            .add(new LogicalJoinToNestedLoopJoin())
            .add(new LogicalOlapScanToPhysicalOlapScan())
            .add(new LogicalProjectToPhysicalProject())
            .add(new LogicalLimitToPhysicalLimit())
            .add(new LogicalSortToPhysicalQuickSort())
            .add(new LogicalTopNToPhysicalTopN())
            .add(new LogicalAssertNumRowsToPhysicalAssertNumRows())
            .add(new LogicalOneRowRelationToPhysicalOneRowRelation())
            .add(new LogicalEmptyRelationToPhysicalEmptyRelation())
            .build();

    public static final List<Rule> LEFT_DEEP_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.LEFT_DEEP)
            .add(JoinLAsscom.INNER)
            .add(JoinLAsscomProject.INNER)
            .add(JoinLAsscom.OUTER)
            .add(JoinLAsscomProject.OUTER)
            // semi join Transpose ....
            .build();

    public static final List<Rule> ZIG_ZAG_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.ZIG_ZAG)
            .add(JoinLAsscom.INNER)
            .add(JoinLAsscomProject.INNER)
            .add(JoinLAsscom.OUTER)
            .add(JoinLAsscomProject.OUTER)
            // semi join Transpose ....
            .build();

    public static final List<Rule> BUSHY_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.BUSHY)
            // TODO: add more rule
            // .add(JoinLeftAssociate.INNER)
            // .add(JoinLeftAssociateProject.INNER)
            // .add(JoinRightAssociate.INNER)
            // .add(JoinRightAssociateProject.INNER)
            // .add(JoinExchange.INNER)
            // .add(JoinExchangeBothProject.INNER)
            // .add(JoinExchangeLeftProject.INNER)
            // .add(JoinExchangeRightProject.INNER)
            // .add(JoinRightAssociate.OUTER)
            .add(JoinLAsscom.OUTER)
            // semi join Transpose ....
            .build();

    public List<Rule> getExplorationRules() {
        return EXPLORATION_RULES;
    }

    public List<Rule> getImplementationRules() {
        return IMPLEMENTATION_RULES;
    }

    public static RuleFactories planRuleFactories() {
        return new RuleFactories();
    }

    /**
     * generate rule factories.
     */
    public static class RuleFactories {
        final Builder<Rule> rules = ImmutableList.builder();

        public RuleFactories add(RuleFactory ruleFactory) {
            rules.addAll(ruleFactory.buildRules());
            return this;
        }

        public List<Rule> build() {
            return rules.build();
        }
    }
}
