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

import org.apache.doris.nereids.rules.exploration.join.InnerJoinLAsscom;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLAsscomProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLeftAssociate;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLeftAssociateProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinRightAssociate;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinRightAssociateProject;
import org.apache.doris.nereids.rules.exploration.join.JoinCommute;
import org.apache.doris.nereids.rules.exploration.join.JoinExchange;
import org.apache.doris.nereids.rules.exploration.join.JoinExchangeBothProject;
import org.apache.doris.nereids.rules.exploration.join.LogicalJoinSemiJoinTranspose;
import org.apache.doris.nereids.rules.exploration.join.LogicalJoinSemiJoinTransposeProject;
import org.apache.doris.nereids.rules.exploration.join.OuterJoinLAsscom;
import org.apache.doris.nereids.rules.exploration.join.OuterJoinLAsscomProject;
import org.apache.doris.nereids.rules.exploration.join.PushdownProjectThroughInnerJoin;
import org.apache.doris.nereids.rules.exploration.join.PushdownProjectThroughSemiJoin;
import org.apache.doris.nereids.rules.exploration.join.SemiJoinSemiJoinTranspose;
import org.apache.doris.nereids.rules.exploration.join.SemiJoinSemiJoinTransposeProject;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.rules.implementation.LogicalAssertNumRowsToPhysicalAssertNumRows;
import org.apache.doris.nereids.rules.implementation.LogicalEmptyRelationToPhysicalEmptyRelation;
import org.apache.doris.nereids.rules.implementation.LogicalEsScanToPhysicalEsScan;
import org.apache.doris.nereids.rules.implementation.LogicalExceptToPhysicalExcept;
import org.apache.doris.nereids.rules.implementation.LogicalFileScanToPhysicalFileScan;
import org.apache.doris.nereids.rules.implementation.LogicalFilterToPhysicalFilter;
import org.apache.doris.nereids.rules.implementation.LogicalGenerateToPhysicalGenerate;
import org.apache.doris.nereids.rules.implementation.LogicalIntersectToPhysicalIntersect;
import org.apache.doris.nereids.rules.implementation.LogicalJdbcScanToPhysicalJdbcScan;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToHashJoin;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToNestedLoopJoin;
import org.apache.doris.nereids.rules.implementation.LogicalLimitToPhysicalLimit;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalOneRowRelationToPhysicalOneRowRelation;
import org.apache.doris.nereids.rules.implementation.LogicalProjectToPhysicalProject;
import org.apache.doris.nereids.rules.implementation.LogicalRepeatToPhysicalRepeat;
import org.apache.doris.nereids.rules.implementation.LogicalSchemaScanToPhysicalSchemaScan;
import org.apache.doris.nereids.rules.implementation.LogicalSortToPhysicalQuickSort;
import org.apache.doris.nereids.rules.implementation.LogicalTVFRelationToPhysicalTVFRelation;
import org.apache.doris.nereids.rules.implementation.LogicalTopNToPhysicalTopN;
import org.apache.doris.nereids.rules.implementation.LogicalUnionToPhysicalUnion;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow;
import org.apache.doris.nereids.rules.rewrite.logical.EliminateOuterJoin;
import org.apache.doris.nereids.rules.rewrite.logical.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.logical.MergeGenerates;
import org.apache.doris.nereids.rules.rewrite.logical.MergeLimits;
import org.apache.doris.nereids.rules.rewrite.logical.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownAliasThroughJoin;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownExpressionsInHashCondition;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughAggregation;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughJoin;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughRepeat;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownFilterThroughSetOperation;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownJoinOtherCondition;
import org.apache.doris.nereids.rules.rewrite.logical.PushdownProjectThroughLimit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.ArrayList;
import java.util.List;

/**
 * Containers for set of different type rules.
 */
public class RuleSet {
    public static final List<Rule> EXPLORATION_RULES = planRuleFactories()
            .add(new PushdownFilterThroughProject())
            .add(new MergeProjects())
            .build();

    public static final List<Rule> OTHER_REORDER_RULES = planRuleFactories()
            .add(OuterJoinLAsscom.INSTANCE)
            .add(OuterJoinLAsscomProject.INSTANCE)
            .add(SemiJoinSemiJoinTranspose.INSTANCE)
            .add(SemiJoinSemiJoinTransposeProject.INSTANCE)
            .add(LogicalJoinSemiJoinTranspose.INSTANCE)
            .add(LogicalJoinSemiJoinTransposeProject.INSTANCE)
            .add(PushdownProjectThroughInnerJoin.INSTANCE)
            .add(PushdownProjectThroughSemiJoin.INSTANCE)
            .build();

    public static final List<RuleFactory> PUSH_DOWN_FILTERS = ImmutableList.of(
            new PushdownFilterThroughProject(),
            new PushdownJoinOtherCondition(),
            new PushdownFilterThroughJoin(),
            new PushdownExpressionsInHashCondition(),
            new PushdownFilterThroughAggregation(),
            new PushdownFilterThroughRepeat(),
            new PushdownFilterThroughSetOperation(),
            new PushdownProjectThroughLimit(),
            new PushdownAliasThroughJoin(),
            new EliminateOuterJoin(),
            new MergeProjects(),
            new MergeFilters(),
            new MergeGenerates(),
            new MergeLimits());

    public static final List<Rule> IMPLEMENTATION_RULES = planRuleFactories()
            .add(new LogicalRepeatToPhysicalRepeat())
            .add(new LogicalFilterToPhysicalFilter())
            .add(new LogicalJoinToHashJoin())
            .add(new LogicalJoinToNestedLoopJoin())
            .add(new LogicalOlapScanToPhysicalOlapScan())
            .add(new LogicalSchemaScanToPhysicalSchemaScan())
            .add(new LogicalFileScanToPhysicalFileScan())
            .add(new LogicalJdbcScanToPhysicalJdbcScan())
            .add(new LogicalEsScanToPhysicalEsScan())
            .add(new LogicalProjectToPhysicalProject())
            .add(new LogicalLimitToPhysicalLimit())
            .add(new LogicalWindowToPhysicalWindow())
            .add(new LogicalSortToPhysicalQuickSort())
            .add(new LogicalTopNToPhysicalTopN())
            .add(new LogicalAssertNumRowsToPhysicalAssertNumRows())
            .add(new LogicalOneRowRelationToPhysicalOneRowRelation())
            .add(new LogicalEmptyRelationToPhysicalEmptyRelation())
            .add(new LogicalTVFRelationToPhysicalTVFRelation())
            .add(new AggregateStrategies())
            .add(new LogicalUnionToPhysicalUnion())
            .add(new LogicalExceptToPhysicalExcept())
            .add(new LogicalIntersectToPhysicalIntersect())
            .add(new LogicalGenerateToPhysicalGenerate())
            .build();

    public static final List<Rule> ZIG_ZAG_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.ZIG_ZAG)
            .add(InnerJoinLAsscom.INSTANCE)
            .add(InnerJoinLAsscomProject.INSTANCE)
            .build();

    public static final List<Rule> BUSHY_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.BUSHY)
            .add(InnerJoinLAsscom.INSTANCE)
            .add(InnerJoinLAsscomProject.INSTANCE)
            .add(InnerJoinLeftAssociate.INSTANCE)
            .add(InnerJoinLeftAssociateProject.INSTANCE)
            .add(InnerJoinRightAssociate.INSTANCE)
            .add(InnerJoinRightAssociateProject.INSTANCE)
            .add(JoinExchange.INSTANCE)
            .add(JoinExchangeBothProject.INSTANCE)
            .build();

    public List<Rule> getZigZagTreeJoinReorder() {
        List<Rule> rules = new ArrayList<>();
        rules.addAll(ZIG_ZAG_TREE_JOIN_REORDER);
        rules.addAll(OTHER_REORDER_RULES);
        rules.addAll(EXPLORATION_RULES);
        return rules;
    }

    public List<Rule> getBushyTreeJoinReorder() {
        List<Rule> rules = new ArrayList<>();
        rules.addAll(BUSHY_TREE_JOIN_REORDER);
        rules.addAll(OTHER_REORDER_RULES);
        rules.addAll(EXPLORATION_RULES);
        return rules;
    }

    public List<Rule> getExplorationRulesWithoutReorder() {
        return new ArrayList<>(EXPLORATION_RULES);
    }

    public List<Rule> getJoinOrderRule() {
        return BUSHY_TREE_JOIN_REORDER;
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

        public RuleFactories addAll(List<Rule> rules) {
            this.rules.addAll(rules);
            return this;
        }

        public List<Rule> build() {
            return rules.build();
        }
    }
}
