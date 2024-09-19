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

import org.apache.doris.nereids.rules.exploration.IntersectReorder;
import org.apache.doris.nereids.rules.exploration.MergeProjectsCBO;
import org.apache.doris.nereids.rules.exploration.TransposeAggSemiJoinProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLAsscomProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinLeftAssociateProject;
import org.apache.doris.nereids.rules.exploration.join.InnerJoinRightAssociateProject;
import org.apache.doris.nereids.rules.exploration.join.JoinCommute;
import org.apache.doris.nereids.rules.exploration.join.JoinExchangeBothProject;
import org.apache.doris.nereids.rules.exploration.join.LogicalJoinSemiJoinTransposeProject;
import org.apache.doris.nereids.rules.exploration.join.OuterJoinAssocProject;
import org.apache.doris.nereids.rules.exploration.join.OuterJoinLAsscomProject;
import org.apache.doris.nereids.rules.exploration.join.PushDownProjectThroughInnerOuterJoin;
import org.apache.doris.nereids.rules.exploration.join.PushDownProjectThroughSemiJoin;
import org.apache.doris.nereids.rules.exploration.join.SemiJoinSemiJoinTransposeProject;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewAggregateOnNoneAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterProjectAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterProjectJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterProjectScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewFilterScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewOnlyJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewOnlyScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterAggregateRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectFilterScanRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectJoinRule;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewProjectScanRule;
import org.apache.doris.nereids.rules.expression.ExpressionNormalization;
import org.apache.doris.nereids.rules.expression.ExpressionOptimization;
import org.apache.doris.nereids.rules.implementation.AggregateStrategies;
import org.apache.doris.nereids.rules.implementation.LogicalAssertNumRowsToPhysicalAssertNumRows;
import org.apache.doris.nereids.rules.implementation.LogicalCTEAnchorToPhysicalCTEAnchor;
import org.apache.doris.nereids.rules.implementation.LogicalCTEConsumerToPhysicalCTEConsumer;
import org.apache.doris.nereids.rules.implementation.LogicalCTEProducerToPhysicalCTEProducer;
import org.apache.doris.nereids.rules.implementation.LogicalDeferMaterializeOlapScanToPhysicalDeferMaterializeOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalDeferMaterializeResultSinkToPhysicalDeferMaterializeResultSink;
import org.apache.doris.nereids.rules.implementation.LogicalDeferMaterializeTopNToPhysicalDeferMaterializeTopN;
import org.apache.doris.nereids.rules.implementation.LogicalEmptyRelationToPhysicalEmptyRelation;
import org.apache.doris.nereids.rules.implementation.LogicalEsScanToPhysicalEsScan;
import org.apache.doris.nereids.rules.implementation.LogicalExceptToPhysicalExcept;
import org.apache.doris.nereids.rules.implementation.LogicalFileScanToPhysicalFileScan;
import org.apache.doris.nereids.rules.implementation.LogicalFileSinkToPhysicalFileSink;
import org.apache.doris.nereids.rules.implementation.LogicalFilterToPhysicalFilter;
import org.apache.doris.nereids.rules.implementation.LogicalGenerateToPhysicalGenerate;
import org.apache.doris.nereids.rules.implementation.LogicalHiveTableSinkToPhysicalHiveTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalHudiScanToPhysicalHudiScan;
import org.apache.doris.nereids.rules.implementation.LogicalIcebergTableSinkToPhysicalIcebergTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalIntersectToPhysicalIntersect;
import org.apache.doris.nereids.rules.implementation.LogicalJdbcScanToPhysicalJdbcScan;
import org.apache.doris.nereids.rules.implementation.LogicalJdbcTableSinkToPhysicalJdbcTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToHashJoin;
import org.apache.doris.nereids.rules.implementation.LogicalJoinToNestedLoopJoin;
import org.apache.doris.nereids.rules.implementation.LogicalLimitToPhysicalLimit;
import org.apache.doris.nereids.rules.implementation.LogicalOdbcScanToPhysicalOdbcScan;
import org.apache.doris.nereids.rules.implementation.LogicalOlapScanToPhysicalOlapScan;
import org.apache.doris.nereids.rules.implementation.LogicalOlapTableSinkToPhysicalOlapTableSink;
import org.apache.doris.nereids.rules.implementation.LogicalOneRowRelationToPhysicalOneRowRelation;
import org.apache.doris.nereids.rules.implementation.LogicalPartitionTopNToPhysicalPartitionTopN;
import org.apache.doris.nereids.rules.implementation.LogicalProjectToPhysicalProject;
import org.apache.doris.nereids.rules.implementation.LogicalRepeatToPhysicalRepeat;
import org.apache.doris.nereids.rules.implementation.LogicalResultSinkToPhysicalResultSink;
import org.apache.doris.nereids.rules.implementation.LogicalSchemaScanToPhysicalSchemaScan;
import org.apache.doris.nereids.rules.implementation.LogicalSortToPhysicalQuickSort;
import org.apache.doris.nereids.rules.implementation.LogicalTVFRelationToPhysicalTVFRelation;
import org.apache.doris.nereids.rules.implementation.LogicalTopNToPhysicalTopN;
import org.apache.doris.nereids.rules.implementation.LogicalUnionToPhysicalUnion;
import org.apache.doris.nereids.rules.implementation.LogicalWindowToPhysicalWindow;
import org.apache.doris.nereids.rules.rewrite.ConvertOuterJoinToAntiJoin;
import org.apache.doris.nereids.rules.rewrite.CreatePartitionTopNFromWindow;
import org.apache.doris.nereids.rules.rewrite.EliminateFilter;
import org.apache.doris.nereids.rules.rewrite.EliminateOuterJoin;
import org.apache.doris.nereids.rules.rewrite.MaxMinFilterPushDown;
import org.apache.doris.nereids.rules.rewrite.MergeFilters;
import org.apache.doris.nereids.rules.rewrite.MergeGenerates;
import org.apache.doris.nereids.rules.rewrite.MergeLimits;
import org.apache.doris.nereids.rules.rewrite.MergeProjects;
import org.apache.doris.nereids.rules.rewrite.PushDownAliasThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownExpressionsInHashCondition;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughAggregation;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughGenerate;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughJoin;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughPartitionTopN;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughProject;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughRepeat;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughSetOperation;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughSort;
import org.apache.doris.nereids.rules.rewrite.PushDownFilterThroughWindow;
import org.apache.doris.nereids.rules.rewrite.PushDownJoinOtherCondition;
import org.apache.doris.nereids.rules.rewrite.PushDownProjectThroughLimit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import java.util.List;

/**
 * Containers for set of different type rules.
 */
public class RuleSet {

    public static final List<Rule> EXPLORATION_RULES = planRuleFactories()
            .add(new MergeProjectsCBO())
            .add(IntersectReorder.INSTANCE)
            .build();

    public static final List<Rule> OTHER_REORDER_RULES = planRuleFactories()
            .addAll(EXPLORATION_RULES)
            .add(OuterJoinLAsscomProject.INSTANCE)
            .add(SemiJoinSemiJoinTransposeProject.INSTANCE)
            .add(LogicalJoinSemiJoinTransposeProject.INSTANCE)
            .add(PushDownProjectThroughInnerOuterJoin.INSTANCE)
            .add(PushDownProjectThroughSemiJoin.INSTANCE)
            .add(TransposeAggSemiJoinProject.INSTANCE)
            .build();

    public static final List<RuleFactory> PUSH_DOWN_FILTERS = ImmutableList.of(
            new MaxMinFilterPushDown(),
            new CreatePartitionTopNFromWindow(),
            new PushDownFilterThroughProject(),
            new PushDownFilterThroughSort(),
            new PushDownJoinOtherCondition(),
            new PushDownFilterThroughJoin(),
            new PushDownExpressionsInHashCondition(),
            new PushDownFilterThroughAggregation(),
            new PushDownFilterThroughRepeat(),
            new PushDownFilterThroughSetOperation(),
            new PushDownFilterThroughGenerate(),
            new PushDownProjectThroughLimit(),
            new EliminateOuterJoin(),
            new ConvertOuterJoinToAntiJoin(),
            new MergeProjects(),
            new MergeFilters(),
            new MergeGenerates(),
            new MergeLimits(),
            new PushDownAliasThroughJoin(),
            new PushDownFilterThroughWindow(),
            new PushDownFilterThroughPartitionTopN(),
            new ExpressionOptimization(),
            // some useless predicates(e.g. 1=1) can be inferred by InferPredicates,
            // the FoldConstantRule in ExpressionNormalization can fold 1=1 to true
            // and EliminateFilter can eliminate the useless filter
            new ExpressionNormalization(),
            new EliminateFilter()
    );

    public static final List<Rule> IMPLEMENTATION_RULES = planRuleFactories()
            .add(new LogicalCTEProducerToPhysicalCTEProducer())
            .add(new LogicalCTEConsumerToPhysicalCTEConsumer())
            .add(new LogicalCTEAnchorToPhysicalCTEAnchor())
            .add(new LogicalRepeatToPhysicalRepeat())
            .add(new LogicalFilterToPhysicalFilter())
            .add(new LogicalJoinToHashJoin())
            .add(new LogicalJoinToNestedLoopJoin())
            .add(new LogicalOlapScanToPhysicalOlapScan())
            .add(new LogicalDeferMaterializeOlapScanToPhysicalDeferMaterializeOlapScan())
            .add(new LogicalSchemaScanToPhysicalSchemaScan())
            .add(new LogicalHudiScanToPhysicalHudiScan())
            .add(new LogicalFileScanToPhysicalFileScan())
            .add(new LogicalJdbcScanToPhysicalJdbcScan())
            .add(new LogicalOdbcScanToPhysicalOdbcScan())
            .add(new LogicalEsScanToPhysicalEsScan())
            .add(new LogicalProjectToPhysicalProject())
            .add(new LogicalLimitToPhysicalLimit())
            .add(new LogicalWindowToPhysicalWindow())
            .add(new LogicalSortToPhysicalQuickSort())
            .add(new LogicalTopNToPhysicalTopN())
            .add(new LogicalDeferMaterializeTopNToPhysicalDeferMaterializeTopN())
            .add(new LogicalPartitionTopNToPhysicalPartitionTopN())
            .add(new LogicalAssertNumRowsToPhysicalAssertNumRows())
            .add(new LogicalOneRowRelationToPhysicalOneRowRelation())
            .add(new LogicalEmptyRelationToPhysicalEmptyRelation())
            .add(new LogicalTVFRelationToPhysicalTVFRelation())
            .add(new AggregateStrategies())
            .add(new LogicalUnionToPhysicalUnion())
            .add(new LogicalExceptToPhysicalExcept())
            .add(new LogicalIntersectToPhysicalIntersect())
            .add(new LogicalGenerateToPhysicalGenerate())
            .add(new LogicalOlapTableSinkToPhysicalOlapTableSink())
            .add(new LogicalHiveTableSinkToPhysicalHiveTableSink())
            .add(new LogicalIcebergTableSinkToPhysicalIcebergTableSink())
            .add(new LogicalJdbcTableSinkToPhysicalJdbcTableSink())
            .add(new LogicalFileSinkToPhysicalFileSink())
            .add(new LogicalResultSinkToPhysicalResultSink())
            .add(new LogicalDeferMaterializeResultSinkToPhysicalDeferMaterializeResultSink())
            .build();

    // left-zig-zag tree is used when column stats are not available.
    public static final List<Rule> LEFT_ZIG_ZAG_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.LEFT_ZIG_ZAG)
            .add(InnerJoinLAsscomProject.LEFT_ZIG_ZAG)
            .addAll(OTHER_REORDER_RULES)
            .build();

    public static final List<Rule> ZIG_ZAG_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.ZIG_ZAG)
            .add(InnerJoinLAsscomProject.INSTANCE)
            .build();

    public static final List<Rule> BUSHY_TREE_JOIN_REORDER = planRuleFactories()
            .add(JoinCommute.BUSHY)
            .add(InnerJoinLeftAssociateProject.INSTANCE)
            .add(InnerJoinRightAssociateProject.INSTANCE)
            .add(JoinExchangeBothProject.INSTANCE)
            .add(OuterJoinAssocProject.INSTANCE)
            .build();

    public static final List<Rule> ZIG_ZAG_TREE_JOIN_REORDER_RULES = ImmutableList.<Rule>builder()
            .addAll(ZIG_ZAG_TREE_JOIN_REORDER)
            .addAll(OTHER_REORDER_RULES)
            .build();

    public static final List<Rule> BUSHY_TREE_JOIN_REORDER_RULES = ImmutableList.<Rule>builder()
            .addAll(BUSHY_TREE_JOIN_REORDER)
            .addAll(OTHER_REORDER_RULES)
            .build();

    public static final List<Rule> MATERIALIZED_VIEW_RULES = planRuleFactories()
            .add(MaterializedViewOnlyJoinRule.INSTANCE)
            .add(MaterializedViewProjectJoinRule.INSTANCE)
            .add(MaterializedViewFilterJoinRule.INSTANCE)
            .add(MaterializedViewFilterProjectJoinRule.INSTANCE)
            .add(MaterializedViewProjectFilterJoinRule.INSTANCE)
            .add(MaterializedViewAggregateRule.INSTANCE)
            .add(MaterializedViewProjectAggregateRule.INSTANCE)
            .add(MaterializedViewFilterAggregateRule.INSTANCE)
            .add(MaterializedViewProjectFilterAggregateRule.INSTANCE)
            .add(MaterializedViewFilterProjectAggregateRule.INSTANCE)
            .add(MaterializedViewFilterScanRule.INSTANCE)
            .add(MaterializedViewFilterProjectScanRule.INSTANCE)
            .add(MaterializedViewProjectScanRule.INSTANCE)
            .add(MaterializedViewProjectFilterScanRule.INSTANCE)
            .add(MaterializedViewAggregateOnNoneAggregateRule.INSTANCE)
            .add(MaterializedViewOnlyScanRule.INSTANCE)
            .build();

    public static final List<Rule> DPHYP_REORDER_RULES = ImmutableList.<Rule>builder()
            .add(JoinCommute.BUSHY.build())
            .build();

    public List<Rule> getDPHypReorderRules() {
        return DPHYP_REORDER_RULES;
    }

    public List<Rule> getZigZagTreeJoinReorder() {
        return ZIG_ZAG_TREE_JOIN_REORDER_RULES;
    }

    public List<Rule> getLeftZigZagTreeJoinReorder() {
        return LEFT_ZIG_ZAG_TREE_JOIN_REORDER;
    }

    public List<Rule> getBushyTreeJoinReorder() {
        return BUSHY_TREE_JOIN_REORDER_RULES;
    }

    public List<Rule> getImplementationRules() {
        return IMPLEMENTATION_RULES;
    }

    public List<Rule> getMaterializedViewRules() {
        return MATERIALIZED_VIEW_RULES;
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
