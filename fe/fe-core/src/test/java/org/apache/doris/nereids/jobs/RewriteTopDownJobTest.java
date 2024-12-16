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

package org.apache.doris.nereids.jobs;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

public class RewriteTopDownJobTest {
    public static class FakeRule implements RewriteRuleFactory {
        @Override
        public List<Rule> buildRules() {
            return ImmutableList.of(
                    RuleType.BINDING_RELATION.build(
                            unboundRelation().then(unboundRelation ->
                                    new LogicalBoundRelation(
                                            PlanConstructor.newOlapTable(0L, "test", 0),
                                            Lists.newArrayList("test"))
                            )
                    ),
                    RuleType.BINDING_PROJECT_SLOT.build(
                            logicalProject()
                                    .when(Plan::canBind)
                                    .then(LogicalPlan::recomputeLogicalProperties)
                    )
            );
        }
    }

    @Test
    public void testSimplestScene() {
        Plan leaf = new UnboundRelation(StatementScopeIdGenerator.newRelationId(), Lists.newArrayList("test"));
        LogicalProject<Plan> project = new LogicalProject<>(ImmutableList.of(
                new SlotReference("name", StringType.INSTANCE, true, ImmutableList.of("test"))),
                leaf
        );
        PlanChecker.from(MemoTestUtils.createConnectContext(), project)
                .applyBottomUp(new FakeRule())
                .checkMemo(memo -> {
                    Group rootGroup = memo.getRoot();
                    Assertions.assertEquals(1, rootGroup.getLogicalExpressions().size());
                    GroupExpression rootGroupExpression = rootGroup.getLogicalExpression();
                    List<Slot> output = rootGroup.getLogicalProperties().getOutput();
                    Assertions.assertEquals(1, output.size());
                    Assertions.assertEquals("name", output.get(0).getName());
                    Assertions.assertEquals(StringType.INSTANCE, output.get(0).getDataType());
                    Assertions.assertEquals(1, rootGroupExpression.children().size());
                    Assertions.assertEquals(PlanType.LOGICAL_PROJECT, rootGroupExpression.getPlan().getType());

                    Group leafGroup = rootGroupExpression.child(0);
                    output = leafGroup.getLogicalProperties().getOutput();
                    Assertions.assertEquals(2, output.size());
                    Assertions.assertEquals("id", output.get(0).getName());
                    Assertions.assertEquals(IntegerType.INSTANCE, output.get(0).getDataType());
                    Assertions.assertEquals("name", output.get(1).getName());
                    Assertions.assertEquals(StringType.INSTANCE, output.get(1).getDataType());
                    Assertions.assertEquals(1, leafGroup.getLogicalExpressions().size());
                    GroupExpression leafGroupExpression = leafGroup.getLogicalExpression();
                    Assertions.assertEquals(PlanType.LOGICAL_BOUND_RELATION, leafGroupExpression.getPlan().getType());
                });
    }

    private static class LogicalBoundRelation extends LogicalCatalogRelation {

        public LogicalBoundRelation(TableIf table, List<String> qualifier) {
            super(StatementScopeIdGenerator.newRelationId(), PlanType.LOGICAL_BOUND_RELATION, table, qualifier);
        }

        public LogicalBoundRelation(TableIf table, List<String> qualifier, Optional<GroupExpression> groupExpression,
                Optional<LogicalProperties> logicalProperties) {
            super(StatementScopeIdGenerator.newRelationId(), PlanType.LOGICAL_BOUND_RELATION, table, qualifier,
                    groupExpression, logicalProperties);
        }

        @Override
        public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
            return new LogicalBoundRelation(table, qualifier, groupExpression, Optional.of(getLogicalProperties()));
        }

        @Override
        public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
                Optional<LogicalProperties> logicalProperties, List<Plan> children) {
            return new LogicalBoundRelation(table, qualifier, groupExpression, logicalProperties);
        }

        @Override
        public LogicalBoundRelation withRelationId(RelationId relationId) {
            throw new RuntimeException("should not call LogicalBoundRelation's withRelationId method");
        }
    }
}
