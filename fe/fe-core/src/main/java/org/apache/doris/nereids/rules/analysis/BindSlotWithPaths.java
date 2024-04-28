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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Rule to bind slot with path in query plan.
 * Slots with paths do not exist in OlapTable so in order to materialize them,
 * generate a LogicalProject on LogicalOlapScan which merges both slots from LogicalOlapScan
 * and alias functions from original expressions before rewritten.
 */
public class BindSlotWithPaths implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // only scan
                RuleType.BINDING_SLOT_WITH_PATHS_SCAN.build(
                        logicalOlapScan().whenNot(LogicalOlapScan::isProjectPulledUp).thenApply(ctx -> {
                            if (ConnectContext.get() != null
                                    && ConnectContext.get().getSessionVariable() != null
                                    && !ConnectContext.get().getSessionVariable().isEnableRewriteElementAtToSlot()) {
                                return ctx.root;
                            }
                            LogicalOlapScan logicalOlapScan = ctx.root;
                            List<NamedExpression> newProjectsExpr = new ArrayList<>(logicalOlapScan.getOutput());
                            Set<SlotReference> pathsSlots = ctx.statementContext.getAllPathsSlots();
                            // With new logical properties that contains new slots with paths
                            StatementContext stmtCtx = ConnectContext.get().getStatementContext();
                            ImmutableList.Builder<NamedExpression> newExprsBuilder
                                    = ImmutableList.builderWithExpectedSize(pathsSlots.size());
                            for (SlotReference slot : pathsSlots) {
                                Preconditions.checkNotNull(stmtCtx.getRelationBySlot(slot),
                                        "[Not implemented] Slot not found in relation map, slot ", slot);
                                if (stmtCtx.getRelationBySlot(slot).getRelationId()
                                        == logicalOlapScan.getRelationId()) {
                                    newExprsBuilder.add(new Alias(slot.getExprId(),
                                            stmtCtx.getOriginalExpr(slot), slot.getName()));
                                }
                            }
                            ImmutableList<NamedExpression> newExprs = newExprsBuilder.build();
                            if (newExprs.isEmpty()) {
                                return ctx.root;
                            }
                            newProjectsExpr.addAll(newExprs);
                            return new LogicalProject<>(newProjectsExpr, logicalOlapScan.withProjectPulledUp());
                        }))
        );
    }
}
