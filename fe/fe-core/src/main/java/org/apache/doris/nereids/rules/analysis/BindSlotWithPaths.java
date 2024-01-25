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
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rule to bind slot with path in query plan.
 * Slots with paths do not exist in OlapTable so in order to materialize them,
 * we need first put them into LogicalOlapScan and get them in LogicalOlapScan::getOutput.
 * But getOutput is memorized in `Suppliers` so, we need to update and refresh each supplier,
 * in order to get the latest slots
 */
public class BindSlotWithPaths implements AnalysisRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // only scan
                RuleType.BINDING_SLOT_WITH_PATHS_SCAN.build(
                        logicalOlapScan().whenNot(LogicalOlapScan::isProjectPulledUp).thenApply(ctx -> {
                            LogicalOlapScan logicalOlapScan = ctx.root;
                            List<NamedExpression> newProjectsExpr = new ArrayList<>(logicalOlapScan.getOutput());
                            Set<SlotReference> pathsSlots = ctx.statementContext.getAllPathsSlots();
                            // With new logical properties that contains new slots with paths
                            StatementContext stmtCtx = ConnectContext.get().getStatementContext();
                            List<Slot> olapScanPathSlots = pathsSlots.stream().filter(
                                    slot -> {
                                        return stmtCtx.getRelationBySlot(slot) != null
                                                && stmtCtx.getRelationBySlot(slot).getRelationId()
                                                == logicalOlapScan.getRelationId();
                                    }).collect(
                                    Collectors.toList());
                            List<NamedExpression> newExprs = olapScanPathSlots.stream()
                                    .map(SlotReference.class::cast)
                                    .map(slotReference ->
                                            new Alias(slotReference.getExprId(),
                                                    stmtCtx.getOriginalExpr(slotReference), slotReference.getName()))
                                    .collect(
                                            Collectors.toList());
                            newProjectsExpr.addAll(newExprs);
                            return new LogicalProject(newProjectsExpr, logicalOlapScan.withProjectPulledUp());
                        }))
        );
    }
}

