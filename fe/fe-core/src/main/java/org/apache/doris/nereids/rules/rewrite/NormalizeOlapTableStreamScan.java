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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 1. remove STREAM_CHANGE_TYPE_VIRTUAL_COLUMN & STREAM_SEQ_VIRTUAL_COLUMN from olap table stream scan output
 *    with alias projection
 * 2. add delete sign column if unique base table
 */
public class NormalizeOlapTableStreamScan implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(OlapTableStreamScanReplacer.INSTANCE, null);
    }

    private static class OlapTableStreamScanReplacer extends DefaultPlanRewriter<Void> {
        protected static final OlapTableStreamScanReplacer INSTANCE = new OlapTableStreamScanReplacer();

        @Override
        public Plan visitLogicalOlapTableStreamScan(LogicalOlapTableStreamScan scan, Void context) {
            if (scan.isNormalized()) {
                return scan;
            }
            List<Long> selectedPartitionIds = scan.getSelectedPartitionIds();
            if (selectedPartitionIds.isEmpty()) {
                return scan;
            }
            List<Long> historicalPartitionIds = ImmutableList.copyOf(((OlapTableStreamWrapper) scan.getTable())
                    .filterHistoryPartitionIds(selectedPartitionIds));
            List<Long> incrementalPartitionIds = ImmutableList.copyOf(((OlapTableStreamWrapper) scan.getTable())
                    .filterIncrementalPartitionIds(selectedPartitionIds));
            Plan historyPlan = null;
            Plan incrementalPlan = null;
            List<Slot> originSlots = scan.getLogicalProperties().getOutput();
            List<Slot> newSlots = originSlots.stream()
                    .filter(slot -> !(slot instanceof SlotReference
                            && ((SlotReference) slot).getOriginalColumn().isPresent()
                            && ((SlotReference) slot).getOriginalColumn().get()
                            .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)))
                    .filter(slot -> !(slot instanceof SlotReference
                            && ((SlotReference) slot).getOriginalColumn().isPresent()
                            && ((SlotReference) slot).getOriginalColumn().get()
                            .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)))
                    .collect(Collectors.toList());

            // history plan
            if (!historicalPartitionIds.isEmpty()) {
                // add delete sign column if unique base table
                Slot deleteSlot = null;
                for (Column column : scan.getTable().getBaseSchema(true)) {
                    if (column.getName().equals(Column.DELETE_SIGN)) {
                        deleteSlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), scan.getTable(),
                                column, scan.qualified());
                        newSlots.add(deleteSlot);
                        break;
                    }
                }
                Plan plan = scan.withSelectedPartitionIds(historicalPartitionIds, true)
                                .withCachedOutput(new ArrayList<>(newSlots))
                                .withNormalized(true);
                if (deleteSlot != null) {
                    Expression conjunct = new EqualTo(deleteSlot, new TinyIntLiteral((byte) 0));
                    if (!scan.getTable().getEnableUniqueKeyMergeOnWrite()) {
                        plan = scan.withPreAggStatus(PreAggStatus.off(
                                Column.DELETE_SIGN + " is used as conjuncts."));
                    }
                    plan = new LogicalFilter<>(ImmutableSet.of(conjunct), plan);
                }
                // replace virtual column with constant projection
                List<NamedExpression> newProject = newSlots.stream()
                        .map(NamedExpression.class::cast).collect(Collectors.toList());
                for (Slot slot : originSlots) {
                    if (slot instanceof SlotReference
                            && ((SlotReference) slot).getOriginalColumn().isPresent()
                            && ((SlotReference) slot).getOriginalColumn().get()
                            .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)) {
                        newProject.add(new Alias(slot.getExprId(), new VarcharLiteral("APPEND"),
                                Column.STREAM_CHANGE_TYPE_COL));
                    }
                    if (slot instanceof SlotReference
                            && ((SlotReference) slot).getOriginalColumn().isPresent()
                            && ((SlotReference) slot).getOriginalColumn().get()
                            .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)) {
                        newProject.add(new Alias(slot.getExprId(), new BigIntLiteral(-1), Column.STREAM_SEQ_COL));
                    }
                }
                historyPlan = new LogicalProject<>(newProject, plan);
            }

            // incremental plan
            if (!incrementalPartitionIds.isEmpty()) {
                List<NamedExpression> newProject = newSlots.stream()
                        .map(NamedExpression.class::cast).collect(Collectors.toList());
                // add slot from binlog
                Slot opSlot = null;
                Slot seqSlot = null;
                for (Column column : ((OlapTableStreamWrapper) scan.getTable()).getRowBinlogSchema()) {
                    if (column.getName().equals(Column.BINLOG_LSN_COL)) {
                        seqSlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), scan.getTable(),
                                column, scan.qualified());
                        newSlots.add(seqSlot);
                    }
                    if (column.getName().equals(Column.BINLOG_OPERATION_COL)) {
                        opSlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), scan.getTable(),
                                column, scan.qualified());
                        newSlots.add(opSlot);
                    }
                }
                Plan plan = scan.withSelectedPartitionIds(incrementalPartitionIds, true)
                        .withCachedOutput(new ArrayList<>(newSlots))
                        .withIncrementalScan(true)
                        .withNormalized(true);
                for (Slot slot : originSlots) {
                    if (slot instanceof SlotReference
                            && ((SlotReference) slot).getOriginalColumn().isPresent()
                            && ((SlotReference) slot).getOriginalColumn().get()
                            .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)) {
                        newProject.add(new Alias(slot.getExprId(), opSlot,
                                Column.STREAM_CHANGE_TYPE_COL));
                    }
                    if (slot instanceof SlotReference
                            && ((SlotReference) slot).getOriginalColumn().isPresent()
                            && ((SlotReference) slot).getOriginalColumn().get()
                            .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)) {
                        newProject.add(new Alias(slot.getExprId(), seqSlot, Column.STREAM_SEQ_COL));
                    }
                }
                incrementalPlan = new LogicalProject<>(newProject, plan);
            }

            if (historyPlan == null && incrementalPlan == null) {
                return new LogicalEmptyRelation(ConnectContext.get().getStatementContext().getNextRelationId(),
                        scan.getOutput());
            } else if (historyPlan == null) {
                return incrementalPlan;
            } else if (incrementalPlan == null) {
                return historyPlan;
            }
            // return union plan
            List<Plan> children = Lists.newArrayList(historyPlan, incrementalPlan);
            return new LogicalUnion(Qualifier.ALL,
                    originSlots.stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                    children.stream()
                            .map(plan -> plan.getOutput().stream()
                                    .map(slot -> (SlotReference) slot.toSlot())
                                    .collect(Collectors.toList()))
                            .collect(Collectors.toList()),
                    ImmutableList.of(),
                    false,
                    children);
        }
    }
}
