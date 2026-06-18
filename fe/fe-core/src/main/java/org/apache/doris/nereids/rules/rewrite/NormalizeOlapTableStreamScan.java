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

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
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
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 1. remove STREAM_CHANGE_TYPE_VIRTUAL_COLUMN & STREAM_SEQ_VIRTUAL_COLUMN from olap table stream scan output
 * with alias projection
 * 2. add delete sign column if unique base table
 */
public class NormalizeOlapTableStreamScan extends OneRewriteRuleFactory {
    private static final long ROW_BINLOG_APPEND = 0;
    private static final long ROW_BINLOG_DELETE = 1;
    private static final long ROW_BINLOG_UPDATE_BEFORE = 2;
    private static final long ROW_BINLOG_UPDATE_AFTER = 3;

    @Override
    public Rule build() {
        return logicalOlapTableStreamScan()
                .when(scan -> !scan.isNormalized())
                .then(this::normalize)
                .toRule(RuleType.NORMALIZE_OlAP_TABLE_STREAM_SCAN);
    }

    private static Expression buildChangeTypeExpr(Slot opSlot) {
        return new CaseWhen(ImmutableList.of(
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(ROW_BINLOG_APPEND)),
                        new VarcharLiteral("APPEND")),
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(ROW_BINLOG_DELETE)),
                        new VarcharLiteral("DELETE")),
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(ROW_BINLOG_UPDATE_BEFORE)),
                        new VarcharLiteral("UPDATE_BEFORE")),
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(ROW_BINLOG_UPDATE_AFTER)),
                        new VarcharLiteral("UPDATE_AFTER"))), new VarcharLiteral("UNKNOWN"));
    }

    private Plan normalize(LogicalOlapTableStreamScan scan) {
        List<Long> selectedPartitionIds = scan.getPartitionSelection().getSelectedPartitionIds();
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
        List<Slot> newSlots = ImmutableList.copyOf(originSlots.stream()
                .filter(slot -> !(slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)))
                .filter(slot -> !(slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)))
                .collect(Collectors.toList()));

        // history plan
        if (!historicalPartitionIds.isEmpty()) {
            List<Slot> scanSlots = new ArrayList<>(newSlots);
            // add delete sign column if unique base table
            Slot deleteSlot = null;
            for (Column column : scan.getTable().getBaseSchema(true)) {
                if (column.getName().equals(Column.DELETE_SIGN)) {
                    deleteSlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), scan.getTable(),
                            column, scan.qualified());
                    scanSlots.add(deleteSlot);
                    break;
                }
            }
            LogicalOlapTableStreamScan newScan = scan.withSelectedPartitionIds(historicalPartitionIds, true)
                    .withCachedOutput(new ArrayList<>(scanSlots))
                    .withNormalized(true);
            Plan plan = newScan;
            if (deleteSlot != null) {
                Expression conjunct = new EqualTo(deleteSlot, new TinyIntLiteral((byte) 0));
                if (!scan.getTable().getEnableUniqueKeyMergeOnWrite()) {
                    newScan = newScan.withPreAggStatus(PreAggStatus.off(
                            Column.DELETE_SIGN + " is used as conjuncts."));
                }
                plan = new LogicalFilter<>(ImmutableSet.of(conjunct), newScan);
            }
            // replace virtual column with constant projection
            List<NamedExpression> project = newSlots.stream()
                    .map(NamedExpression.class::cast).collect(Collectors.toList());
            for (Slot slot : originSlots) {
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)) {
                    project.add(new Alias(slot.getExprId(), new VarcharLiteral("APPEND"),
                            Column.STREAM_CHANGE_TYPE_COL));
                }
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)) {
                    project.add(new Alias(slot.getExprId(), new Nullable(new BigIntLiteral(-1)),
                            Column.STREAM_SEQ_COL));
                }
            }
            historyPlan = new LogicalProject<>(project, plan);
        }

        // incremental plan
        if (!incrementalPartitionIds.isEmpty()) {
            List<Slot> scanSlots = new ArrayList<>(newSlots);
            // add slot from binlog
            Slot opSlot = null;
            Slot seqSlot = null;
            for (Column column : ((OlapTableStreamWrapper) scan.getTable()).getRowBinlogSchema()) {
                if (column.getName().equals(Column.BINLOG_TIMESTAMP_COL)) {
                    seqSlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), scan.getTable(),
                            column, scan.qualified());
                    scanSlots.add(seqSlot);
                } else if (column.getName().equals(Column.BINLOG_OPERATION_COL)) {
                    opSlot = SlotReference.fromColumn(StatementScopeIdGenerator.newExprId(), scan.getTable(),
                            column, scan.qualified());
                    scanSlots.add(opSlot);
                }
            }
            Map<String, String> scanParams = new HashMap<>();
            scanParams.put(OlapScanNode.OLAP_INCREMENT_TYPE,
                    ((OlapTableStreamWrapper) scan.getTable()).getStreamScanType().toString());
            Plan plan = scan.withSelectedPartitionIds(incrementalPartitionIds, true)
                    .withCachedOutput(new ArrayList<>(scanSlots))
                    .withIncrementalScan(true)
                    .withTableScanParams(new TableScanParams(TableScanParams.INCREMENTAL_READ, scanParams,
                            Lists.newArrayList()))
                    .withNormalized(true);
            // replace virtual column with alias slot reference
            List<NamedExpression> project = newSlots.stream()
                    .map(NamedExpression.class::cast).collect(Collectors.toList());
            for (Slot slot : originSlots) {
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)) {
                    project.add(new Alias(slot.getExprId(), buildChangeTypeExpr(opSlot),
                            Column.STREAM_CHANGE_TYPE_COL));
                } else if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)) {
                    project.add(new Alias(slot.getExprId(), seqSlot, Column.STREAM_SEQ_COL));
                }
            }
            incrementalPlan = new LogicalProject<>(project, plan);
        }

        if (historyPlan == null && incrementalPlan == null) {
            return new LogicalEmptyRelation(ConnectContext.get().getStatementContext().getNextRelationId(),
                    scan.getOutput());
        } else if (historyPlan == null) {
            return incrementalPlan;
        } else if (incrementalPlan == null) {
            return historyPlan;
        }
        historyPlan = refreshUnionChildOutputExprIds(historyPlan, originSlots);
        incrementalPlan = refreshUnionChildOutputExprIds(incrementalPlan, originSlots);
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

    private Plan refreshUnionChildOutputExprIds(Plan plan, List<Slot> unionOutputs) {
        Preconditions.checkState(plan.getOutput().size() == unionOutputs.size(),
                "Union child output size %s does not match union output size %s",
                plan.getOutput().size(), unionOutputs.size());
        List<NamedExpression> project = new ArrayList<>(plan.getOutput().size());
        for (int i = 0; i < plan.getOutput().size(); i++) {
            project.add(new Alias(plan.getOutput().get(i), unionOutputs.get(i).getName()));
        }
        return new LogicalProject<>(project, plan);
    }
}
