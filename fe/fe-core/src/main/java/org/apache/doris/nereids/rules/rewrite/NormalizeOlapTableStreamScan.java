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
import org.apache.doris.binlog.BinlogUtils;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.OlapTableWrapper;
import org.apache.doris.catalog.RowBinlogTableWrapper;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.WhenClause;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.planner.OlapScanNode;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 1. remove STREAM_CHANGE_TYPE_VIRTUAL_COLUMN & STREAM_SEQ_VIRTUAL_COLUMN from olap table stream scan output
 * with alias projection
 * 2. add delete sign column if unique base table
 */
public class NormalizeOlapTableStreamScan extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalOlapTableStreamScan()
                .thenApply(ctx -> normalize(ctx.root, ctx.cascadesContext))
                .toRule(RuleType.NORMALIZE_OlAP_TABLE_STREAM_SCAN);
    }

    private static Expression buildChangeTypeExpr(Slot opSlot) {
        return new CaseWhen(ImmutableList.of(
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(BinlogUtils.ROW_BINLOG_APPEND)),
                        new VarcharLiteral("APPEND")),
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(BinlogUtils.ROW_BINLOG_DELETE)),
                        new VarcharLiteral("DELETE")),
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(BinlogUtils.ROW_BINLOG_UPDATE_BEFORE)),
                        new VarcharLiteral("UPDATE_BEFORE")),
                new WhenClause(new EqualTo(opSlot, new BigIntLiteral(BinlogUtils.ROW_BINLOG_UPDATE_AFTER)),
                        new VarcharLiteral("UPDATE_AFTER"))), new VarcharLiteral("UNKNOWN"));
    }

    private Plan normalize(LogicalOlapTableStreamScan scan, CascadesContext cascadesContext) {
        // short-cut for empty partition
        if (scan.getSelectedPartitionIds().isEmpty()) {
            return new LogicalEmptyRelation(cascadesContext.getStatementContext().getNextRelationId(),
                    scan.getOutput());
        }
        if (scan.isReset()) {
            return makeResetOlapFullScan(scan, cascadesContext);
        }
        if (scan.isSnapshot()) {
            return makeSnapshotScan(scan, cascadesContext);
        }
        return makeTableStreamScan(scan, cascadesContext);
    }

    /**
     * Build a projection list that exposes the columns of {@code wantedSlots} (slots taken from the
     * original stream scan output) on top of the rewritten child whose output is {@code childOutput}.
     */
    private List<NamedExpression> mapOriginOutputFromChild(List<Slot> wantedSlots, List<Slot> childOutput,
                                                           boolean useOriginalExprIds) {
        Map<String, Slot> childSlotByName = new HashMap<>();
        for (Slot slot : childOutput) {
            childSlotByName.put(slot.getName(), slot);
        }
        List<NamedExpression> project = new ArrayList<>(wantedSlots.size());
        for (Slot wanted : wantedSlots) {
            Slot match = childSlotByName.get(wanted.getName());
            Preconditions.checkArgument(match != null,
                    "column %s not found in child output", wanted.getName());
            if (useOriginalExprIds) {
                project.add(new Alias(wanted.getExprId(), match, wanted.getName()));
            } else {
                project.add(new Alias(match, wanted.getName()));
            }
        }
        return project;
    }

    // project from child slots to origin output slots with new expr ids
    private Plan projectFromOriginSlots(Plan plan, List<Slot> originSlots) {
        return new LogicalProject<>(mapOriginOutputFromChild(originSlots, plan.getOutput(), false), plan);
    }

    // project to origin output slots with original expr ids. always add the project before final normalize output
    private Plan projectToOriginSlots(Plan plan, List<Slot> originSlots) {
        return new LogicalProject<>(mapOriginOutputFromChild(originSlots, plan.getOutput(), true), plan);
    }

    /**
     * Build an incremental scan reading row-level changes from base table binlog for the given
     * partitions and their {@code offsetMap} ((startTso, endTso) per partition).
     *
     * <p>The binlog scan is wrapped by {@link RowBinlogTableWrapper} and marked as INCREMENTAL_READ.
     * {@code streamScanType} decides which binlog rows are emitted:
     * APPEND_ONLY keeps only APPEND rows; MIN_DELTA/DETAIL keep the raw change rows.
     *
     * <p>{@code isIncremental} distinguishes the two callers:
     * <ul>
     *   <li>true  — normal stream consumption: map the binlog op/timestamp columns into the stream
     *               virtual columns STREAM_CHANGE_TYPE_COL / STREAM_SEQ_COL.</li>
     *   <li>false — snapshot rebuild: only keep DELETE &amp; UPDATE_BEFORE rows so they can be added
     *               back to reconstruct the "before" image at the snapshot point.</li>
     * </ul>
     */
    private Plan makeIncrementalScanFromBinlog(CascadesContext cascadesContext, LogicalOlapTableStreamScan scan,
                                               List<Long> selectedPartitionIds,
                                               OlapTable baseTable, Map<Long, Pair<Long, Long>> offsetMap,
                                               BaseTableStream.StreamScanType streamScanType, List<Slot> originSlots,
                                               List<Slot> notVirtualSlots, boolean isIncremental) {
        // remap scan from binlog
        RowBinlogTableWrapper table =
                new RowBinlogTableWrapper(baseTable, offsetMap);
        Map<String, String> scanParams = new HashMap<>();
        scanParams.put(OlapScanNode.OLAP_INCREMENT_TYPE, streamScanType.toString());
        LogicalOlapScan newScan = new LogicalOlapScan(cascadesContext.getStatementContext().getNextRelationId(),
                table, scan.qualified(), selectedPartitionIds, scan.getSelectedTabletIds(),
                new ArrayList<>(), scan.getTableSample(), ImmutableList.of(),
                Optional.of(new TableScanParams(TableScanParams.INCREMENTAL_READ, scanParams, Lists.newArrayList())));
        Plan plan = newScan;
        List<Slot> binlogOutputSlots = newScan.getOutput();
        // project stream virtual slot from binlog
        Slot opSlot = null;
        Slot seqSlot = null;
        for (int i = 0; i < binlogOutputSlots.size(); i++) {
            if (binlogOutputSlots.get(i).getName().equals(Column.BINLOG_TIMESTAMP_COL)) {
                seqSlot = binlogOutputSlots.get(i);
            } else if (binlogOutputSlots.get(i).getName().equals(Column.BINLOG_OPERATION_COL)) {
                opSlot = binlogOutputSlots.get(i);
            }
        }
        if (streamScanType.equals(BaseTableStream.StreamScanType.APPEND_ONLY)) {
            // filter append-only operation if needed
            Preconditions.checkArgument(opSlot != null);
            plan = new LogicalFilter<>(ImmutableSet.of(new EqualTo(opSlot,
                    new BigIntLiteral(BinlogUtils.ROW_BINLOG_APPEND))), plan);
        }
        List<NamedExpression> project = mapOriginOutputFromChild(notVirtualSlots, binlogOutputSlots, false);
        if (isIncremental) {
            // replace stream virtual column with alias slot reference
            for (Slot slot : originSlots) {
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)) {
                    project.add(new Alias(StatementScopeIdGenerator.newExprId(), buildChangeTypeExpr(opSlot),
                            Column.STREAM_CHANGE_TYPE_COL));
                } else if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)) {
                    project.add(new Alias(StatementScopeIdGenerator.newExprId(), seqSlot, Column.STREAM_SEQ_COL));
                }
            }
        } else {
            // only filter delete & update before rows for building before snapshot image
            Preconditions.checkArgument(opSlot != null);
            Expression opFilter = new InPredicate(opSlot, ImmutableList.of(
                    new BigIntLiteral(BinlogUtils.ROW_BINLOG_DELETE),
                    new BigIntLiteral(BinlogUtils.ROW_BINLOG_UPDATE_BEFORE)));
            plan = new LogicalFilter<>(ImmutableSet.of(opFilter), plan);
        }
        return new LogicalProject<>(project, plan);
    }

    /**
     * Reset mode: the stream is (re)initialized to a full snapshot of the base table, so we simply
     * do a full olap scan over the base table (with full schema) and project back to the origin
     * stream output slots. No binlog / virtual columns are involved.
     */
    private Plan makeResetOlapFullScan(LogicalOlapTableStreamScan scan, CascadesContext cascadesContext) {
        // make olap scan on base table
        OlapTableStreamWrapper streamWrapper = scan.getTable();
        OlapTable baseTable = streamWrapper.getBaseTable();
        Plan plan = makeOlapScanOnBaseTable(scan, cascadesContext, baseTable, scan.getSelectedPartitionIds());
        List<Slot> originSlots = scan.getLogicalProperties().getOutput();
        return projectToOriginSlots(plan, originSlots);
    }

    /**
     * Snapshot mode: read a consistent snapshot of the stream at its consumption point.
     *
     * <p>For DUP_KEYS tables the snapshot can be rebuilt directly from the base table using the
     * historical partition offsets.
     *
     * <p>For unique/agg tables partitions are split into two groups:
     * <ul>
     *   <li>normal partitions — no new data after the snapshot point, scanned from base table as-is;</li>
     *   <li>rebuild partitions — have newer data after the snapshot point, so the snapshot image is
     *       reconstructed by scanning the base table (rows with commit tso &lt;= consumption tso) and
     *       adding back the DELETE / UPDATE_BEFORE rows from binlog.</li>
     * </ul>
     * The two parts are unioned and projected back to the origin output slots.
     */
    private Plan makeSnapshotScan(LogicalOlapTableStreamScan scan, CascadesContext cascadesContext) {
        List<Long> selectedPartitionIds = scan.getSelectedPartitionIds();
        OlapTableStreamWrapper streamWrapper = scan.getTable();
        OlapTable baseTable = streamWrapper.getBaseTable();
        List<Slot> originSlots = scan.getLogicalProperties().getOutput();
        selectedPartitionIds = streamWrapper.filterConsumedPartitionIds(selectedPartitionIds);
        if (baseTable.getKeysType().equals(KeysType.DUP_KEYS)) {
            // dup key table can just rebuild from base table
            Map<Long, Pair<Long, Long>> partitionOffsetMap =
                    streamWrapper.getHistoryPartitionOffsets(selectedPartitionIds);
            OlapTableWrapper table =
                    new OlapTableWrapper(baseTable, partitionOffsetMap);
            return projectToOriginSlots(makeOlapScanOnBaseTable(scan, cascadesContext, table, selectedPartitionIds),
                    originSlots);
        }
        // normal partition has no new data after snapshot, scan base table directly
        List<Long> normalPartitionIds = streamWrapper.filterNormalSnapshotPartitionIds(selectedPartitionIds);
        Set<Long> normalPartitionIdSet = ImmutableSet.copyOf(normalPartitionIds);
        // rebuild partition has new data after snapshot, need to rebuild
        List<Long> rebuildPartitionIds =
                selectedPartitionIds.stream()
                        .filter(id -> !normalPartitionIdSet.contains(id)).collect(ImmutableList.toImmutableList());
        Plan normalPlan = null;
        Plan rebuildPlan = null;
        if (!normalPartitionIds.isEmpty()) {
            normalPlan = makeOlapScanOnBaseTable(scan, cascadesContext, baseTable, normalPartitionIds);
        }
        if (!rebuildPartitionIds.isEmpty()) {
            // base table scan part
            // build base table offset
            // for row commit tso <= consumption tso we scan from base table
            Map<Long, Pair<Long, Long>> partitionOffsetMap =
                    streamWrapper.getHistoryPartitionOffsets(rebuildPartitionIds);
            OlapTableWrapper table =
                    new OlapTableWrapper(baseTable, partitionOffsetMap);
            Plan basePartPlan = makeOlapScanOnBaseTable(scan, cascadesContext, table, rebuildPartitionIds);
            // we rebuild by add back updated & deleted rows from binlog
            Plan binlogPartPlan = makeIncrementalScanFromBinlog(cascadesContext, scan, rebuildPartitionIds,
                    baseTable, streamWrapper.getPartitionOffsets(rebuildPartitionIds),
                    BaseTableStream.StreamScanType.MIN_DELTA, originSlots, originSlots, false);
            rebuildPlan = combineTwoPlan(cascadesContext, basePartPlan, binlogPartPlan, originSlots);
        }
        return projectToOriginSlots(combineTwoPlan(cascadesContext, normalPlan, rebuildPlan, originSlots), originSlots);
    }

    private Plan makeOlapScanOnBaseTable(LogicalOlapTableStreamScan scan, CascadesContext cascadesContext,
                                         OlapTable baseTable, List<Long> partitionIds) {
        LogicalOlapScan baseScan = new LogicalOlapScan(cascadesContext.getStatementContext().getNextRelationId(),
                baseTable, scan.qualified(), partitionIds, scan.getSelectedTabletIds(),
                new ArrayList<>(), scan.getTableSample(), ImmutableList.of());
        Plan plan = baseScan;
        Slot deleteSlot = null;
        List<Slot> baseOutputSlots = baseScan.getOutput();
        for (Slot slot : baseOutputSlots) {
            if (slot.getName().equals(Column.DELETE_SIGN)) {
                deleteSlot = slot;
            }
            if (deleteSlot != null) {
                break;
            }
        }
        if (deleteSlot != null) {
            Expression conjunct = new EqualTo(deleteSlot, new TinyIntLiteral((byte) 0));
            if (!scan.getTable().getEnableUniqueKeyMergeOnWrite()) {
                plan = baseScan.withPreAggStatus(PreAggStatus.off(
                        Column.DELETE_SIGN + " is used as conjuncts."));
            }
            plan = new LogicalFilter<>(ImmutableSet.of(conjunct), plan);
        }
        return plan;
    }

    /**
     * Normal stream consumption: emit the incremental changes since the last consumed offset.
     *
     * <p>Selected partitions are split into:
     * <ul>
     *   <li>historical partitions — never consumed history data; scanned from the base table and all
     *       rows are treated as APPEND (change type = "APPEND", seq = commit tso);</li>
     *   <li>incremental partitions — read row-level changes from binlog via
     *       {@link #makeIncrementalScanFromBinlog}.</li>
     * </ul>
     * The two plans are unioned. {@code notVirtualSlots} are the origin output slots excluding the
     * two stream virtual columns (STREAM_CHANGE_TYPE / STREAM_SEQ), which are filled separately.
     */
    private Plan makeTableStreamScan(LogicalOlapTableStreamScan scan, CascadesContext cascadesContext) {
        OlapTableStreamWrapper streamWrapper = scan.getTable();
        OlapTable baseTable = streamWrapper.getBaseTable();
        List<Long> historicalPartitionIds = streamWrapper.filterHistoryPartitionIds(scan.getSelectedPartitionIds());
        List<Long> incrementalPartitionIds =
                streamWrapper.filterIncrementalPartitionIds(scan.getSelectedPartitionIds());
        Plan historyPlan = null;
        Plan incrementalPlan = null;
        List<Slot> originSlots = scan.getOutput();
        // notVirtualSlots = originSlots - (STREAM_CHANGE_TYPE_VIRTUAL_COLUMN + STREAM_SEQ_VIRTUAL_COLUMN)
        List<Slot> notVirtualSlots = originSlots.stream()
                .filter(slot -> !(slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)))
                .filter(slot -> !(slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)))
                .collect(ImmutableList.toImmutableList());

        // history plan
        if (!historicalPartitionIds.isEmpty()) {
            // for not consume history partition we just scan base table
            Plan plan = makeOlapScanOnBaseTable(scan, cascadesContext, baseTable, historicalPartitionIds);
            List<Slot> baseOutputSlots = plan.getOutput();
            Slot tsoSlot = null;
            for (Slot slot : baseOutputSlots) {
                if (slot.getName().equals(Column.COMMIT_TSO_COL)) {
                    tsoSlot = slot;
                }
                if (tsoSlot != null) {
                    break;
                }
            }
            Preconditions.checkArgument(tsoSlot != null, "Commit tso column not found in base table output");
            List<NamedExpression> project = mapOriginOutputFromChild(notVirtualSlots, baseOutputSlots, false);
            for (Slot slot : originSlots) {
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_CHANGE_TYPE_VIRTUAL_COLUMN)) {
                    project.add(new Alias(StatementScopeIdGenerator.newExprId(), new VarcharLiteral("APPEND"),
                            Column.STREAM_CHANGE_TYPE_COL));
                }
                if (slot instanceof SlotReference
                        && ((SlotReference) slot).getOriginalColumn().isPresent()
                        && ((SlotReference) slot).getOriginalColumn().get()
                        .equals(Column.STREAM_SEQ_VIRTUAL_COLUMN)) {
                    project.add(new Alias(StatementScopeIdGenerator.newExprId(), tsoSlot, Column.STREAM_SEQ_COL));
                }
            }
            historyPlan = new LogicalProject<>(project, plan);
        }

        // incremental plan
        if (!incrementalPartitionIds.isEmpty()) {
            // remap scan from binlog
            incrementalPlan = makeIncrementalScanFromBinlog(cascadesContext, scan, incrementalPartitionIds,
                    baseTable, streamWrapper.getPartitionOffsets(incrementalPartitionIds),
                    streamWrapper.getStreamScanType(), originSlots, notVirtualSlots, true);
        }

        return projectToOriginSlots(combineTwoPlan(cascadesContext, historyPlan, incrementalPlan, originSlots),
                originSlots);
    }

    private Plan combineTwoPlan(CascadesContext cascadesContext, Plan plan0, Plan plan1, List<Slot> originSlots) {
        if (plan0 == null && plan1 == null) {
            return new LogicalEmptyRelation(cascadesContext.getStatementContext().getNextRelationId(),
                    originSlots);
        } else if (plan0 == null) {
            return plan1;
        } else if (plan1 == null) {
            return plan0;
        }
        return makeUnionPlan(plan0, plan1, originSlots);
    }

    private Plan makeUnionPlan(Plan child0, Plan child1, List<Slot> originSlots) {
        child0 = projectFromOriginSlots(child0, originSlots);
        child1 = projectFromOriginSlots(child1, originSlots);
        // return union plan
        List<Plan> children = Lists.newArrayList(child0, child1);
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
