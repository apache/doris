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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.catalog.stream.OlapTableStreamWrapper;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.ivm.IvmException;
import org.apache.doris.mtmv.ivm.IvmFailureReason;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Rewrites full-refresh base table scans after IVM normalization. */
public class IvmFullRefreshMTMV extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return logicalOlapScan()
                .whenNot(LogicalOlapTableStreamScan.class::isInstance)
                // The rewrite context is read from the statement context this plan is planned under, not
                // from the thread-local one: an EXPLAIN REFRESH preview plans the command's own statement
                // context (see RefreshMTMVCommand#getExplainConnectContext), while the thread-local context
                // may be a temporary one the preview's own analysis swapped in and out.
                .thenApply(ctx -> {
                    Optional<IvmRewriteContext> rewriteContext = ctx.statementContext.getIvmRewriteContext()
                            .filter(context -> context.getMode() == IvmRewriteContext.Mode.FULL)
                            .filter(IvmRewriteContext::hasFullRefreshStreamScans);
                    if (!rewriteContext.isPresent()) {
                        return ctx.root;
                    }
                    ctx.statementContext.addPlannerHook(CloudTableStreamReadStateHook.INSTANCE);
                    return rewriteScan(ctx.root, rewriteContext.get());
                })
                .toRule(RuleType.IVM_FULL_REFRESH_MTMV);
    }

    private Plan rewriteScan(LogicalOlapScan scan, IvmRewriteContext rewriteContext) {
        OlapTable baseTable = scan.getTable();
        BaseTableInfo baseTableInfo = new BaseTableInfo(baseTable);
        MTMV mtmv = rewriteContext.getMtmv();
        if (MTMVPartitionUtil.isTableExcluded(mtmv.getExcludedTriggerTables(),
                TableNameInfoUtils.fromCatalogDb(baseTable.getDatabase().getCatalog(),
                        baseTable.getDatabase(), baseTable))) {
            return scan;
        }
        Optional<Set<Long>> resetPartitionIds =
                rewriteContext.getFullRefreshResetPartitionIds(baseTableInfo);
        if (resetPartitionIds.isPresent() && resetPartitionIds.get().isEmpty()) {
            // A PCT table this batch's mapping does not name is read as no rows: the partitions it feeds
            // the batch with are none, and its own ordinary scan would read the ones the alignment no
            // longer records -- the sync window dropped them, say -- so their rows would enter the MV
            // partition while the snapshot says the table was not read. Answered here rather than as a
            // stream scan with no partitions, so that a table read as nothing needs no usable stream.
            return new LogicalEmptyRelation(StatementScopeIdGenerator.newRelationId(), scan.getOutput());
        }
        StreamReadMode readMode;
        if (resetPartitionIds.isPresent()) {
            readMode = StreamReadMode.RESET;
        } else {
            boolean isPctTable = mtmv.getMvPartitionInfo().getPctInfos().stream()
                    .anyMatch(pctInfo -> pctInfo.getTableInfo().equals(baseTableInfo));
            Optional<StreamReadMode> nonPctReadMode = rewriteContext.getFullRefreshNonPctReadMode();
            if (isPctTable || !nonPctReadMode.isPresent()) {
                return scan;
            }
            readMode = nonPctReadMode.get();
        }
        List<Long> selectedPartitionIds = resetPartitionIds
                .map(ArrayList::new)
                .orElseGet(() -> new ArrayList<>(scan.getSelectedPartitionIds()));
        selectedPartitionIds.sort(Long::compareTo);

        Optional<OlapTableStream> stream = IvmUtil.findIvmStream(rewriteContext.getMtmv(), baseTable);
        if (!stream.isPresent() && rewriteContext.isExplain()) {
            // A refresh reconciles the MV's streams before it reads anything (MTMVTask#reconcileIvmStreams),
            // so the refresh this previews would read through a usable stream here. A preview cannot create
            // one -- it must stay read-only -- and failing instead would deny the plan for a refresh that
            // would run: show the read the refresh makes, the same partitions from the base table itself.
            // Execution keeps failing closed on a stream it did not reconcile.
            return selectedPartitionIds.equals(scan.getSelectedPartitionIds())
                    ? scan : scan.withSelectedPartitionIds(selectedPartitionIds);
        }
        OlapTableStreamWrapper streamWrapper = new OlapTableStreamWrapper(
                stream.orElseGet(() -> IvmUtil.getIvmStream(rewriteContext.getMtmv(), baseTable)),
                baseTable, selectedPartitionIds);
        LogicalOlapTableStreamScan streamScan = new LogicalOlapTableStreamScan(
                StatementScopeIdGenerator.newRelationId(), streamWrapper, scan.getQualifier(),
                selectedPartitionIds, scan.getSelectedTabletIds(), scan.getHints(),
                scan.getTableSample(), scan.getOperativeSlots())
                .withReadMode(readMode);
        Map<String, Slot> streamSlotByName = new HashMap<>();
        for (Slot slot : streamScan.getOutput()) {
            streamSlotByName.put(slot.getName(), slot);
        }

        List<NamedExpression> projects = new ArrayList<>();
        for (Slot oldSlot : scan.getOutput()) {
            Slot streamSlot = streamSlotByName.get(oldSlot.getName());
            Expression child;
            if (streamSlot != null) {
                child = streamSlot;
            } else if (IvmUtil.isCommonHiddenSlot(oldSlot.getName())) {
                child = IvmUtil.getCommonHiddenSlotDefault(oldSlot.getName(), oldSlot.getDataType());
            } else if (oldSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                child = new NullLiteral(oldSlot.getDataType());
            } else {
                throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                        "IVM full refresh stream scan missing column "
                                + oldSlot.getName() + " for table " + scan.getTable().getName());
            }
            if (child.nullable() != oldSlot.nullable()) {
                child = oldSlot.nullable() ? new Nullable(child) : new NonNullable(child);
            }
            projects.add(new Alias(oldSlot.getExprId(), ImmutableList.of(child), oldSlot.getName(),
                    oldSlot.getQualifier(), false));
        }
        return new LogicalProject<>(projects, streamScan);
    }
}
