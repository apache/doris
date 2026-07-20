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
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.mtmv.ivm.IvmUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
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
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

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
                .when(scan -> ConnectContext.get().getStatementContext().getIvmRewriteContext()
                        .filter(context -> context.getMode() == IvmRewriteContext.Mode.FULL)
                        .filter(IvmRewriteContext::hasFullRefreshStreamScans)
                        .isPresent())
                .thenApply(ctx -> rewriteScan(ctx.root,
                        ctx.statementContext.getIvmRewriteContext().get()))
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

        OlapTableStream stream = IvmUtil.getIvmStream(rewriteContext.getMtmv(), baseTable);
        OlapTableStreamWrapper streamWrapper = new OlapTableStreamWrapper(stream, baseTable, selectedPartitionIds);
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
            } else if (oldSlot.getName().startsWith(Column.HIDDEN_COLUMN_PREFIX)) {
                if (Column.DELETE_SIGN.equals(oldSlot.getName())) {
                    child = new TinyIntLiteral((byte) 0);
                } else if (Column.VERSION_COL.equals(oldSlot.getName())
                        || Column.COMMIT_TSO_COL.equals(oldSlot.getName())) {
                    child = new BigIntLiteral(0L);
                } else {
                    child = new NullLiteral(oldSlot.getDataType());
                }
            } else {
                throw new AnalysisException("IVM full refresh stream scan missing column "
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
