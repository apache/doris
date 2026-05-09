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
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableSet;

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
            if (scan.isIncrementalScan()) {
                return scan;
            }
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

            if (originSlots.equals(newSlots)) {
                return scan;
            }

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
            Plan plan = scan.withCachedOutput(newSlots);
            if (deleteSlot != null) {
                Expression conjunct = new EqualTo(deleteSlot, new TinyIntLiteral((byte) 0));
                if (!scan.getTable().getEnableUniqueKeyMergeOnWrite()) {
                    plan = scan.withPreAggStatus(PreAggStatus.off(
                            Column.DELETE_SIGN + " is used as conjuncts."));
                }
                plan = new LogicalFilter<>(ImmutableSet.of(conjunct), plan);
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
                    project.add(new Alias(slot.getExprId(), new BigIntLiteral(-1), Column.STREAM_SEQ_COL));
                }
            }
            return new LogicalProject<>(project, plan);
        }
    }
}
