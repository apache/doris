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
import org.apache.doris.nereids.trees.ChangeScanInfo;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableStreamScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Normalize CHANGES semantic binlog scans without touching real stream scan behavior.
 */
public class NormalizeOlapTableBinlogScan implements CustomRewriter {
    private static final long ROW_BINLOG_APPEND = 0L;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(BinlogScanReplacer.INSTANCE, null);
    }

    private static class BinlogScanReplacer extends DefaultPlanRewriter<Void> {
        private static final BinlogScanReplacer INSTANCE = new BinlogScanReplacer();

        @Override
        public Plan visitLogicalOlapTableStreamScan(LogicalOlapTableStreamScan scan, Void context) {
            if (scan.isNormalized() || !scan.getChangeScanInfo().isPresent()) {
                return scan;
            }
            ChangeScanInfo.InformationKind informationKind = scan.getChangeScanInfo().get().getInformationKind();
            Plan plan = scan.withIncrementalScan(true)
                    .withNormalized(true);
            if (informationKind == ChangeScanInfo.InformationKind.APPEND_ONLY) {
                Slot opSlot = findSlotByName(plan.getOutput(), Column.BINLOG_OPERATION_COL);
                plan = new LogicalFilter<>(ImmutableSet.of(new EqualTo(opSlot, new BigIntLiteral(ROW_BINLOG_APPEND))),
                        plan);
                List<Slot> originSlots = scan.getLogicalProperties().getOutput();
                plan = new LogicalProject<>(originSlots.stream().map(NamedExpression.class::cast)
                        .collect(Collectors.toList()), plan);
            }
            return plan;
        }

        private Slot findSlotByName(List<Slot> slots, String slotName) {
            for (Slot slot : slots) {
                if (slot.getName().equals(slotName)) {
                    return slot;
                }
            }
            throw new IllegalStateException("Missing binlog slot " + slotName);
        }
    }
}
