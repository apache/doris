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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

/**
 * DELETE plan synthesizer for Paimon tables.
 *
 * <p>The projection differs from the Iceberg base on purpose. An Iceberg position delete writes a
 * standalone DeleteFile addressed purely by locator, so its plan carries {@code [operation,
 * locator]}. A Paimon delete instead feeds the SAME sink row shape as an insert:
 * <ul>
 *   <li>primary-key table: the writer emits the full row re-tagged {@code RowKind.DELETE} and the
 *       merge engine cancels it against the key, so the plan must carry every data column (the
 *       locator rides along as NULL and the write schema skips it);</li>
 *   <li>unaware-bucket append table with deletion vectors: the writer marks the locator's
 *       (file, ordinal) in a deletion-vector index, so the plan must carry the locator with real
 *       values (the data columns ride along unused).</li>
 * </ul>
 * One shape — {@code [data columns..., locator]} — serves both, mirroring the sink's
 * {@code column_names} contract in {@code PaimonWritePlanProvider.planWrite}.
 */
public class PaimonRowLevelDeletePlanBuilder extends ExternalRowLevelDeletePlanBuilder {

    public PaimonRowLevelDeletePlanBuilder(
            List<String> nameParts,
            String tableAlias,
            boolean isTempPart,
            List<String> partitions,
            LogicalPlan logicalQuery) {
        super(nameParts, tableAlias, isTempPart, partitions, logicalQuery);
    }

    @Override
    protected LogicalPlan buildPositionDeletePlan(ConnectContext ctx, LogicalPlan logicalQuery,
                                                  ExternalTable paimonTable) {
        LogicalPlan planWithRowId = RowLevelDmlRowIdUtils.injectRowIdColumn(logicalQuery);
        ImmutableList.Builder<NamedExpression> projectItems = ImmutableList.builder();
        if (!RowLevelDmlRowIdUtils.hasUnboundPlan(planWithRowId)) {
            Optional<Slot> rowIdSlot = RowLevelDmlRowIdUtils.findRowIdSlot(planWithRowId.getOutput());
            for (Slot slot : planWithRowId.getOutput()) {
                if (rowIdSlot.isPresent() && slot.equals(rowIdSlot.get())) {
                    continue;
                }
                projectItems.add(slot);
            }
            projectItems.add(rowIdSlot.isPresent()
                    ? (NamedExpression) rowIdSlot.get()
                    : new UnboundSlot(RowLevelDmlRowIdUtils.rowIdColumnName(paimonTable)));
        } else {
            // Unbound plan: the star expands to the visible data columns at bind time (the invisible
            // locator is not part of a star expansion), and the locator is named explicitly after it.
            projectItems.add(new UnboundStar(ImmutableList.of()));
            projectItems.add(new UnboundSlot(RowLevelDmlRowIdUtils.rowIdColumnName(paimonTable)));
        }
        return new LogicalProject<>(projectItems.build(), planWithRowId);
    }
}
