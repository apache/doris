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

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.table.TableValuedFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** CheckPrivileges */
public class CheckPrivileges extends ColumnPruning {
    private JobContext jobContext;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        this.jobContext = jobContext;
        super.rewriteRoot(plan, jobContext);

        // don't rewrite plan
        return plan;
    }

    @Override
    public Plan visitLogicalView(LogicalView<? extends Plan> view, PruneContext context) {
        checkColumnPrivileges(view.getView(), computeUsedColumns(view, context.requiredSlots));

        // stop check privilege in the view
        return view;
    }

    @Override
    public Plan visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, PruneContext context) {
        TableValuedFunction tvf = tvfRelation.getFunction();
        tvf.checkAuth(jobContext.getCascadesContext().getConnectContext());
        return super.visitLogicalTVFRelation(tvfRelation, context);
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PruneContext context) {
        if (relation instanceof LogicalCatalogRelation) {
            TableIf table = ((LogicalCatalogRelation) relation).getTable();
            checkColumnPrivileges(table, computeUsedColumns(relation, context.requiredSlots));
        }
        return super.visitLogicalRelation(relation, context);
    }

    private Set<String> computeUsedColumns(Plan plan, Set<Slot> requiredSlots) {
        List<Slot> outputs = plan.getOutput();
        Map<Integer, Slot> idToSlot = new LinkedHashMap<>(outputs.size());
        for (Slot output : outputs) {
            idToSlot.putIfAbsent(output.getExprId().asInt(), output);
        }

        Set<String> usedColumns = Sets.newLinkedHashSetWithExpectedSize(requiredSlots.size());
        for (Slot requiredSlot : requiredSlots) {
            Slot slot = idToSlot.get(requiredSlot.getExprId().asInt());
            if (slot != null) {
                // don't check privilege for hidden column, e.g. __DORIS_DELETE_SIGN__
                if (slot instanceof SlotReference && ((SlotReference) slot).getColumn().isPresent()
                        && !((SlotReference) slot).getColumn().get().isVisible()) {
                    continue;
                }
                usedColumns.add(slot.getName());
            }
        }
        return usedColumns;
    }

    private void checkColumnPrivileges(TableIf table, Set<String> usedColumns) {
        CascadesContext cascadesContext = jobContext.getCascadesContext();
        ConnectContext connectContext = cascadesContext.getConnectContext();
        try {
            UserAuthentication.checkPermission(table, connectContext, usedColumns);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        StatementContext statementContext = cascadesContext.getStatementContext();
        Optional<SqlCacheContext> sqlCacheContext = statementContext.getSqlCacheContext();
        if (sqlCacheContext.isPresent()) {
            sqlCacheContext.get().addCheckPrivilegeTablesOrViews(table, usedColumns);
        }
    }
}
