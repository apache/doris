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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.analysis.UserAuthentication;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
    public Plan visitLogicalRelation(LogicalRelation relation, PruneContext context) {
        if (relation instanceof LogicalCatalogRelation) {
            TableIf table = ((LogicalCatalogRelation) relation).getTable();
            checkColumnPrivileges(table, computeUsedColumns(relation, context.requiredSlots));
        }
        return super.visitLogicalRelation(relation, context);
    }

    private Set<String> computeUsedColumns(Plan plan, Set<Slot> requiredSlots) {
        Map<Integer, Slot> idToSlot = plan.getOutputSet()
                .stream()
                .collect(Collectors.toMap(slot -> slot.getExprId().asInt(), slot -> slot));
        return requiredSlots
                .stream()
                .map(slot -> idToSlot.get(slot.getExprId().asInt()))
                .filter(slot -> slot != null)
                .map(NamedExpression::getName)
                .collect(Collectors.toSet());
    }

    private void checkColumnPrivileges(TableIf table, Set<String> usedColumns) {
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        try {
            UserAuthentication.checkPermission(table, connectContext, usedColumns);
        } catch (UserException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }
}
