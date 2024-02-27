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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.qe.ConnectContext;

import java.util.Set;
import java.util.stream.Collectors;

/** CheckColumnPrivilege */
public class CheckColumnPrivilege extends ColumnPruning {
    private JobContext jobContext;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        this.jobContext = jobContext;
        return super.rewriteRoot(plan, jobContext);
    }

    @Override
    public Plan visitLogicalView(LogicalView<? extends Plan> view, PruneContext context) {
        checkColumnPrivileges(view.getCatalog(), view.getDb(), view.getName(), context.requiredSlots);

        // stop check privilege in the view
        return view;
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, PruneContext context) {
        if (relation instanceof LogicalCatalogRelation) {
            DatabaseIf database = ((LogicalCatalogRelation) relation).getDatabase();
            CatalogIf catalog = database.getCatalog();
            TableIf table = ((LogicalCatalogRelation) relation).getTable();
            checkColumnPrivileges(catalog.getName(), database.getFullName(), table.getName(), context.requiredSlots);
        }
        return super.visitLogicalRelation(relation, context);
    }

    private void checkColumnPrivileges(String catalog, String db, String name, Set<Slot> requiredSlots) {
        ConnectContext connectContext = jobContext.getCascadesContext().getConnectContext();
        if (connectContext != null) {
            Env env = connectContext.getEnv();
            AccessControllerManager accessManager = env.getAccessManager();
            UserIdentity user = connectContext.getCurrentUserIdentity();
            Set<String> usedColumns = requiredSlots.stream()
                    .map(Slot::getName).collect(Collectors.toSet());
            try {
                accessManager.checkColumnsPriv(user, catalog, db, name, usedColumns, PrivPredicate.SELECT);
            } catch (UserException e) {
                throw new AnalysisException(e.getMessage(), e);
            }
        }
    }
}
