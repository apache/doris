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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.SelectAuthChecker.AuthCols;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * check select col auth
 */
public class SelectAuthChecker extends DefaultPlanRewriter<AuthCols> implements CustomRewriter {
    private static final Logger LOG = LogManager.getLogger(SelectAuthChecker.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        return plan.accept(this, null);
    }

    @Override
    public Plan visitLogicalProject(LogicalProject<? extends Plan> project, AuthCols authCols) {
        // include: logicalProject(logicalFilter(logicalRelation())) || logicalProject(logicalRelation())
        // exclude: logicalProject(LogicalJoin(*)) or other
        if (CollectionUtils.isEmpty(project.children()) || project.children().size() != 1 || (
                !(project.child(0) instanceof LogicalFilter) && !(project.child(0) instanceof LogicalRelation))) {
            return visit(project, authCols);

        }
        if (authCols == null) {
            authCols = new AuthCols();
        }
        // if `hasProject` is false,match sql is `select *`,we need check priv of all columns
        authCols.setHasProject(true);
        getCols(project.getProjects(), authCols.getCols());
        return visit(project, authCols);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, AuthCols authCols) {
        if (authCols == null) {
            authCols = new AuthCols();
        }
        getCols(filter.getConjuncts(), authCols.getCols());
        return visit(filter, authCols);
    }

    @Override
    public Plan visitLogicalRelation(LogicalRelation relation, AuthCols authCols) {
        if (relation instanceof CatalogRelation) {
            if (authCols == null) {
                authCols = new AuthCols();
            }
            checkSelectAuth((CatalogRelation) relation, authCols);
        }
        return relation;
    }

    /**
     * check col select auth
     */
    private void checkSelectAuth(CatalogRelation catalogRelation, AuthCols authCols) {
        TableIf table;
        DatabaseIf database;
        CatalogIf catalog;
        try {
            table = catalogRelation.getTable();
            if (table == null) {
                return;
            }
            database = catalogRelation.getDatabase();
            if (database == null) {
                return;
            }
            catalog = database.getCatalog();
            if (catalog == null) {
                return;
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("get schema failed:" + e.getMessage());
            return;
        }

        if (!authCols.isHasProject()) {
            authCols.setCols(table.getColumns().stream().map(Column::getName).collect(Collectors.toSet()));
        }
        try {
            Env.getCurrentEnv().getAccessManager()
                    .checkColumnsPriv(ConnectContext.get().getCurrentUserIdentity(), catalog.getName(),
                            database.getFullName(), table.getName(), authCols.getCols(), PrivPredicate.SELECT);
        } catch (UserException e) {
            throw new AnalysisException("Permission denied:" + e.getMessage());
        }
    }

    /**
     * Recursively obtaining columns in an expression
     */
    public static void getCols(Collection<? extends Expression> expressions, Set<String> cols) {
        for (Expression expression : expressions) {
            if (expression instanceof SlotReference) {
                cols.add(((SlotReference) expression).getName());
            }
            if (expression.children().size() != 0) {
                getCols(expression.children(), cols);
            }
        }
    }

    /**
     * cols need check priv
     */
    protected static class AuthCols {
        private boolean hasProject;
        private Set<String> cols;

        public AuthCols() {
            this.cols = Sets.newHashSet();
            this.hasProject = false;
        }

        public boolean isHasProject() {
            return hasProject;
        }

        public void setHasProject(boolean hasProject) {
            this.hasProject = hasProject;
        }

        public Set<String> getCols() {
            return cols;
        }

        public void setCols(Set<String> cols) {
            this.cols = cols;
        }
    }
}