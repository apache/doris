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
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * check select col auth
 */
public class SelectAuthChecker implements RewriteRuleFactory {
    private static final Logger LOG = LogManager.getLogger(SelectAuthChecker.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                // select a from tbl;
                logicalProject(logicalRelation()).then(project -> dealProjectRelation(project))
                        .toRule(RuleType.RELATION_AUTHENTICATION),
                // select * from tbl;
                logicalRelation().then(relation -> dealRelation(relation))
                        .toRule(RuleType.RELATION_AUTHENTICATION),
                // select * from tbl where a=1;
                logicalFilter(logicalRelation()).then(filter -> dealFilterRelation(filter))
                        .toRule(RuleType.RELATION_AUTHENTICATION),
                // select a from tbl where a=1;
                logicalProject(logicalFilter(logicalRelation())).then(project -> dealProjectFilterRelation(project))
                        .toRule(RuleType.RELATION_AUTHENTICATION)
        );
    }

    public static Plan dealFilterRelation(LogicalFilter<LogicalRelation> filter) {
        Plan plan = filter.child(0);
        if (!(plan instanceof CatalogRelation)) {
            return filter;
        }
        checkSelectAuth((CatalogRelation) plan, null);
        return filter;
    }

    public static Plan dealRelation(LogicalRelation relation) {
        if (!(relation instanceof CatalogRelation)) {
            return relation;
        }
        checkSelectAuth((CatalogRelation) relation, null);
        return relation;
    }

    /**
     * deal select with filter
     */
    public static Plan dealProjectFilterRelation(LogicalProject<LogicalFilter<LogicalRelation>> project) {
        Plan plan = project.child(0);
        if (!(plan instanceof LogicalFilter)) {
            return project;
        }
        LogicalFilter filter = (LogicalFilter) plan;
        Plan relation = filter.child(0);
        if (!(relation instanceof CatalogRelation)) {
            return project;
        }
        Set<String> cols = Sets.newHashSet();
        // get cols from filter
        getCols(filter.getConjuncts(), cols);
        // get cols from project
        getCols(project.getProjects(), cols);
        checkSelectAuth((CatalogRelation) relation, cols);
        return project;
    }

    /**
     * deal select with no filter
     */
    public static LogicalProject dealProjectRelation(
            LogicalProject<LogicalRelation> project) {
        Plan relation = project.child(0);
        if (!(relation instanceof CatalogRelation)) {
            return project;
        }
        Set<String> cols = Sets.newHashSet();
        // get cols from project
        getCols(project.getProjects(), cols);
        checkSelectAuth((CatalogRelation) relation, cols);
        return project;
    }

    /**
     * check col select auth
     */
    public static void checkSelectAuth(CatalogRelation catalogRelation, Set<String> cols) {
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

        if (CollectionUtils.isEmpty(cols)) {
            cols = table.getColumns().stream().map(Column::getName).collect(Collectors.toSet());
        }
        try {
            Env.getCurrentEnv().getAccessManager()
                    .checkColumnsPriv(ConnectContext.get().getCurrentUserIdentity(), catalog.getName(),
                            database.getFullName(), table.getName(), cols, PrivPredicate.SELECT);
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
}
