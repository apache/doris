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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.ColWithComment;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/**
 * CreateViewInfo
 */
public class CreateViewInfo {
    private CreateViewStmt createViewStmt;
    private final boolean ifNotExists;
    private final TableNameInfo viewName;
    private final String comment;
    private final LogicalPlan logicalQuery;
    private final String querySql;
    private final List<SimpleColumnDefinition> simpleColumnDefinitions;

    /** constructor*/
    public CreateViewInfo(boolean ifNotExists, TableNameInfo viewName, String comment, LogicalPlan logicalQuery,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        this.ifNotExists = ifNotExists;
        this.viewName = viewName;
        this.comment = comment;
        if (logicalQuery instanceof LogicalFileSink) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
        this.logicalQuery = logicalQuery;
        this.querySql = querySql;
        this.simpleColumnDefinitions = simpleColumnDefinitions;
    }

    /**translateToLegacyStmt*/
    public CreateViewStmt translateToLegacyStmt(ConnectContext ctx) {
        viewName.analyze(ctx);
        List<ColWithComment> cols = Lists.newArrayList();
        for (SimpleColumnDefinition def : simpleColumnDefinitions) {
            cols.add(def.transferToColWithComment());
        }
        createViewStmt = new CreateViewStmt(ifNotExists, viewName.transferToTableName(), cols, comment,
                null);
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        Plan plan = planner.plan(new UnboundResultSink<>(logicalQuery), PhysicalProperties.ANY, ExplainLevel.NONE);
        createViewStmt.createColumnAndViewDefsForNereids(querySql, plan);
        return createViewStmt;
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {
        FeNameFormat.checkTableName(viewName.getTbl());
        // disallow external catalog
        Util.prohibitExternalCatalog(viewName.getCtl(), "CreateViewStmt");
        // check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), viewName.getDb(),
                viewName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }
        // Do not rewrite nondeterministic functions to constant in create view's def stmt
        // ctx.setNotEvalNondeterministicFunction(true);
        List<Column> finalCols = createViewStmt.getFinalColumns();
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }
    }
}
