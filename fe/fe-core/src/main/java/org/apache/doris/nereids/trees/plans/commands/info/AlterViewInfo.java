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

import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.ColWithComment;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

/** AlterViewInfo */
public class AlterViewInfo extends BaseViewInfo {
    /** constructor*/
    public AlterViewInfo(TableNameInfo viewName,
            String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        super(viewName, querySql, simpleColumnDefinitions);
    }

    /** init */
    public void init(ConnectContext ctx) throws UserException {
        if (viewName == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        viewName.analyze(ctx);
        FeNameFormat.checkTableName(viewName.getTbl());
        // disallow external catalog
        Util.prohibitExternalCatalog(viewName.getCtl(), "AlterViewStmt");

        DatabaseIf db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(viewName.getDb());
        TableIf table = db.getTableOrAnalysisException(viewName.getTbl());
        if (!(table instanceof View)) {
            throw new org.apache.doris.common.AnalysisException(
                    String.format("ALTER VIEW not allowed on a table:%s.%s", viewName.getDb(), viewName.getTbl()));
        }

        // check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, new TableName(viewName.getCtl(), viewName.getDb(),
                viewName.getTbl()), PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.ALTER.getPrivs().toString(), viewName.getTbl());
        }
        analyzeAndFillRewriteSqlMap(querySql, ctx);
        PlanUtils.OutermostPlanFinderContext outermostPlanFinderContext = new PlanUtils.OutermostPlanFinderContext();
        analyzedPlan.accept(PlanUtils.OutermostPlanFinder.INSTANCE, outermostPlanFinderContext);
        List<Slot> outputs = outermostPlanFinderContext.outermostPlan.getOutput();
        createFinalCols(outputs);
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.planWithLock(new UnboundResultSink<>(logicalQuery), PhysicalProperties.ANY, ExplainLevel.NONE);
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }
    }

    /**translateToLegacyStmt*/
    public AlterViewStmt translateToLegacyStmt(ConnectContext ctx) {
        List<ColWithComment> cols = Lists.newArrayList();
        for (SimpleColumnDefinition def : simpleColumnDefinitions) {
            cols.add(def.translateToColWithComment());
        }
        AlterViewStmt alterViewStmt = new AlterViewStmt(viewName.transferToTableName(), cols,
                null);
        // expand star(*) in project list and replace table name with qualifier
        String rewrittenSql = rewriteSql(ctx.getStatementContext().getIndexInSqlToString(), querySql);

        // rewrite project alias
        rewrittenSql = rewriteProjectsToUserDefineAlias(rewrittenSql);

        alterViewStmt.setInlineViewDef(rewrittenSql);
        alterViewStmt.setFinalColumns(finalCols);
        return alterViewStmt;
    }
}
