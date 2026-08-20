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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.util.PlanUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.List;
import java.util.TreeMap;

/**
 * CreateViewInfo
 */
public class CreateViewInfo extends BaseViewInfo {
    private final boolean ifNotExists;
    private final boolean orReplace;
    private final String comment;
    private String inlineViewDef;

    /** constructor*/
    public CreateViewInfo(boolean ifNotExists, boolean orReplace, TableNameInfo viewName, String comment,
                          String querySql, List<SimpleColumnDefinition> simpleColumnDefinitions) {
        super(viewName, querySql, simpleColumnDefinitions);
        this.ifNotExists = ifNotExists;
        this.orReplace = orReplace;
        this.comment = Strings.nullToEmpty(comment);
    }

    /** init */
    public void init(ConnectContext ctx) throws UserException {
        viewName.analyze(ctx.getNameSpaceContext());
        FeNameFormat.checkTableName(viewName.getTbl());
        // disallow external catalog
        Util.prohibitExternalCatalog(viewName.getCtl(), "CreateViewCommand");
        // check privilege
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx,
                new TableNameInfo(viewName.getCtl(), viewName.getDb(), viewName.getTbl()), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.CREATE.getPrivs().toString(), viewName.getTbl());
        }
        TreeMap<Pair<Integer, Integer>, String> rewriteMap = ctx.getStatementContext().getIndexInSqlToString();
        TreeMap<Pair<Integer, Integer>, String> snapshot = new TreeMap<>(rewriteMap);
        String rewrittenSql;
        try {
            rewriteMap.clear();
            analyzeAndFillRewriteSqlMap(querySql, ctx);
            PlanUtils.OutermostPlanFinderContext outermostPlanFinderContext =
                    new PlanUtils.OutermostPlanFinderContext();
            analyzedPlan.accept(PlanUtils.OutermostPlanFinder.INSTANCE, outermostPlanFinderContext);
            List<Slot> outputs = outermostPlanFinderContext.outermostPlan.getOutput();
            createFinalCols(outputs);
            // expand star(*) in project list and replace table name with qualifier
            rewrittenSql = rewriteSql(rewriteMap, querySql);
            rewrittenSql = rewriteProjectsToUserDefineAlias(rewrittenSql);
        } finally {
            rewriteMap.clear();
            rewriteMap.putAll(snapshot);
        }
        checkViewSql(rewrittenSql);
        this.inlineViewDef = rewrittenSql;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public boolean isOrReplace() {
        return orReplace;
    }

    public TableNameInfo getViewName() {
        return this.viewName;
    }

    public String getComment() {
        return comment;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public List<Column> getColumns() {
        return this.finalCols;
    }
}
