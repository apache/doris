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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ToSqlContext;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BaseViewStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(BaseViewStmt.class);

    protected final TableName tableName;
    protected final List<ColWithComment> cols;
    protected final QueryStmt viewDefStmt;

    // Set during analyze
    protected final List<Column> finalCols;

    protected String inlineViewDef;

    protected QueryStmt cloneStmt;

    public BaseViewStmt(TableName tableName, List<ColWithComment> cols, QueryStmt queryStmt) {
        this.tableName = tableName;
        this.cols = cols;
        this.viewDefStmt = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public String getDbName() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }


    public List<Column> getColumns() {
        return finalCols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    protected void checkQueryAuth() throws UserException {
        for (int i = 0; i < viewDefStmt.getBaseTblResultExprs().size(); ++i) {
            Expr expr = viewDefStmt.getBaseTblResultExprs().get(i);
            if (!(expr instanceof SlotRef)) {
                continue;
            }
            SlotRef slotRef = (SlotRef) expr;
            TableName queryTableName = slotRef.getTableName();
            if (queryTableName == null) {
                continue;
            }
            String queryColumnName = slotRef.getColumnName();
            String ctlName = StringUtils.isEmpty(queryTableName.getCtl()) ? InternalCatalog.INTERNAL_CATALOG_NAME
                    : queryTableName.getCtl();
            // check privilege
            Env.getCurrentEnv().getAccessManager()
                    .checkColumnsPriv(ConnectContext.get().getCurrentUserIdentity(), ctlName,
                            queryTableName.getDb(), queryTableName.getTbl(), Sets.newHashSet(queryColumnName),
                            PrivPredicate.SELECT);
        }
    }

    /**
     * Sets the originalViewDef and the expanded inlineViewDef based on viewDefStmt.
     * If columnNames were given, checks that they do not contain duplicate column names
     * and throws an exception if they do.
     */
    protected void createColumnAndViewDefs(Analyzer analyzer) throws AnalysisException, UserException {
        if (cols != null) {
            if (cols.size() != viewDefStmt.getColLabels().size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < cols.size(); ++i) {
                Type type = viewDefStmt.getBaseTblResultExprs().get(i).getType();
                Column col = new Column(cols.get(i).getColName(), type);
                col.setComment(cols.get(i).getComment());
                finalCols.add(col);
            }
        } else {
            for (int i = 0; i < viewDefStmt.getBaseTblResultExprs().size(); ++i) {
                Type type = viewDefStmt.getBaseTblResultExprs().get(i).getType();
                finalCols.add(new Column(viewDefStmt.getColLabels().get(i), type));
            }
        }
        // Set for duplicate columns
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }

        // format view def string
        if (cols == null) {
            try (ToSqlContext toSqlContext = ToSqlContext.getOrNewThreadLocalContext()) {
                // after being analyzed, the toSql() of SlotRef will output like "<slot 10> col as col",
                // we don't need the slot id info, so using ToSqlContext to remove it.
                toSqlContext.setNeedSlotRefId(false);
                inlineViewDef = viewDefStmt.toSql();
            }
            return;
        }

        Analyzer tmpAnalyzer = new Analyzer(analyzer);
        List<String> colNames = cols.stream().map(c -> c.getColName()).collect(Collectors.toList());
        cloneStmt.setNeedToSql(true);
        cloneStmt.substituteSelectList(tmpAnalyzer, colNames);

        try (ToSqlContext toSqlContext = ToSqlContext.getOrNewThreadLocalContext()) {
            // after being analyzed, the toSql() of SlotRef will output like "<slot 10> col as col",
            // we don't need the slot id info, so using ToSqlContext to remove it.
            toSqlContext.setNeedSlotRefId(false);
            inlineViewDef = cloneStmt.toSql();
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (viewDefStmt.hasOutFileClause()) {
            throw new AnalysisException("Not support OUTFILE clause in CREATE VIEW statement");
        }
    }
}
