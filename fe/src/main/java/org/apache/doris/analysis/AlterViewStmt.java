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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

// Alter view statement
public class AlterViewStmt extends DdlStmt {

    private TableName tbl;

    private final List<ColWithComment> cols;
    private final QueryStmt queryStmt;

    // Set during analyze
    private final List<Column> finalCols;

    private String inlineViewDef;
    private QueryStmt cloneStmt;

    public List<Column> getColumns() {
        return finalCols;
    }

    public String getInlineViewDef() {
        return inlineViewDef;
    }

    public AlterViewStmt(TableName tbl, List<ColWithComment> cols, QueryStmt queryStmt) {
        this.tbl = tbl;
        this.cols = cols;
        this.queryStmt = queryStmt;
        finalCols = Lists.newArrayList();
    }

    public TableName getTbl() {
        return tbl;
    }

    /**
     * Sets the originalViewDef and the expanded inlineViewDef based on viewDefStmt.
     * If columnNames were given, checks that they do not contain duplicate column names
     * and throws an exception if they do.
     */
    private void createColumnAndViewDefs(Analyzer analyzer) throws AnalysisException, UserException {

        if (cols != null) {
            if (cols.size() != queryStmt.getColLabels().size()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_VIEW_WRONG_LIST);
            }
            for (int i = 0; i < cols.size(); ++i) {
                PrimitiveType type = queryStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                Column col = new Column(cols.get(i).getColName(), ScalarType.createType(type));
                col.setComment(cols.get(i).getComment());
                finalCols.add(col);
            }
        } else {
            for (int i = 0; i < queryStmt.getBaseTblResultExprs().size(); ++i) {
                PrimitiveType type = queryStmt.getBaseTblResultExprs().get(i).getType().getPrimitiveType();
                finalCols.add(new Column(
                        queryStmt.getColLabels().get(i),
                        ScalarType.createType(type)));
            }
        }
        // Set for duplicate columns
        Set<String> colSets = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
        for (Column col : finalCols) {
            if (!colSets.add(col.getName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_DUP_FIELDNAME, col.getName());
            }
        }

        if (cols == null) {
            inlineViewDef = queryStmt.toSql();
        } else {
            Analyzer tmpAnalyzer = new Analyzer(analyzer);
            List<String> colNames = cols.stream().map(c -> c.getColName()).collect(Collectors.toList());
            cloneStmt.substituteSelectList(tmpAnalyzer, colNames);
            inlineViewDef = cloneStmt.toSql();
        }
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(analyzer);

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tbl.getDb(), tbl.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tbl.getTbl());
        }

        if (cols != null) {
            cloneStmt = queryStmt.clone();
        }
        queryStmt.setNeedToSql(true);
        Analyzer viewAnalyzer = new Analyzer(analyzer);

        queryStmt.analyze(viewAnalyzer);
        createColumnAndViewDefs(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER VIEW ").append(tbl.toSql()).append("\n");
        if (cols != null) {
            sb.append("(\n");
            for (int i = 0 ; i < cols.size(); i++) {
                if (i != 0) {
                    sb.append(",\n");
                }
                sb.append("  ").append(cols.get(i).getColName());
            }
            sb.append("\n)");
        }
        sb.append("\n");
        sb.append("AS ").append(queryStmt.toSql()).append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
