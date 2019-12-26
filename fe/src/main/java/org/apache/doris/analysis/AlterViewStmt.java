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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

// Alter view statement
public class AlterViewStmt extends BaseViewStmt {

    private final List<Type> types;

    public AlterViewStmt(TableName tbl, List<ColWithComment> cols, QueryStmt queryStmt) {
        super(tbl, cols, queryStmt);
        types = Lists.newArrayList();
    }

    public TableName getTbl() {
        return tableName;
    }

    public List<Type> getTypes() {
        return types;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (tableName == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tableName.analyze(analyzer);

        if (!Catalog.getCurrentCatalog().getAuth().checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(),
                PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "ALTER VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    tableName.getTbl());
        }

        if (cols != null) {
            cloneStmt = viewDefStmt.clone();
        }
        viewDefStmt.setNeedToSql(true);
        Analyzer viewAnalyzer = new Analyzer(analyzer);

        viewDefStmt.analyze(viewAnalyzer);

        // save the types here to update the schema of view, or else we have to analyze inlineViewDef when init the view.
        for(int i = 0 ; i < viewDefStmt.getBaseTblResultExprs().size(); i++) {
            types.add(viewDefStmt.getBaseTblResultExprs().get(i).getType());
        }

        createColumnAndViewDefs(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER VIEW ").append(tableName.toSql()).append("\n");
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
        sb.append("AS ").append(viewDefStmt.toSql()).append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
