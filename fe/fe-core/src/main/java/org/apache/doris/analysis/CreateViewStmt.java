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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

@Deprecated
public class CreateViewStmt extends BaseViewStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(CreateViewStmt.class);

    private final boolean ifNotExists;
    private final String comment;

    public CreateViewStmt(boolean ifNotExists, TableName tableName, List<ColWithComment> cols,
            String comment, QueryStmt queryStmt) {
        super(tableName, cols, queryStmt);
        this.ifNotExists = ifNotExists;
        this.comment = Strings.nullToEmpty(comment);
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        FeNameFormat.checkTableName(tableName.getTbl());
        viewDefStmt.setNeedToSql(true);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        // check privilege
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(),
                        tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.CREATE.getPrivs().toString(), tableName.getTbl());
        }

        // Do not rewrite nondeterministic functions to constant in create view's def stmt
        if (ConnectContext.get() != null) {
            ConnectContext.get().setNotEvalNondeterministicFunction(true);
        }
        try {
            if (cols != null) {
                cloneStmt = viewDefStmt.clone();
                cloneStmt.forbiddenMVRewrite();
            }

            // Analyze view define statement
            Analyzer viewAnalyzer = new Analyzer(analyzer);
            viewDefStmt.forbiddenMVRewrite();
            viewDefStmt.analyze(viewAnalyzer);
            checkQueryAuth();
            createColumnAndViewDefs(viewAnalyzer);
        } finally {
            // must reset this flag, otherwise, all following query statement in this connection
            // will not do constant fold for nondeterministic functions.
            if (ConnectContext.get() != null) {
                ConnectContext.get().setNotEvalNondeterministicFunction(false);
            }
        }
    }

    public void setInlineViewDef(String querySql) {
        inlineViewDef = querySql;
    }

    public void setFinalColumns(List<Column> columns) {
        finalCols.addAll(columns);
    }
}
