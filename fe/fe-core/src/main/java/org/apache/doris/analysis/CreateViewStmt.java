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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class CreateViewStmt extends BaseViewStmt {
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
        tableName.analyze(analyzer);
        viewDefStmt.setNeedToSql(true);

        // check privilege
        if (!Catalog.getCurrentCatalog().getAuth()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(), PrivPredicate.CREATE)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "CREATE");
        }

        boolean originEnableVec = true;
        if (ConnectContext.get() != null) {
            // Do not rewrite nondeterministic functions to constant in create view's def stmt
            ConnectContext.get().setNotEvalNondeterministicFunction(true);
            // Because in current v1.1, the vec engine do not support some of outer join sql.
            // So it we set enable_vectorized_engine = true, it may throw VecNotImplementExcetion.
            // But it is not necessary because here we only neet to pass the analysis phase,
            // So here we temporarily set enable_vectorized_engine = false to avoid this expcetion.
            SessionVariable sv = ConnectContext.get().getSessionVariable();
            originEnableVec = sv.enableVectorizedEngine;
            sv.setEnableVectorizedEngine(false);
        }
        try {
            if (cols != null) {
                cloneStmt = viewDefStmt.clone();
            }

            // Analyze view define statement
            Analyzer viewAnalyzer = new Analyzer(analyzer);
            viewDefStmt.analyze(viewAnalyzer);

            createColumnAndViewDefs(analyzer);
        } finally {
            // must reset this flag, otherwise, all following query statement in this connection
            // will not do constant fold for nondeterministic functions.
            if (ConnectContext.get() != null) {
                ConnectContext.get().setNotEvalNondeterministicFunction(false);
                SessionVariable sv = ConnectContext.get().getSessionVariable();
                sv.setEnableVectorizedEngine(originEnableVec);
            }
        }
    }
}
