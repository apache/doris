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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

/**
 * CLEAN LABEL FROM db;
 * CLEAN LABEL my_label FROM db;
 */
public class CleanLabelStmt extends DdlStmt {
    private String db;
    private String label;

    public CleanLabelStmt(String db, String label) {
        this.db = db;
        this.label = label;
    }

    public String getDb() {
        return db;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        label = Strings.nullToEmpty(label);
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), db, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }
    }

    @Override
    public String toSql() {
        return "CLEAN LABEL" + (Strings.isNullOrEmpty(label) ? " " : " " + label) + " FROM " + db;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
