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
import org.apache.doris.indexpolicy.IndexPolicy;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import lombok.Getter;

/**
 * Show index policy statement.
 * syntax:
 * SHOW INVERTED INDEX ANALYZER;
 * SHOW INVERTED INDEX TOKENIZER;
 * SHOW INVERTED INDEX TOKEN_FILTER;
 **/
public class ShowIndexPolicyStmt extends ShowStmt implements NotFallbackInParser {

    @Getter
    private final IndexPolicyTypeEnum type;

    public ShowIndexPolicyStmt(IndexPolicyTypeEnum type) {
        this.type = type;
    }

    @Override
    public void analyze() throws UserException {
        super.analyze();
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW INDEX ").append(type.name());
        return sb.toString();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return IndexPolicy.INDEX_POLICY_META_DATA;
    }
}
