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
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.indexpolicy.IndexPolicyTypeEnum;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import lombok.Getter;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Create index policy statement.
 * syntax:
 * CREATE INVERTED INDEX ANALYZER [IF NOT EXISTS] policy_name PROPERTIES (key1 = value1, ...)
 * CREATE INVERTED INDEX TOKENIZER [IF NOT EXISTS] policy_name PROPERTIES (key1 = value1, ...)
 * CREATE INVERTED INDEX TOKEN_FILTER [IF NOT EXISTS] policy_name PROPERTIES (key1 = value1, ...)
 */
public class CreateIndexPolicyStmt extends DdlStmt implements NotFallbackInParser {

    @Getter
    private final boolean ifNotExists;

    @Getter
    private final String name;

    @Getter
    private final IndexPolicyTypeEnum type;

    @Getter
    private Map<String, String> properties;

    public CreateIndexPolicyStmt(boolean ifNotExists, String name, IndexPolicyTypeEnum type,
            Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.name = name;
        this.type = type;
        this.properties = properties;
    }

    @Override
    public void analyze() throws UserException {
        super.analyze();
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkIndexPolicyName(name);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ").append(type.name());
        if (ifNotExists) {
            sb.append(" IF NOT EXISTS");
        }
        sb.append(" ").append(name);
        if (properties != null && !properties.isEmpty()) {
            sb.append(" PROPERTIES(");
            sb.append(properties.entrySet().stream()
                    .map(entry -> entry.getKey() + " = " + entry.getValue())
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.CREATE;
    }
}
