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

package org.apache.doris.nereids.trees.plans.commands.call;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.jdbc.JdbcExternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;

import java.util.List;
import java.util.Objects;

/**
 *  EXECUTE_STMT("catalog_name", "stmt")
 */
public class CallExecuteStmtFunc extends CallFunc {

    private final UserIdentity user;
    private final String catalogName;
    private final String stmt;

    private CallExecuteStmtFunc(UserIdentity user, String catalogName, String stmt) {
        this.user = Objects.requireNonNull(user, "user is missing");
        this.catalogName = Objects.requireNonNull(catalogName, "catalogName is missing");
        this.stmt = Objects.requireNonNull(stmt, "stmt is missing");
    }

    /**
     * Create a CallFunc
     */
    public static CallFunc create(UserIdentity user, List<Expression> args) {
        if (args.size() != 2) {
            throw new AnalysisException("EXECUTE_STMT function must have 2 arguments: 'catalog name' and 'stmt'");
        }

        String catalogName = null;
        String stmt = null;
        for (int i = 0; i < args.size(); i++) {
            if (!(args.get(i).isConstant())) {
                throw new AnalysisException("Argument of EXECUTE_STMT function must be constant string");
            }
            if (!(args.get(i) instanceof Literal)) {
                throw new AnalysisException("Argument of EXECUTE_STMT function must be constant string");
            }
            Literal literal = (Literal) args.get(i);
            if (!literal.isStringLikeLiteral()) {
                throw new AnalysisException("Argument of EXECUTE_STMT function must be constant string");
            }

            String rawString = literal.getStringValue();
            switch (i) {
                case 0:
                    catalogName = rawString;
                    break;
                case 1:
                    stmt = rawString;
                    break;
                default:
                    throw new AnalysisException("EXECUTE_STMT function must have 2 arguments");
            }
        }
        return new CallExecuteStmtFunc(user, catalogName, stmt);
    }

    @Override
    public void run() {
        CatalogIf catalogIf = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
        if (catalogIf == null) {
            throw new AnalysisException("catalog not found: " + catalogName);
        }
        if (!(catalogIf instanceof JdbcExternalCatalog)) {
            throw new AnalysisException("Only support JDBC catalog");
        }

        // check priv
        if (!Env.getCurrentEnv().getAuth().checkCtlPriv(user, catalogName, PrivPredicate.LOAD)) {
            throw new AnalysisException("user " + user + " has no privilege to execute stmt in catalog " + catalogName);
        }

        JdbcExternalCatalog catalog = (JdbcExternalCatalog) catalogIf;
        catalog.executeStmt(stmt);
    }
}
