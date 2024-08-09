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
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;

public class DropFunctionStmt extends DdlStmt {
    private final boolean ifExists;
    private final FunctionName functionName;
    private final FunctionArgsDef argsDef;
    private SetType type = SetType.DEFAULT;

    // set after analyzed
    private FunctionSearchDesc function;

    public DropFunctionStmt(SetType type, boolean ifExists, FunctionName functionName, FunctionArgsDef argsDef) {
        this.type = type;
        this.ifExists = ifExists;
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    public SetType getType() {
        return type;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public FunctionSearchDesc getFunction() {
        return function;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // analyze function name
        functionName.analyze(analyzer, this.type);

        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // analyze arguments
        argsDef.analyze(analyzer);
        function = new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic());
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP FUNCTION ").append(functionName).append(argsDef);
        return stringBuilder.toString();
    }

    public String signatureString() {
        StringBuilder sb = new StringBuilder();
        sb.append(functionName.getFunction()).append("(").append(Joiner.on(", ").join(argsDef.getArgTypes()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
