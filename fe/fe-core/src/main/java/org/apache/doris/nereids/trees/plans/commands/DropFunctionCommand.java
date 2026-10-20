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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.FunctionSearchDesc;
import org.apache.doris.catalog.FunctionUtil;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.FunctionArgTypesInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Joiner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * drop a alias or user defined function
 */
public class DropFunctionCommand extends Command implements ForwardWithSync {
    private SetType setType;
    private final boolean ifExists;
    private final FunctionName functionName;
    private final FunctionArgTypesInfo argsDef;

    /**
     * DropFunctionCommand
     */
    public DropFunctionCommand(SetType setType, boolean ifExists, FunctionName functionName,
                                 FunctionArgTypesInfo argsDef) {
        super(PlanType.CREATE_FUNCTION_COMMAND);
        this.setType = setType;
        this.ifExists = ifExists;
        this.functionName = functionName;
        this.argsDef = argsDef;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        argsDef.analyze();
        FunctionSearchDesc function = new FunctionSearchDesc(functionName, argsDef.getArgTypes(), argsDef.isVariadic());

        List<Long> functionIds;
        if (SetType.GLOBAL.equals(setType)) {
            functionIds = Env.getCurrentEnv().getGlobalFunctionMgr().dropFunction(function, ifExists);
        } else {
            String dbName = functionName.getDb();
            if (dbName == null) {
                dbName = ctx.getDatabase();
                functionName.setDb(dbName);
            }
            Database db = Env.getCurrentInternalCatalog().getDbNullable(dbName);
            if (db == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            functionIds = db.dropFunction(function, ifExists);
        }
        String functionSignature = getSignatureString();
        Map<Long, String> functionSignatures = new HashMap<>();
        for (long functionId : functionIds) {
            functionSignatures.put(functionId, functionSignature);
        }
        FunctionUtil.cleanUdfCache(functionSignatures);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropFunctionCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }

    private String getSignatureString() {
        StringBuilder sb = new StringBuilder();
        sb.append(functionName.getFunction()).append("(").append(Joiner.on(", ").join(argsDef.getArgTypes()));
        sb.append(")");
        return sb.toString();
    }
}
