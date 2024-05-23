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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Prepared Statement
 */
public class ExecuteCommand extends LogicalPlanAdapter  {
    private final String stmtName;
    private final List<Expression> params;

    private StatementContext statementContext;

    public ExecuteCommand(String stmtName, List<Expression> paramsExpr) {
        super(null, null);
        this.stmtName = stmtName;
        this.params = paramsExpr;
    }

    public List<Expression> getParams() {
        return params;
    }

    public String getStmtName() {
        return stmtName;
    }

    @Override
    public String toSql() {
        return "EXECUTE `" + stmtName + "`"
                + params.stream().map(Expression::toSql).collect(Collectors.joining(", ", " USING ", ""));
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return null;
    }

    @Override
    public StatementContext getStatementContext() {
        if (statementContext == null) {
            statementContext = new StatementContext(ConnectContext.get(), new OriginStatement("", 0));
        }
        return statementContext;
    }
}
