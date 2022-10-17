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

package org.apache.doris.nereids.glue;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used for the compatibility and code reuse in.
 * TODO: rethink it, LogicalPlanAdapter should not bind with Query yet, so we need to do some refactor in StmtExecutor
 * @see org.apache.doris.qe.ConnectProcessor
 */
public class LogicalPlanAdapter extends StatementBase implements Queriable {

    private final StatementContext statementContext;
    private final LogicalPlan logicalPlan;
    private List<Expr> resultExprs;
    private ArrayList<String> colLabels;

    public LogicalPlanAdapter(LogicalPlan logicalPlan, StatementContext statementContext) {
        this.logicalPlan = logicalPlan;
        this.statementContext = statementContext;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    @Override
    public boolean hasOutFileClause() {
        return false;
    }

    @Override
    public OutFileClause getOutFileClause() {
        return null;
    }

    public ArrayList<String> getColLabels() {
        return colLabels;
    }

    @Override
    public List<Expr> getResultExprs() {
        return resultExprs;
    }

    public void setResultExprs(List<Expr> resultExprs) {
        this.resultExprs = resultExprs;
    }

    public void setColLabels(ArrayList<String> colLabels) {
        this.colLabels = colLabels;
    }

    public StatementContext getStatementContext() {
        return statementContext;
    }

    public String toDigest() {
        // TODO: generate real digest
        return "";
    }

    public static LogicalPlanAdapter of(Plan plan) {
        return new LogicalPlanAdapter((LogicalPlan) plan, null);
    }
}
