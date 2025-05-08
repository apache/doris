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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.TransProcDir;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.GlobalTransactionMgrIface;

import com.google.common.base.Strings;

/**
 * ShowTransactionCommand
 */
public class ShowTransactionCommand extends ShowCommand {

    private String dbName;
    private Expression whereClause;
    private long txnId = -1;
    private String label = "";

    public ShowTransactionCommand(String dbName, Expression whereClause) {
        super(PlanType.SHOW_TRANSACTION_COMMAND);
        this.dbName = dbName;
        this.whereClause = whereClause;
    }

    public String getDbName() {
        return dbName;
    }

    public long getTxnId() {
        return txnId;
    }

    public String getLabel() {
        return label;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowTransaction(ctx);
    }

    private ShowResultSet handleShowTransaction(ConnectContext ctx) throws AnalysisException {
        DatabaseIf db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);
        GlobalTransactionMgrIface transactionMgr = Env.getCurrentGlobalTransactionMgr();
        if (!label.isEmpty()) {
            txnId = transactionMgr.getTransactionId(db.getId(), label);
            if (txnId == -1) {
                throw new AnalysisException("transaction with label " + label + " does not exist");
            }
        }
        return new ShowResultSet(getMetaData(), transactionMgr.getSingleTranInfo(db.getId(), txnId));
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }

        if (whereClause == null) {
            throw new AnalysisException("Missing transaction id");
        }
        analyzeWhereClause();
    }

    private void analyzeWhereClause() throws AnalysisException {
        if (whereClause == null) {
            return;
        }

        boolean valid = true;
        CHECK: {
            if (!(whereClause instanceof EqualTo)) {
                valid = false;
                break CHECK;
            }

            String left = whereClause.child(0).toString();
            if (left.equalsIgnoreCase("id") && (whereClause.child(1) instanceof IntegerLiteral)) {
                txnId = ((IntegerLiteral) whereClause.child(1)).getLongValue();
            } else if (left.equalsIgnoreCase("label") && (whereClause.child(1) instanceof StringLiteral)) {
                label = ((StringLiteral) whereClause.child(1)).getStringValue();
            } else {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like one of them: id = 123 or label = 'label'");
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTransactionCommand(this, context);
    }

    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
