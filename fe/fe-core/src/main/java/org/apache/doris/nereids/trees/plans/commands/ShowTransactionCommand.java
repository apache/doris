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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.proc.TransProcDir;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;

/**
 * ShowTransactionCommand
 */
public class ShowTransactionCommand extends ShowCommand {

    private String dbName;
    private Expression expr;
    private long txnId = -1;
    private String label = "";
    private TransactionStatus status = TransactionStatus.UNKNOWN;
    private boolean labelMatch = false;

    public ShowTransactionCommand(String dbName, Expression whereClause) {
        super(PlanType.SHOW_TRANSACTION_COMMAND);
        this.dbName = dbName;
        this.expr = whereClause;
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
        ShowResultSet resultSet;
        if (status != TransactionStatus.UNKNOWN) {
            resultSet = new ShowResultSet(getMetaData(), transactionMgr.getDbTransInfoByStatus(db.getId(), status));
        } else if (labelMatch && !label.isEmpty()) {
            resultSet = new ShowResultSet(getMetaData(), transactionMgr.getDbTransInfoByLabelMatch(db.getId(), label));
        } else {
            if (!label.isEmpty()) {
                Long transactionId = transactionMgr.getTransactionId(db.getId(), label);
                if (transactionId == null) {
                    throw new AnalysisException("transaction with label " + label + " does not exist");
                }
                txnId = transactionId;
            }
            resultSet = new ShowResultSet(getMetaData(), transactionMgr.getSingleTranInfo(db.getId(), txnId));
        }
        return resultSet;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                    PrivPredicate.LOAD.getPrivs().toString(), dbName);
        }
        if (expr == null) {
            throw new AnalysisException("Missing transaction id");
        }

        if (!analyzeWhereClause()) {
            throw new AnalysisException("Where clause should looks like one of them: id = 123 or label =/like 'label' "
                + "or status = 'prepare/precommitted/committed/visible/aborted'");
        }
    }

    private boolean analyzeWhereClause() throws AnalysisException {
        if (!(expr instanceof EqualTo || expr instanceof Like)) {
            return false;
        }

        if (!(expr.child(0) instanceof UnboundSlot)) {
            return false;
        }

        String leftKey = ((UnboundSlot) expr.child(0)).getName();
        Expression right = expr.child(1);
        if (leftKey.equalsIgnoreCase("id") && (right instanceof IntegerLikeLiteral)) {
            txnId = ((IntegerLikeLiteral) right).getLongValue();
        } else if (leftKey.equalsIgnoreCase("label") && (right instanceof StringLikeLiteral)) {
            label = ((StringLikeLiteral) right).getStringValue();
        } else if (leftKey.equalsIgnoreCase("status") && (right instanceof StringLiteral)) {
            String txnStatus = ((StringLiteral) right).getStringValue();
            try {
                status = TransactionStatus.valueOf(txnStatus.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException("status should be prepare/precommitted/committed/visible/aborted");
            }
            if (status == TransactionStatus.UNKNOWN) {
                throw new AnalysisException("status should be prepare/precommitted/committed/visible/aborted");
            }
        } else {
            return false;
        }

        if (expr instanceof Like && leftKey.equalsIgnoreCase("label")) {
            //Only supports label like matching
            labelMatch = true;
            label = label.replaceAll("%", ".*");
        }

        return true;
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

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
