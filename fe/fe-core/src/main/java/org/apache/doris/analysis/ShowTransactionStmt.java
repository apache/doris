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

import org.apache.doris.analysis.BinaryPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.TransProcDir;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// syntax:
//      SHOW TRANSACTION  WHERE id=123
public class ShowTransactionStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowTransactionStmt.class);

    private String dbName;
    private Expr whereClause;
    private long txnId = -1;
    private String label = "";
    private TransactionStatus status = TransactionStatus.UNKNOWN;

    public ShowTransactionStmt(String dbName, Expr whereClause) {
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

    public TransactionStatus getStatus() {
        return status;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
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
            if (whereClause instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereClause;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }

            // left child
            if (!(whereClause.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) whereClause.getChild(0)).getColumnName();
            if (leftKey.equalsIgnoreCase("id") && (whereClause.getChild(1) instanceof IntLiteral)) {
                txnId = ((IntLiteral) whereClause.getChild(1)).getLongValue();
            } else if (leftKey.equalsIgnoreCase("label") && (whereClause.getChild(1) instanceof StringLiteral)) {
                label = ((StringLiteral) whereClause.getChild(1)).getStringValue();
            } else if (leftKey.equalsIgnoreCase("status") && (whereClause.getChild(1) instanceof StringLiteral)) {
                String txnStatus = ((StringLiteral) whereClause.getChild(1)).getStringValue();
                try {
                    status = TransactionStatus.valueOf(txnStatus.toUpperCase());
                } catch (Exception e) {
                    throw new AnalysisException("status should be prepare/precommitted/committed/visible/aborted");
                }
                if (status == TransactionStatus.UNKNOWN) {
                    throw new AnalysisException("status should be prepare/precommitted/committed/visible/aborted");
                }
            } else {
                valid = false;
            }
        }

        if (!valid) {
            throw new AnalysisException("Where clause should looks like one of them: id = 123 or label = 'label' "
                    + "or status = 'prepare/precommitted/committed/visible/aborted'");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW TRANSACTION ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }

        sb.append(" WHERE ").append(whereClause.toSql());
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TransProcDir.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
