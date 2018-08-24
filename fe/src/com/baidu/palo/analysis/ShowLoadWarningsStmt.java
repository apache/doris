// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.analysis;

import com.baidu.palo.catalog.Column;
import com.baidu.palo.catalog.ColumnType;
import com.baidu.palo.cluster.ClusterNamespace;
import com.baidu.palo.common.AnalysisException;
import com.baidu.palo.common.ErrorCode;
import com.baidu.palo.common.ErrorReport;
import com.baidu.palo.common.InternalException;
import com.baidu.palo.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// SHOW LOAD WARNINGS statement used to get error detail of src data.
public class ShowLoadWarningsStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowLoadWarningsStmt.class);

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ColumnType.createVarchar(15)))
                    .addColumn(new Column("Label", ColumnType.createVarchar(15)))
                    .addColumn(new Column("ErrorMsgDetail", ColumnType.createVarchar(100)))
                    .build();

    private String dbName;
    private Expr whereClause;
    private LimitElement limitElement;

    private String label;
    private long jobId;

    public ShowLoadWarningsStmt(String db, Expr labelExpr,
                                LimitElement limitElement) {
        this.dbName = db;
        this.whereClause = labelExpr;
        this.limitElement = limitElement;

        this.label = null;
        this.jobId = 0;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLabel() {
        return label;
    }

    public long getJobId() {
        return jobId;
    }

    public long getLimitNum() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public boolean isFindByLabel() {
        return label != null;
    }

    public boolean isFindByJobId() {
        return jobId != 0;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        // analyze where clause if not null
        if (whereClause == null) {
            throw new AnalysisException("should supply condition like: LABEL = \"your_load_label\","
                    + " or LOAD_JOB_ID = $job_id");
        }

        if (whereClause != null) {
            if (whereClause instanceof CompoundPredicate) {
                CompoundPredicate cp = (CompoundPredicate) whereClause;
                if (cp.getOp() != com.baidu.palo.analysis.CompoundPredicate.Operator.AND) {
                    throw new AnalysisException("Only allow compound predicate with operator AND");
                }

                analyzeSubPredicate(cp.getChild(0));
                analyzeSubPredicate(cp.getChild(1));
            } else {
                analyzeSubPredicate(whereClause);
            }
        }

    }

    private void analyzeSubPredicate(Expr subExpr) throws AnalysisException {
        boolean valid = false;
        boolean hasLabel = false;
        boolean hasLoadJobId = false;
        do {
            if (subExpr == null) {
                valid = false;
                break;
            }

            if (subExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) subExpr;
                if (binaryPredicate.getOp() != BinaryPredicate.Operator.EQ) {
                    valid = false;
                    break;
                }
            } else {
                valid = false;
                break;
            }

            // left child
            if (!(subExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break;
            }
            String leftKey = ((SlotRef) subExpr.getChild(0)).getColumnName();
            if (leftKey.equalsIgnoreCase("label")) {
                hasLabel = true;
            } else if (leftKey.equalsIgnoreCase("load_job_id")) {
                hasLoadJobId = true;
            } else {
                valid = false;
                break;
            }

            if (hasLabel) {
                if (!(subExpr.getChild(1) instanceof StringLiteral)) {
                    valid = false;
                    break;
                }

                String value = ((StringLiteral) subExpr.getChild(1)).getStringValue();
                if (Strings.isNullOrEmpty(value)) {
                    valid = false;
                    break;
                }

                label = value;
            }

            if (hasLoadJobId) {
                if (!(subExpr.getChild(1) instanceof IntLiteral)) {
                    LOG.warn("load_job_id is not IntLiteral. value: {}", subExpr.toSql());
                    valid = false;
                    break;
                }
                jobId = ((IntLiteral) subExpr.getChild(1)).getLongValue();
            }

            valid = true;
        } while (false);

        if (!valid) {
            throw new AnalysisException("Where clause should looks like: LABEL = \"your_load_label\","
                    + " or LOAD_JOB_ID = $job_id");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
}
