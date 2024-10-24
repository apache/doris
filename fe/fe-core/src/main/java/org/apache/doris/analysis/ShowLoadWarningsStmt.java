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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;

// SHOW LOAD WARNINGS statement used to get error detail of src data.
public class ShowLoadWarningsStmt extends ShowStmt implements NotFallbackInParser {
    private static final Logger LOG = LogManager.getLogger(ShowLoadWarningsStmt.class);

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(15)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(15)))
                    .addColumn(new Column("ErrorMsgDetail", ScalarType.createVarchar(100)))
                    .build();

    private String dbName;
    private String rawUrl;
    private URL url;
    private Expr whereClause;
    private LimitElement limitElement;

    private String label;
    private long jobId;

    public ShowLoadWarningsStmt(String db, String url, Expr labelExpr,
                                LimitElement limitElement) {
        this.dbName = db;
        this.rawUrl = url;
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

    public URL getURL() {
        return url;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (rawUrl != null) {
            // get load error from url
            if (rawUrl.isEmpty()) {
                throw new AnalysisException("Error load url is missing");
            }

            if (dbName != null || whereClause != null || limitElement != null) {
                throw new AnalysisException(
                        "Can not set database, where or limit clause if getting error log from url");
            }

            // url should like:
            // http://be_ip:be_http_port/api/_load_error_log?file=__shard_xxx/error_log_xxx
            analyzeUrl();
        } else {
            if (Strings.isNullOrEmpty(dbName)) {
                dbName = analyzer.getDefaultDb();
                if (Strings.isNullOrEmpty(dbName)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
                }
            }

            // analyze where clause if not null
            if (whereClause == null) {
                throw new AnalysisException("should supply condition like: LABEL = \"your_load_label\","
                        + " or LOAD_JOB_ID = $job_id");
            }

            if (whereClause != null) {
                if (whereClause instanceof CompoundPredicate) {
                    CompoundPredicate cp = (CompoundPredicate) whereClause;
                    if (cp.getOp() != org.apache.doris.analysis.CompoundPredicate.Operator.AND) {
                        throw new AnalysisException("Only allow compound predicate with operator AND");
                    }

                    analyzeSubPredicate(cp.getChild(0));
                    analyzeSubPredicate(cp.getChild(1));
                } else {
                    analyzeSubPredicate(whereClause);
                }
            }
        }
    }

    private void analyzeUrl() throws AnalysisException {
        try {
            url = new URL(rawUrl);
        } catch (MalformedURLException e) {
            throw new AnalysisException("Invalid url: " + e.getMessage());
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
