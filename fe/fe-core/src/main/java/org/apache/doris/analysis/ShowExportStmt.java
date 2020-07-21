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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.proc.ExportProcNode;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.load.ExportJob.JobState;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

// SHOW EXPORT STATUS statement used to get status of load job.
//
// syntax:
//      SHOW EXPORT [FROM db] [LIKE mask]
// TODO(lingbin): remove like predicate because export do not have label string
public class ShowExportStmt extends ShowStmt {
    private static final Logger LOG = LogManager.getLogger(ShowExportStmt.class);

    private String dbName;
    private Expr whereClause;
    private LimitElement limitElement;
    private List<OrderByElement> orderByElements;

    private long jobId = 0;
    private String stateValue = null;

    private JobState jobState;

    private ArrayList<OrderByPair> orderByPairs;

    public ShowExportStmt(String db, Expr whereExpr, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.dbName = db;
        this.whereClause = whereExpr;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public String getDbName() {
        return dbName;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public long getJobId() {
        return this.jobId;
    }

    public JobState getJobState() {
        if (Strings.isNullOrEmpty(stateValue)) {
            return null;
        }
        return jobState;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
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
        if (whereClause != null) {
            analyzePredicate(whereClause);
        }

        // order by
        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<OrderByPair>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = ExportProcNode.analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void analyzePredicate(Expr whereExpr) throws AnalysisException {
        if (whereExpr == null) {
            return;
        }

        boolean valid = true;
        boolean hasJobId = false;
        boolean hasState = false;
        
        CHECK: {
            // check predicate type
            if (whereExpr instanceof BinaryPredicate) {
                BinaryPredicate binaryPredicate = (BinaryPredicate) whereExpr;
                if (binaryPredicate.getOp() != Operator.EQ) {
                    valid = false;
                    break CHECK;
                }
            } else {
                valid = false;
                break CHECK;
            }
            
            // left child
            if (!(whereExpr.getChild(0) instanceof SlotRef)) {
                valid = false;
                break CHECK;
            }
            String leftKey = ((SlotRef) whereExpr.getChild(0)).getColumnName();
            if (leftKey.equalsIgnoreCase("id")) {
                hasJobId = true;
            } else if (leftKey.equalsIgnoreCase("state")) {
                hasState = true;
            } else {
                valid = false;
                break CHECK;
            }
            
            // right child
            if (hasState) {
                if (!(whereExpr.getChild(1) instanceof StringLiteral)) {
                    valid = false;
                    break CHECK;
                }

                String value = ((StringLiteral) whereExpr.getChild(1)).getStringValue();
                if (Strings.isNullOrEmpty(value)) {
                    valid = false;
                    break CHECK;
                }

                stateValue = value.toUpperCase();

                try {
                    jobState = JobState.valueOf(stateValue);
                } catch (IllegalArgumentException e) {
                    LOG.warn("illegal state argument in export stmt. stateValue={}, error={}", stateValue, e);
                    valid = false;
                    break CHECK;
                }
            } else if (hasJobId) {
                if (!(whereExpr.getChild(1) instanceof IntLiteral)) {
                    valid = false;
                    break CHECK;
                }
                jobId = ((IntLiteral) whereExpr.getChild(1)).getLongValue();
            }
        }
        

        if (!valid) {
            throw new AnalysisException("Where clause should looks like below: "
                    + " job_id = $your_job_id, or STATE = \"PENDING|EXPORTING|FINISHED|CANCELLED\"");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW EXPORT ");
        if (!Strings.isNullOrEmpty(dbName)) {
            sb.append("FROM `").append(dbName).append("`");
        }

        if (whereClause != null) {
            sb.append(" WHERE ").append(whereClause.toSql());
        }

        // Order By clause
        if (orderByElements != null) {
            sb.append(" ORDER BY ");
            for (int i = 0; i < orderByElements.size(); ++i) {
                sb.append(orderByElements.get(i).getExpr().toSql());
                sb.append((orderByElements.get(i).getIsAsc()) ? " ASC" : " DESC");
                sb.append((i + 1 != orderByElements.size()) ? ", " : "");
            }
        }

        if (getLimit() != -1L) {
            sb.append(" LIMIT ").append(getLimit());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : ExportProcNode.TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
