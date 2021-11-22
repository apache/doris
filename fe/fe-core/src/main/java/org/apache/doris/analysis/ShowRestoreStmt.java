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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.function.Predicate;

public class ShowRestoreStmt extends ShowStmt {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("Label").add("Timestamp").add("DbName").add("State")
            .add("AllowLoad").add("ReplicationNum").add("ReplicaAllocation")
            .add("RestoreObjs").add("CreateTime").add("MetaPreparedTime").add("SnapshotFinishedTime")
            .add("DownloadFinishedTime").add("FinishedTime").add("UnfinishedTasks").add("Progress")
            .add("TaskErrMsg").add("Status").add("Timeout")
            .build();

    private String dbName;
    private Expr where;
    private String labelValue;
    private boolean isAccurateMatch;

    public ShowRestoreStmt(String dbName, Expr where) {
        this.dbName = dbName;
        this.where = where;
    }

    public String getDbName() {
        return dbName;
    }

    public String getLabelValue() {
        return labelValue;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), dbName);
        }

        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), dbName, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED,
                    ConnectContext.get().getQualifiedUser(), dbName);
        }

        if (where == null) {
            return;
        }
        boolean valid = analyzeWhereClause();
        if (!valid) {
            throw new AnalysisException("Where clause should like: LABEL = \"your_label_name\", "
                    + " or LABEL LIKE \"matcher\"");
        }
    }

    private boolean analyzeWhereClause() {
        if (!(where instanceof LikePredicate) && !(where instanceof BinaryPredicate)) {
            return false;
        }

        if (where instanceof BinaryPredicate) {
            BinaryPredicate binaryPredicate = (BinaryPredicate) where;
            if (BinaryPredicate.Operator.EQ != binaryPredicate.getOp()) {
                return false;
            }
            isAccurateMatch = true;
        }

        if (where instanceof LikePredicate) {
            LikePredicate likePredicate = (LikePredicate) where;
            if (LikePredicate.Operator.LIKE != likePredicate.getOp()) {
                return false;
            }
        }

        // left child
        if (!(where.getChild(0) instanceof SlotRef)) {
            return false;
        }
        String leftKey = ((SlotRef) where.getChild(0)).getColumnName();
        if (!"label".equalsIgnoreCase(leftKey)) {
            return false;
        }

        // right child
        if (!(where.getChild(1) instanceof StringLiteral)) {
            return false;
        }
        labelValue = ((StringLiteral) where.getChild(1)).getStringValue();
        if (Strings.isNullOrEmpty(labelValue)) {
            return false;
        }

        return true;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public String toSql() {
        StringBuilder builder = new StringBuilder();
        builder.append("SHOW RESTORE");
        if (dbName != null) {
            builder.append(" FROM `").append(dbName).append("` ");
        }

        builder.append(where.toSql());
        return builder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    public boolean isAccurateMatch() {
        return isAccurateMatch;
    }

    public Expr getWhere() {
        return where;
    }

    public Predicate<String> getLabelPredicate() throws AnalysisException {
        if (null == where) {
            return label -> true;
        }
        if (isAccurateMatch) {
            return CaseSensibility.LABEL.getCaseSensibility() ? label -> label.equals(labelValue) : label -> label.equalsIgnoreCase(labelValue);
        } else {
            PatternMatcher patternMatcher = PatternMatcher.createMysqlPattern(labelValue, CaseSensibility.LABEL.getCaseSensibility());
            return patternMatcher::match;
        }
    }
}

