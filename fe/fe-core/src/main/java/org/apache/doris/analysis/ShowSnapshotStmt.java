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

import org.apache.doris.analysis.CompoundPredicate.Operator;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

public class ShowSnapshotStmt extends ShowStmt implements NotFallbackInParser {
    public enum SnapshotType {
        REMOTE,
        LOCAL
    }

    public static final ImmutableList<String> SNAPSHOT_ALL = new ImmutableList.Builder<String>()
            .add("Snapshot").add("Timestamp").add("Status")
            .build();
    public static final ImmutableList<String> SNAPSHOT_DETAIL = new ImmutableList.Builder<String>()
            .add("Snapshot").add("Timestamp").add("Database").add("Details").add("Status")
            .build();

    private String repoName;
    private Expr where;
    private String snapshotName;
    private String timestamp;
    private SnapshotType snapshotType = SnapshotType.REMOTE;

    public ShowSnapshotStmt(String repoName, Expr where) {
        this.repoName = repoName;
        this.where = where;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        // analyze where clause if not null
        if (where != null) {
            // eg: WHERE snapshot="snapshot_label" [and timestamp="2018-04-19-11-11:11"];
            boolean ok = true;
            CHECK: {
                if (where instanceof BinaryPredicate) {
                    if (!analyzeSubExpr((BinaryPredicate) where)) {
                        ok = false;
                        break CHECK;
                    }
                } else if (where instanceof CompoundPredicate) {
                    CompoundPredicate cp = (CompoundPredicate) where;
                    if (cp.getOp() != Operator.AND) {
                        ok = false;
                        break CHECK;
                    }

                    if (!(cp.getChild(0) instanceof BinaryPredicate)
                            || !(cp.getChild(1) instanceof BinaryPredicate)) {
                        ok = false;
                        break CHECK;
                    }

                    if (!analyzeSubExpr((BinaryPredicate) cp.getChild(0))
                            || !analyzeSubExpr((BinaryPredicate) cp.getChild(1))) {
                        ok = false;
                        break CHECK;
                    }
                }
            }

            if (ok && (Strings.isNullOrEmpty(snapshotName) && !Strings.isNullOrEmpty(timestamp))) {
                // can not only set timestamp
                ok = false;
            }

            if (!ok) {
                throw new AnalysisException("Where clause should looks like: SNAPSHOT = 'your_snapshot_name'"
                        + " [AND TIMESTAMP = '2018-04-18-19-19-10'] [AND SNAPSHOTTYPE = 'remote' | 'local']");
            }
        }
    }

    private boolean analyzeSubExpr(BinaryPredicate expr) {
        Expr key = expr.getChild(0);
        Expr val = expr.getChild(1);

        if (!(key instanceof SlotRef)) {
            return false;
        }
        if (!(val instanceof StringLiteral)) {
            return false;
        }

        String name = ((SlotRef) key).getColumnName();
        if (name.equalsIgnoreCase("snapshot")) {
            snapshotName = ((StringLiteral) val).getStringValue();
            if (Strings.isNullOrEmpty(snapshotName)) {
                return false;
            }
            return true;
        } else if (name.equalsIgnoreCase("timestamp")) {
            timestamp = ((StringLiteral) val).getStringValue();
            if (Strings.isNullOrEmpty(timestamp)) {
                return false;
            }
            return true;
        } else if (name.equalsIgnoreCase("snapshotType")) {
            String snapshotTypeVal = ((StringLiteral) val).getStringValue();
            if (Strings.isNullOrEmpty(snapshotTypeVal)) {
                return false;
            }
            // snapshotType now only support "remote" and "local"
            switch (snapshotTypeVal.toLowerCase()) {
                case "remote":
                    snapshotType = SnapshotType.REMOTE;
                    return true;
                case "local":
                    snapshotType = SnapshotType.LOCAL;
                    return true;
                default:
                    return false;
            }
        } else {
            return false;
        }
    }

    public String getRepoName() {
        return repoName;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getSnapshotType() {
        return snapshotType.name();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        if (!Strings.isNullOrEmpty(snapshotName) && !Strings.isNullOrEmpty(timestamp)) {
            for (String title : SNAPSHOT_DETAIL) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        } else {
            for (String title : SNAPSHOT_ALL) {
                builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
            }
        }
        return builder.build();
    }

}
