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

import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * show snapshot command
 */
public class ShowSnapshotCommand extends ShowCommand {
    /**
     * only remote and local type
     */
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
    private Expression where;
    private String snapshotName;
    private String timestamp;
    private SnapshotType snapshotType = SnapshotType.REMOTE;

    /**
     * constructor
     */
    public ShowSnapshotCommand(String repoName, Expression where) {
        super(PlanType.SHOW_SNAPSHOT_COMMAND);
        this.repoName = repoName;
        this.where = where;
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
                builder.addColumn(new Column(title, ScalarType.STRING));
            }
        } else {
            for (String title : SNAPSHOT_ALL) {
                builder.addColumn(new Column(title, ScalarType.STRING));
            }
        }
        return builder.build();
    }

    private boolean analyzeSubExpression(Expression expr) {
        if (expr instanceof ComparisonPredicate) {
            return analyzeComparisonPredicate((ComparisonPredicate) expr);
        }
        if (expr instanceof And) {
            return expr.children().stream().allMatch(this::analyzeSubExpression);
        }

        return false;
    }

    private boolean analyzeComparisonPredicate(ComparisonPredicate expr) {
        Expression key = expr.child(0);
        Expression val = expr.child(1);

        if (!(key instanceof UnboundSlot)) {
            return false;
        }
        if (!(val instanceof StringLikeLiteral)) {
            return false;
        }

        String name = ((UnboundSlot) key).getName();
        if (name.equalsIgnoreCase("snapshot")) {
            snapshotName = ((StringLikeLiteral) val).getStringValue();
            if (Strings.isNullOrEmpty(snapshotName)) {
                return false;
            }
            return true;
        } else if (name.equalsIgnoreCase("timestamp")) {
            timestamp = ((StringLikeLiteral) val).getStringValue();
            if (Strings.isNullOrEmpty(timestamp)) {
                return false;
            }
            return true;
        } else if (name.equalsIgnoreCase("snapshotType")) {
            String snapshotTypeVal = ((StringLikeLiteral) val).getStringValue();
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

    /**
     * validate
     */
    @VisibleForTesting
    protected boolean validate(ConnectContext ctx) throws UserException {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        // validate analyze where clause if not null
        if (where != null) {
            if (!analyzeSubExpression(where)) {
                return false;
            }
            if (Strings.isNullOrEmpty(snapshotName) && !Strings.isNullOrEmpty(timestamp)) {
                // can not only set timestamp
                return false;
            }
        }

        return true;
    }

    /**
     * handle show backup
     */
    private ShowResultSet handleShowSnapshot(ConnectContext ctx, StmtExecutor executor) throws Exception {
        boolean valid = validate(ctx);
        if (!valid) {
            throw new AnalysisException("Where clause should looks like: SNAPSHOT = 'your_snapshot_name'"
                    + " [AND TIMESTAMP = '2018-04-18-19-19-10'] [AND SNAPSHOTTYPE = 'remote' | 'local']");
        }

        Repository repo = Env.getCurrentEnv().getBackupHandler().getRepoMgr().getRepo(repoName);
        if (repo == null) {
            throw new AnalysisException("Repository " + repoName + " does not exist");
        }
        List<List<String>> snapshotInfos = repo.getSnapshotInfos(snapshotName, timestamp);
        return new ShowResultSet(getMetaData(), snapshotInfos);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowSnapshot(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowSnapshotCommand(this, context);
    }
}
