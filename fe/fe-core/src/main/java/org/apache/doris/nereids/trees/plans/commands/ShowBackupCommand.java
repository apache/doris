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

import org.apache.doris.backup.AbstractJob;
import org.apache.doris.backup.BackupJob;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.CaseSensibility;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.PatternMatcherWrapper;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Like;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * show backup command
 */
public class ShowBackupCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId").add("SnapshotName").add("DbName").add("State").add("BackupObjs").add("CreateTime")
            .add("SnapshotFinishedTime").add("UploadFinishedTime").add("FinishedTime").add("UnfinishedTasks")
            .add("Progress").add("TaskErrMsg").add("Status").add("Timeout")
            .build();

    private String dbName;
    private Expression where;
    private boolean isAccurateMatch;
    private String snapshotName;

    /**
     * constructor
     */
    public ShowBackupCommand(String dbName, Expression where) {
        super(PlanType.SHOW_BACKUP_COMMAND);
        this.dbName = dbName;
        this.where = where;
    }

    /**
     * get metadata
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.STRING));
        }
        return builder.build();
    }

    /**
     * get label predicate for show backup
     */
    @VisibleForTesting
    protected Predicate<String> getSnapshotPredicate() throws AnalysisException {
        if (null == where) {
            return label -> true;
        }
        if (isAccurateMatch) {
            return CaseSensibility.LABEL.getCaseSensibility()
                    ? label -> label.equals(snapshotName) : label -> label.equalsIgnoreCase(snapshotName);
        } else {
            PatternMatcher patternMatcher = PatternMatcherWrapper.createMysqlPattern(
                    snapshotName, CaseSensibility.LABEL.getCaseSensibility());
            return patternMatcher::match;
        }
    }

    /**
     * validate
     */
    @VisibleForTesting
    protected boolean validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException("No database selected");
            }
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, dbName, PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    ConnectContext.get().getQualifiedUser(), dbName);
        }

        // SQL may be like : show backup from your_db_name; there is no where clause.
        if (where == null) {
            return true;
        }

        if (!(where instanceof Like) && !(where instanceof EqualTo)) {
            return false;
        }

        if (where instanceof EqualTo) {
            isAccurateMatch = true;
        }

        // left child
        if (!(where.child(0) instanceof UnboundSlot)) {
            return false;
        }
        String leftKey = ((UnboundSlot) where.child(0)).getName();
        if (!"snapshotname".equalsIgnoreCase(leftKey)) {
            return false;
        }

        // right child
        if (!(where.child(1) instanceof StringLikeLiteral)) {
            return false;
        }
        snapshotName = ((StringLikeLiteral) where.child(1)).getStringValue();
        if (Strings.isNullOrEmpty(snapshotName)) {
            return false;
        }

        return true;
    }

    /**
     * handle show backup
     */
    private ShowResultSet handleShowBackup(ConnectContext ctx, StmtExecutor executor) throws Exception {
        boolean valid = validate(ctx);
        if (!valid) {
            throw new AnalysisException("Where clause should like: SnapshotName = \"your_snapshot_name\", "
                + " or SnapshotName LIKE \"matcher\"");
        }

        DatabaseIf database = ctx.getCurrentCatalog().getDbOrAnalysisException(dbName);
        List<AbstractJob> jobs = Env.getCurrentEnv().getBackupHandler()
                .getJobs(database.getId(), getSnapshotPredicate());
        List<BackupJob> backupJobs = jobs.stream().filter(job -> job instanceof BackupJob)
                .map(job -> (BackupJob) job).collect(Collectors.toList());
        List<List<String>> infos = backupJobs.stream().map(BackupJob::getInfo).collect(Collectors.toList());

        return new ShowResultSet(getMetaData(), infos);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowBackup(ctx, executor);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowBackupCommand(this, context);
    }
}
