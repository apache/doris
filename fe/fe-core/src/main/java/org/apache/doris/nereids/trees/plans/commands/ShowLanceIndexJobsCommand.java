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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * SHOW LANCE INDEX JOBS [FROM [catalog.]db] [WHERE TableName = "tbl" [AND State = "PENDING"]].
 *
 * <p>Lists the durable Lance index job records held by the master. Rows whose persisted
 * target no longer resolves (the catalog is gone, or the catalog is there but the db or
 * table no longer resolves) are visible to global ADMIN only; every other row requires
 * table-level SHOW on the persisted (catalog, db, table). Rows that fail the check are
 * omitted entirely, so non-ADMIN users see no orphan trace, not even a count. The job
 * locator, provider, normalized names, propertiesJson and schema contract are never shown.
 *
 * <p>The WHERE clause is deliberately narrowed to EqualTo predicates combined with AND
 * over the case-insensitive keys TableName and State (no Like, unlike the SHOW COPY
 * precedent).
 */
public class ShowLanceIndexJobsCommand extends ShowCommand {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("JobId")
            .add("CatalogName")
            .add("DbName")
            .add("TableName")
            .add("IndexName")
            .add("Operation")
            .add("State")
            .add("RefreshState")
            .add("PossibleLive")
            .add("CreateTime")
            .add("UpdateTime")
            .add("Message")
            .add("ForceReleased")
            .add("ForceActor")
            .add("ForceTime")
            .build();

    private static final String KEY_TABLE_NAME = "TableName";
    private static final String KEY_STATE = "State";
    private static final String WHERE_HINT = "Where clause should looks like: TableName = \"your_table_name\""
            + " or State = \"PENDING|RUNNING|COMMITTED|NOT_COMMITTED|UNKNOWN\","
            + " or compound predicate with operator AND";

    private final List<String> nameParts;
    private final Expression whereClause;

    private String ctlName;
    private String dbName;
    private String tableNameValue;
    private String stateValue;

    public ShowLanceIndexJobsCommand(List<String> nameParts, Expression whereClause) {
        super(PlanType.SHOW_LANCE_INDEX_JOBS_COMMAND);
        this.nameParts = nameParts;
        this.whereClause = whereClause;
    }

    public List<String> getNameParts() {
        return nameParts;
    }

    public Expression getWhereClause() {
        return whereClause;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (nameParts != null) {
            if (nameParts.size() == 1) {
                dbName = nameParts.get(0);
                CatalogIf currentCatalog = ctx.getCurrentCatalog();
                ctlName = currentCatalog == null ? null : currentCatalog.getName();
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
            } else {
                throw new AnalysisException(
                        "Only support SHOW LANCE INDEX JOBS FROM [catalog.]database, but get: " + nameParts);
            }
        }
        analyzeWhereClause();
    }

    private void analyzeWhereClause() throws AnalysisException {
        if (whereClause == null) {
            return;
        }
        List<Expression> children = new ArrayList<>();
        splitCompoundPredicate(whereClause, children);
        Set<String> names = new HashSet<>();
        for (Expression child : children) {
            analyzeSubPredicate(child);
            String name = ((UnboundSlot) child.child(0)).getName().toLowerCase(Locale.ROOT);
            if (!names.add(name)) {
                throw new AnalysisException("column names on both sides of operator AND should be different");
            }
        }
    }

    private void splitCompoundPredicate(Expression expr, List<Expression> children) throws AnalysisException {
        if (expr instanceof CompoundPredicate) {
            if (!(expr instanceof And)) {
                throw new AnalysisException("Only allow compound predicate with operator AND");
            }
            splitCompoundPredicate(expr.child(0), children);
            splitCompoundPredicate(expr.child(1), children);
        } else {
            children.add(expr);
        }
    }

    private void analyzeSubPredicate(Expression expr) throws AnalysisException {
        if (!(expr instanceof EqualTo)
                || !(expr.child(0) instanceof UnboundSlot)
                || !(expr.child(1) instanceof StringLiteral)) {
            throw new AnalysisException(WHERE_HINT);
        }
        String key = ((UnboundSlot) expr.child(0)).getName();
        String value = ((StringLiteral) expr.child(1)).getStringValue();
        if (key.equalsIgnoreCase(KEY_TABLE_NAME)) {
            tableNameValue = value;
        } else if (key.equalsIgnoreCase(KEY_STATE)) {
            stateValue = value.toUpperCase(Locale.ROOT);
            try {
                LanceIndexJobMutationState.valueOf(stateValue);
            } catch (IllegalArgumentException e) {
                throw new AnalysisException("Unknown Lance index job state: " + value + "; " + WHERE_HINT);
            }
        } else {
            throw new AnalysisException(WHERE_HINT);
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        List<List<String>> rows = new ArrayList<>();
        for (LanceIndexJob job : Env.getCurrentEnv().getLanceIndexJobManager().getAllJobsSnapshot()) {
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog =
                    Env.getCurrentEnv().getCatalogMgr().getCatalog(job.getCatalogId());
            if (!matchesFilters(catalog, job)) {
                continue;
            }
            if (!isAuthorized(ctx, catalog, job)) {
                continue;
            }
            rows.add(renderRow(job, catalog));
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    private boolean matchesFilters(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog, LanceIndexJob job) {
        if (ctlName != null && (catalog == null || !ctlName.equals(catalog.getName()))) {
            return false;
        }
        if (dbName != null && !dbName.equals(job.getDbName())) {
            return false;
        }
        if (tableNameValue != null && !tableNameValue.equals(job.getTableName())) {
            return false;
        }
        return stateValue == null
                || job.getMutationState() != null && stateValue.equals(job.getMutationState().name());
    }

    /**
     * Orphan and half-orphan rows (catalog gone, or persisted db/table no longer resolvable)
     * are visible to global ADMIN only; every other row needs table-level SHOW on the
     * persisted target. The caller omits the row when this returns false, so non-ADMIN users
     * see no trace of orphaned jobs, not even a count.
     */
    static boolean isAuthorized(ConnectContext ctx, CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog,
            LanceIndexJob job) {
        if (!targetResolves(catalog, job)) {
            return Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN);
        }
        return Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, catalog.getName(),
                job.getDbName(), job.getTableName(), PrivPredicate.SHOW);
    }

    static boolean targetResolves(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog, LanceIndexJob job) {
        if (catalog == null) {
            return false;
        }
        DatabaseIf<? extends TableIf> db = catalog.getDbNullable(job.getDbName());
        return db != null && db.getTableNullable(job.getTableName()) != null;
    }

    private static List<String> renderRow(LanceIndexJob job,
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
        List<String> row = new ArrayList<>(TITLE_NAMES.size());
        row.add(String.valueOf(job.getJobId()));
        // The catalog name is not persisted on the job record; for orphan rows (catalog gone)
        // there is nothing safe to render, so the column stays empty rather than fabricating
        // a placeholder from the internal catalog id.
        row.add(catalog == null ? "" : catalog.getName());
        row.add(job.getDbName());
        row.add(job.getTableName());
        row.add(job.getDisplayIndexName());
        row.add(job.getMutationType() == null ? "" : job.getMutationType().name());
        row.add(job.getMutationState() == null ? "" : job.getMutationState().name());
        row.add(job.getRefreshState() == null ? "" : job.getRefreshState().name());
        row.add(job.holdsPossibleLiveSlot() ? "YES" : "NO");
        row.add(TimeUtils.longToTimeString(job.getCreateTimeMs()));
        row.add(TimeUtils.longToTimeString(job.getUpdateTimeMs()));
        // job.getResult() is always null until a worker reports (no worker exists yet).
        row.add(job.getResult() == null || job.getResult().getSanitizedMessage() == null
                ? "" : job.getResult().getSanitizedMessage());
        row.addAll(renderForceAudit(job));
        return row;
    }

    static List<String> renderForceAudit(LanceIndexJob job) {
        List<String> forceColumns = new ArrayList<>(3);
        if (!job.isForceReleased()) {
            forceColumns.add("");
            forceColumns.add("");
            forceColumns.add("");
            return forceColumns;
        }
        forceColumns.add("YES");
        forceColumns.add(job.getForceActor() == null ? "" : job.getForceActor());
        forceColumns.add(job.getForceTimeMs() == null ? "" : TimeUtils.longToTimeString(job.getForceTimeMs()));
        return forceColumns;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowLanceIndexJobsCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
