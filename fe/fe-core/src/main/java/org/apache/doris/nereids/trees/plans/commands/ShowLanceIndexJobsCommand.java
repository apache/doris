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
import org.apache.doris.datasource.lance.LanceExternalCatalog;
import org.apache.doris.datasource.lance.job.LanceIndexJob;
import org.apache.doris.datasource.lance.job.LanceIndexJobMutationState;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * SHOW LANCE INDEX JOBS [FROM [catalog.]db] [WHERE TableName = "tbl" [AND State = "PENDING"]].
 *
 * <p>Lists the durable Lance index job records held by the master. Rows whose persisted
 * target no longer resolves (the catalog is gone, the catalog is there but the db or
 * table no longer resolves, or the resolution itself fails because the provider is
 * unreachable), or whose persisted dataset locator no longer matches the catalog's
 * current one, are visible to global ADMIN only; every other row requires table-level
 * SHOW on the persisted (catalog, db, table). Rows that fail the check are omitted
 * entirely, so non-ADMIN users see no orphan trace, not even a count. The job locator,
 * provider, normalized names, propertiesJson and schema contract are never shown.
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
                normalizeDbName(currentCatalog);
            } else if (nameParts.size() == 2) {
                ctlName = nameParts.get(0);
                dbName = nameParts.get(1);
                normalizeDbName(Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName));
            } else {
                throw new AnalysisException(
                        "Only support SHOW LANCE INDEX JOBS FROM [catalog.]database, but get: " + nameParts);
            }
        }
        analyzeWhereClause();
    }

    /**
     * Resolves the FROM database name through the target catalog's own naming rules
     * before it is compared against the persisted job records: with
     * lower_case_database_names = 1/2, {@code FROM DB1} must still match jobs stored
     * under the resolved full name {@code db1}. A name the catalog cannot resolve (or a
     * resolution that fails outright) keeps the raw requested string, so the filter just
     * stays an exact comparison; the resolution itself must never fail the statement.
     */
    private void normalizeDbName(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog) {
        if (dbName == null || catalog == null) {
            return;
        }
        try {
            DatabaseIf<? extends TableIf> db = catalog.getDbNullable(dbName);
            if (db == null) {
                return;
            }
            String fullName = db.getFullName();
            if (fullName != null && !fullName.isEmpty()) {
                dbName = fullName;
            }
        } catch (Exception e) {
            // Keep the raw requested name; the filter remains an exact comparison.
        }
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
                || !(expr.child(1) instanceof StringLikeLiteral)) {
            throw new AnalysisException(WHERE_HINT);
        }
        String key = ((UnboundSlot) expr.child(0)).getName();
        String value = ((StringLikeLiteral) expr.child(1)).getStringValue();
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
        // One locator resolution per unique (catalogId, db, table) for the whole listing:
        // each resolution is a provider round trip, and failed (null) resolutions are
        // cached too so a broken target is probed once, not once per sibling job.
        Map<String, String> locatorCache = new HashMap<>();
        for (LanceIndexJob job : Env.getCurrentEnv().getLanceIndexJobManager().getAllJobsSnapshot()) {
            CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog =
                    Env.getCurrentEnv().getCatalogMgr().getCatalog(job.getCatalogId());
            if (!matchesFilters(catalog, job)) {
                continue;
            }
            if (!isAuthorized(ctx, catalog, job, locatorCache)) {
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
     * Orphan and half-orphan rows (catalog gone, persisted db/table no longer resolvable,
     * or a resolution that fails outright because the provider is unreachable) are
     * visible to global ADMIN only. A Lance catalog target is additionally revalidated
     * against the catalog's current durable dataset locator: after a job reaches a
     * terminal state and releases its guard, a legitimate ALTER can repoint the same
     * db.table names at a different dataset, and SHOW on the new target must not
     * disclose the old job. Every other row needs table-level SHOW on the persisted
     * target. The caller omits the row when this returns false, so non-ADMIN users see
     * no trace of orphaned jobs, not even a count.
     */
    static boolean isAuthorized(ConnectContext ctx, CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog,
            LanceIndexJob job) {
        return isAuthorized(ctx, catalog, job, null);
    }

    /**
     * Same authorization as {@link #isAuthorized(ConnectContext, CatalogIf, LanceIndexJob)},
     * with an optional (catalogId, db, table)-keyed locator cache so a listing resolves
     * each unique target's current locator once per run instead of once per job row.
     */
    static boolean isAuthorized(ConnectContext ctx, CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog,
            LanceIndexJob job, Map<String, String> locatorCache) {
        if (!targetResolves(catalog, job) || !locatorMatches(catalog, job, locatorCache)) {
            return Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx, PrivPredicate.ADMIN);
        }
        return Env.getCurrentEnv().getAccessManager().checkTblPriv(ctx, catalog.getName(),
                job.getDbName(), job.getTableName(), PrivPredicate.SHOW);
    }

    static boolean targetResolves(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog, LanceIndexJob job) {
        if (catalog == null) {
            return false;
        }
        try {
            DatabaseIf<? extends TableIf> db = catalog.getDbNullable(job.getDbName());
            return db != null && db.getTableNullable(job.getTableName()) != null;
        } catch (Exception e) {
            // External metadata resolution can fail outright (credentials expired,
            // provider down). That must not leak the provider error or abort the listing:
            // a failed resolution is handled exactly like an unresolvable (orphan) target.
            return false;
        }
    }

    /**
     * Whether the catalog's current durable dataset locator still points at the dataset
     * this job was admitted against. Only Lance catalogs carry a durable locator; for
     * any other catalog instance (theoretically unreachable, since jobs are only
     * admitted for Lance catalogs) the name resolution above is the whole rule. A
     * locator that cannot be resolved right now counts as a mismatch, so authorization
     * fails closed to the orphan rule instead of granting SHOW through stale names.
     */
    private static boolean locatorMatches(CatalogIf<? extends DatabaseIf<? extends TableIf>> catalog,
            LanceIndexJob job, Map<String, String> locatorCache) {
        if (!(catalog instanceof LanceExternalCatalog)) {
            return true;
        }
        String key = job.getCatalogId() + ":" + job.getDbName() + ":" + job.getTableName();
        String currentLocator;
        if (locatorCache != null && locatorCache.containsKey(key)) {
            currentLocator = locatorCache.get(key);
        } else {
            currentLocator = ((LanceExternalCatalog) catalog).resolveCurrentIndexJobLocator(
                    job.getDbName(), job.getTableName());
            if (locatorCache != null) {
                locatorCache.put(key, currentLocator);
            }
        }
        return currentLocator != null && currentLocator.equals(job.getNormalizedLocator());
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
