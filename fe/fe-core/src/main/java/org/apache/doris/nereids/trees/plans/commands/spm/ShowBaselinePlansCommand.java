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

package org.apache.doris.nereids.trees.plans.commands.spm;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.parser.Origin;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ShowCommand;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * SHOW BASELINE PLANS command (design doc 6.8 / 6.9 / 6.17).
 *
 * Syntax:
 *
 *   SHOW BASELINE PLANS [LIKE 'pattern' | WHERE expression]
 *
 * Execution logic: merge the SESSION-scope baselines of the current connection
 * (SessionBaselineStore) with the GLOBAL ones from
 * BaselineManager.getInstance().getAllBaselines(), order by id and render them as a
 * ShowResultSet. The first 12 columns mirror the spm_baselines internal table (design doc
 * 6.14.1; `query_id` is the audit_log correlation id of the statement that produced the
 * baseline); the trailing `scope` column is synthesized (GLOBAL / SESSION) because the
 * scope is implied by the owning store / the id range (BaselineScope.ofId) and is not
 * persisted in the table.
 *
 * Phase 1 filtering: a non-null pattern (from LIKE or a simplified
 * `WHERE source = 'x'` / `WHERE status = 'x'` clause) is matched in Java against the
 * source / status / scope columns (exact) and against bindSql / planSql (substring).
 */
public class ShowBaselinePlansCommand extends ShowCommand {

    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** SHOW result set columns: the first 12 columns are one-to-one with the
     * spm_baselines internal table (see InternalSchema.SPM_BASELINES_SCHEMA); the
     * trailing `scope` column is synthesized (GLOBAL / SESSION, also derivable from the
     * id range - BaselineScope.ofId). */
    private static final ShowResultSetMetaData META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("id", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("bind_sql", ScalarType.createType(PrimitiveType.STRING)))
            .addColumn(new Column("bind_sql_digest", ScalarType.createType(PrimitiveType.STRING)))
            .addColumn(new Column("bind_sql_hash", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("plan_sql", ScalarType.createType(PrimitiveType.STRING)))
            .addColumn(new Column("query_id", ScalarType.createVarchar(64)))
            .addColumn(new Column("cost", ScalarType.createType(PrimitiveType.DOUBLE)))
            .addColumn(new Column("query_time_ms", ScalarType.createType(PrimitiveType.BIGINT)))
            .addColumn(new Column("source", ScalarType.createVarchar(16)))
            .addColumn(new Column("status", ScalarType.createVarchar(16)))
            .addColumn(new Column("create_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("update_time", ScalarType.createType(PrimitiveType.DATETIME)))
            .addColumn(new Column("scope", ScalarType.createVarchar(16)))
            .build();

    /** Optional LIKE / WHERE filter value; null shows everything. */
    private final String pattern;

    /** Exact-match filter column (id / bind_sql_digest / bind_sql / plan_sql /
     * source / status / scope) extracted from a WHERE col = val clause; null when
     * the filter is only a LIKE / substring pattern. */
    private final String filterColumn;

    /** Exact-match filter value for filterColumn; null when unused. */
    private final String filterValue;

    public ShowBaselinePlansCommand(String pattern) {
        this(pattern, null, null);
    }

    public ShowBaselinePlansCommand(String pattern, String filterColumn, String filterValue) {
        super(PlanType.SHOW_BASELINE_PLANS_COMMAND);
        this.pattern = pattern;
        this.filterColumn = filterColumn;
        this.filterValue = filterValue;
    }

    public String getPattern() {
        return pattern;
    }

    /** Builds the LIKE matcher of one operand; {@code null} means "LIKE omitted". */
    static java.util.regex.Pattern buildLikeMatcher(String pattern)
            throws org.apache.doris.common.AnalysisException {
        if (pattern == null) {
            return null;
        }
        // SHOW BASELINE PLANS searches STORED SQL TEXT. The shared MySQL-pattern helper
        // (PatternMatcher) rejects the literal characters every statement contains
        // ('*', '=', '('), so LIKE '%SELECT * FROM%' failed with an analysis error, and
        // its '%' compiles to a regex '.' that does not span the NEWLINES the stored SQL
        // is printed with. Escape every regex metacharacter and apply the MySQL
        // wildcards explicitly: '%' -> any characters (DOTALL), '_' -> one character,
        // '\' escapes the NEXT wildcard character ('\_' / '\%' are LITERALS, exactly
        // like the regular LIKE matcher), whole-value, case-insensitive.
        StringBuilder regex = new StringBuilder();
        for (int i = 0; i < pattern.length(); i++) {
            char c = pattern.charAt(i);
            if (c == '\\' && i + 1 < pattern.length()
                    && (pattern.charAt(i + 1) == '%' || pattern.charAt(i + 1) == '_')) {
                // the SQL literal parser preserves the backslash; consuming it together
                // with the wildcard keeps a search for '%my\_table%' matching the stored
                // my_table (the old loop emitted the backslash literally and turned the
                // '_' into a wildcard, so the row was missed while unrelated text with a
                // backslash could match)
                char escaped = pattern.charAt(++i);
                regex.append('\\').append(escaped);
            } else if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                if ("\\^$.|?*+()[]{}".indexOf(c) >= 0) {
                    regex.append('\\');
                }
                regex.append(c);
            }
        }
        return java.util.regex.Pattern.compile(regex.toString(),
                java.util.regex.Pattern.DOTALL | java.util.regex.Pattern.CASE_INSENSITIVE);
    }

    @Override
    public Optional<Origin> getOrigin() {
        return super.getOrigin();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // SPM management commands require ADMIN
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(
                ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // SESSION-scope baselines of this connection first, then the GLOBAL ones; ids are
        // self-describing by range (BaselineScope.ofId), so sorting by id keeps the
        // deterministic order and puts the SESSION rows (>= 2^62) after the GLOBAL ones
        List<BaselinePlan> all = Lists.newArrayList();
        all.addAll(ctx.getSessionBaselineStore().getAllBaselines());
        // GLOBAL rows must be authoritative: getAllBaselines() only STARTS the
        // asynchronous load and returns the current map, so right after startup / a
        // promotion (which clears the map) SHOW reported ZERO rows although durable
        // baselines existed, and a failed read never converged. Use the same confirmed
        // read (with a retryable error) the mutating DDL relies on.
        BaselineManager.getInstance().ensureLoadedConfirmed();
        all.addAll(BaselineManager.getInstance().getAllBaselines());
        all.sort(Comparator.comparingLong(BaselinePlan::getId));

        List<List<String>> rows = Lists.newArrayList();
        // LIKE operand: real MySQL wildcard semantics (% and _ are wildcards, the pattern
        // must match the WHOLE value), case-insensitive like the previous substring
        // behavior. The matcher escapes literal regex characters so a SQL-shaped operand
        // ('SELECT * FROM ...') is matchable (see buildLikeMatcher).
        // Only an OMITTED LIKE is "no filter": an empty pattern operand (LIKE '') is a
        // real pattern that matches only empty values - treating it as absent admitted
        // every baseline although none of the searched SQL / status / source fields is
        // empty.
        java.util.regex.Pattern matcher = buildLikeMatcher(pattern);
        for (BaselinePlan baseline : all) {
            if (!matches(baseline, matcher)) {
                continue;
            }
            rows.add(toRow(baseline));
        }
        return new ShowResultSet(META_DATA, rows);
    }

    /**
     * Java-side filter: an exact match on the WHERE column (id / bind_sql_digest /
     * bind_sql / plan_sql / source / status / scope), or a MySQL LIKE match on
     * source / status / bindSql / planSql when the pattern operand was given.
     *
     * @param baseline the baseline to test
     * @param matcher  the LIKE pattern matcher (null when no LIKE pattern was given)
     * @return whether the baseline passes the filter
     */
    private boolean matches(BaselinePlan baseline, java.util.regex.Pattern matcher) {
        if (filterColumn != null) {
            String value = filterValue == null ? "" : filterValue;
            switch (filterColumn.toLowerCase()) {
                case "id":
                    return Long.toString(baseline.getId()).equals(value);
                case "bind_sql_digest":
                    return value.equals(baseline.getBindSqlDigest());
                case "bind_sql_hash":
                    return Long.toString(baseline.getBindSqlHash()).equals(value);
                case "bind_sql":
                    return value.equals(baseline.getBindSql());
                case "plan_sql":
                    return value.equals(baseline.getPlanSql());
                case "source":
                    return baseline.getSource().toString().equalsIgnoreCase(value);
                case "status":
                    return baseline.getStatus().toString().equalsIgnoreCase(value);
                case "scope":
                    return baseline.getScope().toString().equalsIgnoreCase(value);
                default:
                    return true; // unknown column: show nothing matches it exactly
            }
        }
        if (matcher == null) {
            return true;
        }
        // MySQL LIKE semantics: % and _ are wildcards and the pattern must match the
        // WHOLE value; the matcher escapes literal regex characters (see buildLikeMatcher),
        // so a SQL-shaped operand like '%SELECT * FROM%' is matchable and % spans the
        // newlines the stored SQL text is printed with.
        return matcher.matcher(baseline.getBindSql() == null ? "" : baseline.getBindSql())
                .matches()
                || matcher.matcher(baseline.getPlanSql() == null ? "" : baseline.getPlanSql())
                        .matches()
                || matcher.matcher(baseline.getSource().toString()).matches()
                || matcher.matcher(baseline.getStatus().toString()).matches();
    }

    /**
     * Renders a baseline as one result row (column order mirrors META_DATA).
     */
    private List<String> toRow(BaselinePlan baseline) {
        return Lists.newArrayList(
                String.valueOf(baseline.getId()),
                baseline.getBindSql() == null ? "" : baseline.getBindSql(),
                baseline.getBindSqlDigest() == null ? "" : baseline.getBindSqlDigest(),
                String.valueOf(baseline.getBindSqlHash()),
                baseline.getPlanSql() == null ? "" : baseline.getPlanSql(),
                baseline.getQueryId() == null ? "" : baseline.getQueryId(),
                String.valueOf(baseline.getCost()),
                String.valueOf(baseline.getQueryTimeMs()),
                baseline.getSource().toString(),
                baseline.getStatus().toString(),
                formatTime(baseline.getCreateTime()),
                formatTime(baseline.getUpdateTime()),
                baseline.getScope().toString()
        );
    }

    private static String formatTime(long epochMillis) {
        if (epochMillis <= 0) {
            return "";
        }
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        return time.format(DATETIME_FORMAT);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowBaselinePlansCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SHOW;
    }

    @Override
    public void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        throw new DdlException("SHOW BASELINE PLANS is not supported in cloud mode");
    }
}
