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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager;
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.PinnedHboStatistics;
import org.apache.doris.nereids.stats.HboStructFreshness;
import org.apache.doris.nereids.stats.SimpleStructInfo;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatistics;
import org.apache.doris.statistics.hbo.RecentRunsPlanStatisticsEntry;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Manual HBO statistics inspection:
 * <pre>
 *   HBO SHOW STATISTICS [LIKE '&lt;pattern&gt;']              -- pinned + learned, simplified struct info
 *   HBO SHOW STATISTICS FULL [LIKE '&lt;pattern&gt;']         -- canonical struct info
 *   HBO SHOW PINNED STATISTICS [FULL] [LIKE '&lt;pattern&gt;']   -- injected entries only
 *   HBO SHOW LEARNED STATISTICS [FULL] [LIKE '&lt;pattern&gt;']  -- learned entries only
 * </pre>
 * {@code FULL} decides both which struct info is printed and which one {@code LIKE} matches (the
 * canonical form, i.e. the fingerprint input, or its simplified rendering). The simplified form is
 * a pure function of the canonical form (see {@link SimpleStructInfo}), so it is never persisted and
 * can never be stale.
 *
 * <p>Pinned entries come from the in-memory (authoritative) pinned cache of this FE; learned entries
 * come from the recent-runs cache keyed by hbo fingerprint. The learned row count is the output
 * row count of the latest recorded run of that fingerprint (see the Detail column for the number of
 * recorded runs), so it is a summary, not a single current value.
 */
public class HboShowStatisticsCommand extends ShowCommand {
    private static final String SCOPE_PINNED = "pinned";
    private static final String SCOPE_LEARNED = "learned";

    private final String scope;
    private final boolean fullStructInfo;
    private final String likePattern;
    private final ShowResultSetMetaData metaData;

    /**
     * HboShowStatisticsCommand
     * @param scope PINNED / LEARNED, null or empty means both
     * @param fullStructInfo print (and match against) the canonical struct info instead of the
     *                       simplified one
     * @param likePattern optional SQL LIKE pattern applied to the printed struct info column
     */
    public HboShowStatisticsCommand(String scope, boolean fullStructInfo, String likePattern) {
        super(PlanType.HBO_SHOW_STATISTICS_COMMAND);
        this.scope = scope == null || scope.isEmpty() ? null : scope.toLowerCase(Locale.ROOT);
        this.fullStructInfo = fullStructInfo;
        this.likePattern = likePattern == null || likePattern.isEmpty() ? null : likePattern;
        this.metaData = ShowResultSetMetaData.builder()
                .addColumn(new Column("Kind", ScalarType.createVarchar(16)))
                .addColumn(new Column("Fingerprint", ScalarType.createVarchar(64)))
                .addColumn(new Column("LiteralMode", ScalarType.createVarchar(16)))
                .addColumn(new Column("Type", ScalarType.createVarchar(16)))
                .addColumn(new Column("Value", ScalarType.createVarchar(32)))
                .addColumn(new Column(fullStructInfo ? "StructInfo" : "SimpleStruct",
                        ScalarType.createVarchar(65533)))
                .addColumn(new Column("Baseline", ScalarType.createVarchar(512)))
                .addColumn(new Column("State", ScalarType.createVarchar(16)))
                .addColumn(new Column("Detail", ScalarType.createVarchar(512)))
                .build();
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied: HBO statistics management requires ADMIN privilege");
        }
        if (scope != null && !SCOPE_PINNED.equals(scope) && !SCOPE_LEARNED.equals(scope)) {
            throw new AnalysisException("invalid hbo show scope, expect PINNED or LEARNED: " + scope);
        }
        Pattern pattern = compileLikePattern(likePattern);
        HboPlanStatisticsManager hboManager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        List<List<String>> rows = new ArrayList<>();
        if (scope == null || SCOPE_PINNED.equals(scope)) {
            for (PinnedHboStatistics pinned : hboManager.getAllPinnedPlanStatistics().values()) {
                String structInfo = displayedStructInfo(pinned.getStructCanonical());
                if (!matches(pattern, pinned.getStructCanonical(), structInfo)) {
                    continue;
                }
                // an expansion entry is keyed by join conditions, so it is not tied to any data
                // state and can not go stale
                HboStructFreshness freshness = pinned.isExpansion()
                        ? null : HboStructFreshness.of(pinned.getStructCanonical());
                List<String> row = new ArrayList<>();
                row.add(SCOPE_PINNED);
                row.add(pinned.getFingerprint());
                row.add(pinned.getLiteralMode().name().toLowerCase(Locale.ROOT));
                row.add(pinned.getType().name().toLowerCase(Locale.ROOT));
                // a JOIN_EXPANSION entry carries a fan-out factor instead of a row count; the x
                // suffix keeps the two readable in one column
                row.add(pinned.isExpansion()
                        ? trimDouble(pinned.getExpansion()) + "x" : String.valueOf(pinned.getRows()));
                row.add(structInfo);
                row.add(freshness == null ? "-" : freshness.getRecorded());
                row.add(freshness == null ? "-" : freshness.getState());
                row.add(pinnedDetail(pinned.getCreateTime(),
                        freshness == null ? null : freshness.getLiveDetail(), pinned.getLastUse()));
                rows.add(row);
            }
        }
        if (scope == null || SCOPE_LEARNED.equals(scope)) {
            for (Map.Entry<String, RecentRunsPlanStatistics> entry : hboManager.getHboPlanStatisticsProvider()
                    .getAllHboPlanStats().entrySet()) {
                Optional<String> canonical = hboManager.getLearnedStructCanonical(entry.getKey());
                // a learned key is generated internally, so it only has a struct info when it was
                // injected together with a struct literal
                String structInfo = canonical.map(this::displayedStructInfo).orElse("");
                if (!matches(pattern, canonical.orElse(null), structInfo)) {
                    continue;
                }
                List<RecentRunsPlanStatisticsEntry> recentRuns = entry.getValue().getRecentRunsStatistics();
                String rowsText = recentRuns.isEmpty() ? ""
                        : String.valueOf(recentRuns.get(recentRuns.size() - 1).getPlanStatistics().getOutputRows());
                List<String> row = new ArrayList<>();
                row.add(SCOPE_LEARNED);
                row.add(entry.getKey());
                // learned keys are constant agnostic for join / aggregation and the scan token for
                // scans, so no user facing granularity can be reported
                row.add("-");
                row.add("-");
                row.add(rowsText);
                row.add(structInfo.isEmpty() ? "-" : structInfo);
                HboStructFreshness freshness = canonical.map(HboStructFreshness::of).orElse(null);
                row.add(freshness == null ? "-" : freshness.getRecorded());
                row.add(freshness == null ? HboStructFreshness.STATE_UNKNOWN : freshness.getState());
                row.add(freshness == null || freshness.getLive().isEmpty()
                        ? "runs=" + recentRuns.size()
                        : "runs=" + recentRuns.size() + " now=" + freshness.getLiveDetail());
                rows.add(row);
            }
        }
        rows.sort(Comparator.comparing((List<String> row) -> row.get(0))
                .thenComparing(row -> row.get(1)));
        return new ShowResultSet(metaData, rows);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    /**
     * The Detail column: when the entry was created, the data state it is compared with now, and
     * when a query of this FE last applied it (with the state it was applied in). The {@code last}
     * part is the only one which comes from the read side: it describes what the entry was actually
     * applied against, while {@code now} is read from the catalog here (see
     * {@link HboStructFreshness} for why the two can differ).
     */
    private static String pinnedDetail(long createTime, String liveDetail, String lastUse) {
        StringBuilder detail = new StringBuilder("created=").append(
                TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(createTime), ZoneId.systemDefault())));
        if (liveDetail != null && !liveDetail.isEmpty() && !"-".equals(liveDetail)) {
            detail.append(",now=").append(liveDetail);
        }
        if (lastUse != null && !lastUse.isEmpty()) {
            detail.append(",last=").append(lastUse);
        }
        return detail.toString();
    }

    private String displayedStructInfo(String canonicalStructInfo) {
        return fullStructInfo ? canonicalStructInfo : SimpleStructInfo.render(canonicalStructInfo);
    }

    /**
     * {@code LIKE} matches the printed struct info column. An entry without a struct info (a learned
     * entry injected by fingerprint only) cannot match a struct info pattern.
     */
    private boolean matches(Pattern pattern, String canonicalStructInfo, String displayedStructInfo) {
        if (pattern == null) {
            return true;
        }
        return canonicalStructInfo != null && !displayedStructInfo.isEmpty()
                && pattern.matcher(displayedStructInfo).matches();
    }

    /** Render a fan-out factor without a trailing {@code .0}. */
    private static String trimDouble(double value) {
        return value == Math.floor(value) && !Double.isInfinite(value)
                ? String.valueOf((long) value) : String.valueOf(value);
    }

    /**
     * Translate a SQL LIKE pattern into an equivalent regex ('%' matches any sequence, '_' matches
     * a single character, a backslash escapes the next character, which is needed because table and
     * column names in a struct info contain underscores), anchored by the caller via matches().
     */
    private static Pattern compileLikePattern(String like) {
        if (like == null) {
            return null;
        }
        StringBuilder regex = new StringBuilder();
        boolean escaped = false;
        for (char c : like.toCharArray()) {
            if (escaped) {
                regex.append(Pattern.quote(String.valueOf(c)));
                escaped = false;
            } else if (c == '\\') {
                escaped = true;
            } else if (c == '%') {
                regex.append(".*");
            } else if (c == '_') {
                regex.append('.');
            } else {
                regex.append(Pattern.quote(String.valueOf(c)));
            }
        }
        if (escaped) {
            regex.append(Pattern.quote("\\"));
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }
}
