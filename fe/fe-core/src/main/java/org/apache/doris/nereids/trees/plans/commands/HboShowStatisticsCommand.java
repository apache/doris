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
import java.util.regex.Pattern;

/**
 * Manual HBO statistics inspection:
 * <pre>
 *   HBO SHOW STATISTICS [LIKE '&lt;pattern&gt;']              -- pinned + learned
 *   HBO SHOW PINNED STATISTICS [LIKE '&lt;pattern&gt;']       -- injected entries only
 *   HBO SHOW LEARNED STATISTICS [LIKE '&lt;pattern&gt;']      -- automatically collected entries only
 * </pre>
 * Pinned entries come from the in-memory (authoritative) pinned cache of this FE; learned entries
 * come from the recent-runs cache keyed by hbo fingerprint. The learned row count is the output
 * row count of the latest recorded run of that fingerprint (see the Detail column for the number
 * of recorded runs), so it is a summary, not a single current value.
 */
public class HboShowStatisticsCommand extends ShowCommand {
    private static final String SCOPE_PINNED = "pinned";
    private static final String SCOPE_LEARNED = "learned";

    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Kind", ScalarType.createVarchar(16)))
                    .addColumn(new Column("Fingerprint", ScalarType.createVarchar(64)))
                    .addColumn(new Column("NodeType", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("Rows", ScalarType.createVarchar(32)))
                    .addColumn(new Column("StructInfo", ScalarType.createVarchar(65533)))
                    .addColumn(new Column("Detail", ScalarType.createVarchar(64)))
                    .build();

    private final String scope;
    private final String likePattern;

    /**
     * HboShowStatisticsCommand
     * @param scope PINNED / LEARNED, null or empty means both
     * @param likePattern optional SQL LIKE pattern applied to the fingerprint
     */
    public HboShowStatisticsCommand(String scope, String likePattern) {
        super(PlanType.HBO_SHOW_STATISTICS_COMMAND);
        this.scope = scope == null || scope.isEmpty() ? null : scope.toLowerCase(Locale.ROOT);
        this.likePattern = likePattern == null || likePattern.isEmpty() ? null : likePattern;
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
                if (!matches(pattern, pinned.getFingerprint())) {
                    continue;
                }
                List<String> row = new ArrayList<>();
                row.add(SCOPE_PINNED);
                row.add(pinned.getFingerprint());
                row.add(pinned.getNodeType());
                row.add(String.valueOf(pinned.getRows()));
                row.add(pinned.getStructCanonical());
                row.add(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(pinned.getCreateTime()), ZoneId.systemDefault())));
                rows.add(row);
            }
        }
        if (scope == null || SCOPE_LEARNED.equals(scope)) {
            for (Map.Entry<String, RecentRunsPlanStatistics> entry : hboManager.getHboPlanStatisticsProvider()
                    .getAllHboPlanStats().entrySet()) {
                if (!matches(pattern, entry.getKey())) {
                    continue;
                }
                List<RecentRunsPlanStatisticsEntry> recentRuns = entry.getValue().getRecentRunsStatistics();
                String rowsText = recentRuns.isEmpty() ? ""
                        : String.valueOf(recentRuns.get(recentRuns.size() - 1).getPlanStatistics().getOutputRows());
                List<String> row = new ArrayList<>();
                row.add(SCOPE_LEARNED);
                row.add(entry.getKey());
                row.add("");
                row.add(rowsText);
                row.add("");
                row.add("runs=" + recentRuns.size());
                rows.add(row);
            }
        }
        rows.sort(Comparator.comparing((List<String> row) -> row.get(0))
                .thenComparing(row -> row.get(1)));
        return new ShowResultSet(META_DATA, rows);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCommand(this, context);
    }

    private static boolean matches(Pattern pattern, String fingerprint) {
        return pattern == null || pattern.matcher(fingerprint).matches();
    }

    /**
     * Translate a SQL LIKE pattern into an equivalent regex ('%' matches any sequence, '_' matches
     * a single character, everything else is literal), anchored by the caller via matches().
     */
    private static Pattern compileLikePattern(String like) {
        if (like == null) {
            return null;
        }
        StringBuilder regex = new StringBuilder();
        for (char c : like.toCharArray()) {
            switch (c) {
                case '%':
                    regex.append(".*");
                    break;
                case '_':
                    regex.append('.');
                    break;
                default:
                    regex.append(Pattern.quote(String.valueOf(c)));
                    break;
            }
        }
        return Pattern.compile(regex.toString(), Pattern.DOTALL);
    }
}
