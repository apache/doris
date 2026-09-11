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
import org.apache.doris.nereids.stats.HboPlanStatisticsManager.PinnedJoinExpansion;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

/**
 * {@code HBO SHOW EXPANSION STATISTICS [LIKE '<pattern>']}: list the join expansion entries
 * injected by {@code HBO SET EXPANSION}.
 */
public class HboShowExpansionCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("CondFingerprint", ScalarType.createVarchar(64)))
                    .addColumn(new Column("Conditions", ScalarType.createVarchar(65533)))
                    .addColumn(new Column("Expansion", ScalarType.createVarchar(32)))
                    .addColumn(new Column("Detail", ScalarType.createVarchar(64)))
                    .build();

    private final String likePattern;

    public HboShowExpansionCommand(String likePattern) {
        super(PlanType.HBO_SHOW_EXPANSION_COMMAND);
        this.likePattern = likePattern == null || likePattern.isEmpty() ? null : likePattern;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            throw new AnalysisException("Access denied: HBO statistics management requires ADMIN privilege");
        }
        Pattern pattern = compileLikePattern(likePattern);
        HboPlanStatisticsManager hboManager = Env.getCurrentEnv().getHboPlanStatisticsManager();
        List<List<String>> rows = new ArrayList<>();
        for (PinnedJoinExpansion entry : hboManager.getAllPinnedJoinExpansion().values()) {
            if (pattern != null && !pattern.matcher(entry.getCondFingerprint()).matches()) {
                continue;
            }
            List<String> row = new ArrayList<>();
            row.add(entry.getCondFingerprint());
            row.add(entry.getCondCanonical());
            row.add(trimDouble(entry.getExpansion()) + "x");
            row.add(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.ofInstant(
                    Instant.ofEpochMilli(entry.getCreateTime()), ZoneId.systemDefault())));
            rows.add(row);
        }
        rows.sort(Comparator.comparing((List<String> row) -> row.get(0)));
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

    private static String trimDouble(double value) {
        if (value == Math.floor(value) && !Double.isInfinite(value)) {
            return String.valueOf((long) value);
        }
        return String.valueOf(value);
    }

    /** Translate a SQL LIKE pattern into an equivalent regex ('%' any sequence, '_' one char). */
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
