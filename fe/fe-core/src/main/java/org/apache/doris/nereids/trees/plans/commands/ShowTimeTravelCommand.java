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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.service.FrontendOptions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * SHOW TIME TRAVEL ON tbl [BETWEEN 'T1' AND 'T2']
 *
 * Returns distinct commit snapshots visible within the retention window,
 * plus header lines showing the queryable time range.
 */
public class ShowTimeTravelCommand extends ShowCommand {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final List<String> tableName;   // [db, table] or [table]
    private final String startTime;         // nullable — BETWEEN lower bound
    private final String endTime;           // nullable — BETWEEN upper bound

    public ShowTimeTravelCommand(List<String> tableName, String startTime, String endTime) {
        super(PlanType.SHOW_TIME_TRAVEL_COMMAND);
        this.tableName = tableName;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Config.isCloudMode()) {
            throw new AnalysisException(
                    "SHOW TIME TRAVEL is only supported in cloud/decoupled mode.");
        }

        // Resolve table
        String dbName = tableName.size() >= 2 ? tableName.get(tableName.size() - 2)
                : ctx.getDatabase();
        String tblName = tableName.get(tableName.size() - 1);
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        OlapTable table = (OlapTable) db.getTableOrAnalysisException(tblName);

        if (!table.isEnableTimeTravel()) {
            throw new AnalysisException(
                    "Time travel is not enabled on table '" + tblName + "'. "
                    + "Enable it with PROPERTIES (\"enable_time_travel\" = \"true\").");
        }

        // Build RPC request
        Cloud.ShowTimeTravelRequest.Builder req = Cloud.ShowTimeTravelRequest.newBuilder()
                .setCloudUniqueId(Config.cloud_unique_id)
                .setRequestIp(FrontendOptions.getLocalHostAddressCached())
                .setTableId(table.getId())
                .setLimit(20)
                .setRetentionDays(table.getTimeTravelRetentionDays());

        table.getPartitionIds().forEach(req::addPartitionIds);

        ZoneId tz = ZoneId.systemDefault();
        if (startTime != null) {
            req.setStartMs(parseTs(startTime, tz));
        }
        if (endTime != null) {
            req.setEndMs(parseTs(endTime, tz));
        }

        Cloud.ShowTimeTravelResponse resp =
                MetaServiceProxy.getInstance().showTimeTravel(req.build());
        if (resp.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
            throw new DdlException("SHOW TIME TRAVEL failed: " + resp.getStatus().getMsg());
        }

        List<List<String>> rows = new ArrayList<>();

        // MS returns limit+1 snapshots when more exist (has_more signal).
        // If we got more than the limit we requested (20), there are more to show.
        int requestedLimit = 20;
        boolean hasMore = resp.getSnapshotsCount() > requestedLimit;
        int shown = hasMore ? requestedLimit : resp.getSnapshotsCount();
        String retentionStr = table.getTimeTravelRetentionDays() + " days";

        // Header row: table info + snapshot count
        String countStr = hasMore ? shown + "+ snapshots" : shown + " snapshot" + (shown == 1 ? "" : "s");
        rows.add(List.of("Table:  " + dbName + "." + tblName
                + "  |  Retention: " + retentionStr + "  |  " + countStr, "", ""));

        // Queryable range row
        String earliest = resp.hasEarliestMs() ? formatTs(resp.getEarliestMs(), tz) : "unknown";
        rows.add(List.of("Queryable: " + earliest + "  →  now", "", ""));

        // BETWEEN window row (only when user specified a range)
        if (startTime != null || endTime != null) {
            String winStart = startTime != null ? startTime : earliest;
            String winEnd = endTime != null ? endTime : formatTs(
                    java.time.Instant.now().toEpochMilli(), tz);
            rows.add(List.of("Window: " + winStart + "  →  " + winEnd, "", ""));
        }

        // Separator
        rows.add(List.of("", "", ""));

        // Snapshot rows — newest first, capped at requestedLimit
        int emitted = 0;
        for (Cloud.TtSnapshotInfo snap : resp.getSnapshotsList()) {
            if (emitted >= requestedLimit) {
                break;
            }
            long commitMs = snap.getCommitTimeMs();
            long queryMs = (commitMs / 1000L + 1L) * 1000L;
            rows.add(List.of(
                    formatTs(commitMs, tz),
                    formatTs(queryMs, tz),
                    String.valueOf(snap.getVersion())));
            emitted++;
        }

        // Separator
        rows.add(List.of("", "", ""));

        // Footer
        if (!hasMore) {
            rows.add(List.of("All " + shown + " snapshot" + (shown == 1 ? "" : "s") + " shown."
                    + "  Use query_at directly in FOR SYSTEM_TIME AS OF.", "", ""));
        } else {
            long oldestShownMs = resp.hasFirstDataMs() ? resp.getFirstDataMs() : -1;
            rows.add(List.of("Showing " + shown + " most recent snapshots."
                    + "  To see older snapshots:", "", ""));
            if (oldestShownMs > 0) {
                long nextEndMs = oldestShownMs - 1000L;
                rows.add(List.of("  SHOW TIME TRAVEL ON " + dbName + "." + tblName, "", ""));
                rows.add(List.of("    BETWEEN '" + earliest + "'", "", ""));
                rows.add(List.of("    AND '" + formatTs(nextEndMs, tz) + "';", "", ""));
            }
        }

        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        builder.addColumn(new Column("snapshot_time", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("query_at", ScalarType.createVarchar(32)));
        builder.addColumn(new Column("version", ScalarType.createVarchar(25)));
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTimeTravelCommand(this, context);
    }

    private static long parseTs(String ts, ZoneId tz) throws AnalysisException {
        try {
            return LocalDateTime.parse(ts.trim(), FMT)
                    .atZone(tz).toInstant().toEpochMilli();
        } catch (Exception e) {
            throw new AnalysisException(
                    "Invalid timestamp '" + ts + "'. Expected: yyyy-MM-dd HH:mm:ss");
        }
    }

    private static String formatTs(long epochMs, ZoneId tz) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMs), tz).format(FMT);
    }
}
