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

package org.apache.doris.nereids.stats;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.rpc.RpcException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Whether a hbo entry may still be applied: the recorded data state of an entry (the baseline of
 * every scan of its struct info, see {@link HboScanDescriptor}) compared with the data state of the
 * query which wants to use it.
 *
 * <p>A hbo fingerprint does not contain any data state any more, so an entry keeps matching while a
 * table grows. The read side therefore has to decide whether the injected row count still describes
 * the data it was measured on:
 * <ul>
 *   <li>{@link #STATE_LIVE}: nothing changed (the recorded visible version is still the current
 *       one), the entry is applied;</li>
 *   <li>{@link #STATE_DRIFTED}: the data moved, but by at most
 *       {@code Config.hbo_row_count_change_ratio} of the recorded row count (10% by default): the
 *       entry is applied, which is the point of the tolerance - a table which merely keeps growing
 *       (e.g. a few new rows in a partition) does not invalidate a measured row count;</li>
 *   <li>{@link #STATE_STALE}: the data moved further than that (or the recorded state cannot be
 *       verified any more): the entry is <b>not</b> applied, and the query falls back to the
 *       optimizer estimation / the learned entries;</li>
 *   <li>{@link #STATE_UNKNOWN}: the entry records no data state at all (e.g. a hand written struct
 *       literal without a baseline), so there is nothing to judge: the entry is applied as it
 *       always was.</li>
 * </ul>
 * The recorded row count is never scaled: an entry either applies with the value the user measured
 * or does not apply at all ("soft strategy" - a user who needs the exact recorded state in any case
 * should not inject hbo statistics for that query).
 *
 * <p>A scan is judged by the rows of its <b>selected</b> partitions, so pruning decides which data
 * the entry depends on: an unrelated partition which grows does not invalidate a pruned entry.
 * {@code HBO SHOW STATISTICS} and {@code HBO DELETE STALE STATISTICS} cannot run the query of an
 * entry, so there the recorded state is compared with the table as a whole; an entry whose struct
 * prunes partitions can therefore only be judged by rows when it was recorded as a whole table scan
 * (its state is reported as unknown otherwise, and such an entry is kept unless the user asks for
 * {@code OLDER_THAN}).
 */
public class HboStructFreshness {
    /** The recorded data state is still the current one. */
    public static final String STATE_LIVE = "live";
    /** The data moved, but within the tolerance of {@code Config.hbo_row_count_change_ratio}. */
    public static final String STATE_DRIFTED = "drifted";
    /** The data moved further than the tolerance: the entry must not be applied. */
    public static final String STATE_STALE = "stale";
    /** The entry records no data state (or it cannot be read any more): nothing to judge. */
    public static final String STATE_UNKNOWN = "unknown";

    private static final Logger LOG = LogManager.getLogger(HboStructFreshness.class);

    private final String state;
    // the recorded baseline and the current state, both rendered for display
    private final String recorded;
    private final String live;
    // the row counts of the scan which decided the state, {@link HboScanDescriptor#UNKNOWN} when the
    // state was decided by something else (e.g. a version which is not readable)
    private final long recordedRows;
    private final long liveRows;

    private HboStructFreshness(String state, String recorded, String live, long recordedRows, long liveRows) {
        this.state = state;
        this.recorded = recorded;
        this.live = live;
        this.recordedRows = recordedRows;
        this.liveRows = liveRows;
    }

    public String getState() {
        return state;
    }

    public boolean isStale() {
        return STATE_STALE.equals(state);
    }

    public boolean isUnknown() {
        return STATE_UNKNOWN.equals(state);
    }

    public boolean isDrifted() {
        return STATE_DRIFTED.equals(state);
    }

    /** The data state the entry was recorded in, e.g. {@code hbo_test.t1:v3,r1000,p1/5}. */
    public String getRecorded() {
        return recorded;
    }

    /** The data state the entry is compared with (empty when it cannot be read). */
    public String getLive() {
        return live;
    }

    /**
     * The verdict as one token, printed after {@code used=} / {@code skipped=} in the explain
     * annotation, e.g. {@code drifted(rows=1030,rec=1000,+3.0%)}.
     */
    public String getSummary() {
        if (!STATE_DRIFTED.equals(state) && !STATE_STALE.equals(state)) {
            return state;
        }
        if (recordedRows == HboScanDescriptor.UNKNOWN || liveRows == HboScanDescriptor.UNKNOWN) {
            return state;
        }
        return state + "(rows=" + liveRows + ",rec=" + recordedRows + ","
                + changeText(recordedRows, liveRows) + ")";
    }

    /** The {@code now} part of {@code HBO SHOW STATISTICS}: the state read from the catalog. */
    public String getLiveDetail() {
        if (live.isEmpty()) {
            return "-";
        }
        if (recordedRows == HboScanDescriptor.UNKNOWN || liveRows == HboScanDescriptor.UNKNOWN) {
            return live;
        }
        return live + "," + changeText(recordedRows, liveRows);
    }

    /**
     * Judge an entry against the current query: the data state recorded in {@code recordedCanonical}
     * (the struct info of the entry) against the state of {@code liveCanonical} (the struct info the
     * read side just computed for the group). This is the decision of the read side.
     */
    public static HboStructFreshness between(String recordedCanonical, String liveCanonical) {
        List<HboScanDescriptor> recordedScans = HboScanDescriptor.parseAll(recordedCanonical);
        List<HboScanDescriptor> liveScans = HboScanDescriptor.parseAll(liveCanonical);
        if (recordedScans.isEmpty()) {
            // an entry without a baseline (a hand written struct literal): nothing to judge
            return new HboStructFreshness(STATE_UNKNOWN, "", renderScans(liveScans),
                    HboScanDescriptor.UNKNOWN, HboScanDescriptor.UNKNOWN);
        }
        if (recordedScans.size() != liveScans.size()) {
            // the entry describes a different number of scans than the group: cannot be compared
            return new HboStructFreshness(STATE_UNKNOWN, renderScans(recordedScans), renderScans(liveScans),
                    HboScanDescriptor.UNKNOWN, HboScanDescriptor.UNKNOWN);
        }
        String state = STATE_LIVE;
        String recordedText = renderScans(recordedScans);
        String liveText = renderScans(liveScans);
        long recordedRows = HboScanDescriptor.UNKNOWN;
        long liveRows = HboScanDescriptor.UNKNOWN;
        for (int i = 0; i < recordedScans.size(); i++) {
            HboScanDescriptor recorded = recordedScans.get(i);
            HboScanDescriptor live = liveScans.get(i);
            String scanState;
            if (!recorded.getTable().equals(live.getTable())) {
                // the same fingerprint describes the same scans in the same order, so this cannot
                // happen for an entry of this group: refuse to judge rather than guess
                scanState = STATE_UNKNOWN;
            } else {
                scanState = stateOf(recorded, live);
            }
            if (worse(scanState, state).equals(scanState)) {
                state = scanState;
                recordedRows = recorded.getScanRows();
                liveRows = live.getScanRows();
            }
        }
        return new HboStructFreshness(state, recordedText, liveText, recordedRows, liveRows);
    }

    /**
     * Judge an entry against the catalog as a whole (used by {@code HBO SHOW STATISTICS} and
     * {@code HBO DELETE STALE STATISTICS}, which cannot run the query of an entry): the recorded
     * visible version and row count of every table against the current ones.
     *
     * <p>Row counts are only comparable when the scan read the whole table (no partition was pruned
     * away), because only then the recorded rows are the rows of the table.
     */
    public static HboStructFreshness of(String canonicalStructInfo) {
        List<HboScanDescriptor> recordedScans = HboScanDescriptor.parseAll(canonicalStructInfo);
        if (recordedScans.isEmpty()) {
            return new HboStructFreshness(STATE_UNKNOWN, "", "", HboScanDescriptor.UNKNOWN,
                    HboScanDescriptor.UNKNOWN);
        }
        String state = STATE_LIVE;
        List<String> liveTexts = new ArrayList<>();
        long recordedRows = HboScanDescriptor.UNKNOWN;
        long liveRows = HboScanDescriptor.UNKNOWN;
        for (HboScanDescriptor recorded : recordedScans) {
            OlapTable table = resolveTable(recorded.getTable());
            if (table == null) {
                if (worse(STATE_UNKNOWN, state).equals(STATE_UNKNOWN)) {
                    state = STATE_UNKNOWN;
                }
                liveTexts.add(recorded.getTable() + ":-");
                continue;
            }
            long currentVersion = visibleVersionOf(table);
            // the rows of the whole table, computed the same way the recorded rows of a whole table
            // scan were computed, so the two numbers describe the same thing
            long currentRows = HboScanDescriptor.scanRowsOf(table, new ArrayList<>(table.getPartitionIds()),
                    table.getBaseIndexId());
            liveTexts.add(recorded.getTable() + ":v" + currentVersion
                    + (currentRows == HboScanDescriptor.UNKNOWN ? "" : ",r" + currentRows));
            String scanState;
            if (recorded.hasVisibleVersion() && currentVersion == recorded.getVisibleVersion()) {
                scanState = STATE_LIVE;
            } else if (!recorded.hasVisibleVersion() && !recorded.hasScanRows()) {
                // nothing was recorded for this scan: it applies as it always was, nothing to judge
                scanState = STATE_UNKNOWN;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (threshold() <= 0) {
                scanState = STATE_STALE;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (!recorded.hasScanRows()) {
                // the entry records no row count at all (a struct literal of an older format, for
                // example), so the version check is the only signal left: the same rule the read
                // side applies, and such an entry should be injected again
                scanState = STATE_STALE;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (!recorded.isPartitionSelectionComplete() || currentRows == HboScanDescriptor.UNKNOWN) {
                // the recorded rows are the rows of the pruned partitions only (or the current row
                // count cannot be read): the entry stays applicable, only a query can judge it
                scanState = STATE_UNKNOWN;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else {
                scanState = changeRatio(recorded.getScanRows(), currentRows) <= threshold()
                        ? STATE_DRIFTED : STATE_STALE;
            }
            if (worse(scanState, state).equals(scanState) && scanState != STATE_LIVE) {
                state = scanState;
                recordedRows = scanState == STATE_UNKNOWN
                        ? HboScanDescriptor.UNKNOWN : recorded.getScanRows();
                liveRows = scanState == STATE_UNKNOWN ? HboScanDescriptor.UNKNOWN : currentRows;
            }
        }
        return new HboStructFreshness(state, renderScans(recordedScans), String.join("; ", liveTexts),
                recordedRows, liveRows);
    }

    /**
     * The state of one scan of the current query against the same scan of an entry.
     * See the class comment for the meaning of the states.
     */
    private static String stateOf(HboScanDescriptor recorded, HboScanDescriptor live) {
        if (!recorded.hasVisibleVersion() && !recorded.hasScanRows()) {
            // the entry records nothing about this scan (e.g. a struct literal typed by hand)
            return STATE_UNKNOWN;
        }
        if (recorded.hasSameVersion(live)) {
            return STATE_LIVE;
        }
        if (threshold() <= 0) {
            // strict mode: a data change of any size invalidates the entry
            return STATE_STALE;
        }
        if (!recorded.hasScanRows() || !live.hasScanRows()) {
            // no row count on one side, so the version check is the only signal left: a version
            // which moved is not a hit
            return STATE_STALE;
        }
        return changeRatio(recorded.getScanRows(), live.getScanRows()) <= threshold()
                ? STATE_DRIFTED : STATE_STALE;
    }

    /** How much the row count of a scan may change and still be reused; a non-positive value is strict. */
    private static double threshold() {
        return Config.hbo_row_count_change_ratio;
    }

    /** {@code |now - recorded| / recorded}; 1 when the recorded side is empty and the other is not. */
    private static double changeRatio(long recordedRows, long liveRows) {
        if (recordedRows == 0) {
            return liveRows == 0 ? 0 : 1;
        }
        return Math.abs(liveRows - recordedRows) / (double) recordedRows;
    }

    /** {@code +3.0%} / {@code -20.0%}: how the current row count differs from the recorded one. */
    private static String changeText(long recordedRows, long liveRows) {
        double change = recordedRows == 0
                ? (liveRows == 0 ? 0 : 1) : (liveRows - recordedRows) / (double) recordedRows;
        return String.format(Locale.ROOT, "%+.1f%%", change * 100);
    }

    /** The worse of two states (live, drifted, unknown, stale - in this order). */
    private static String worse(String left, String right) {
        return severity(left) >= severity(right) ? left : right;
    }

    private static int severity(String state) {
        switch (state) {
            case STATE_STALE:
                return 3;
            case STATE_UNKNOWN:
                return 2;
            case STATE_DRIFTED:
                return 1;
            default:
                return 0;
        }
    }

    /** Render the baseline of a scan, e.g. {@code hbo_test.t1:v3,r1000,p1/5}. */
    private static String renderScans(List<HboScanDescriptor> scans) {
        Set<String> rendered = new LinkedHashSet<>();
        for (HboScanDescriptor scan : scans) {
            StringBuilder sb = new StringBuilder(scan.getTable());
            if (scan.hasVisibleVersion()) {
                sb.append(":v").append(scan.getVisibleVersion());
            }
            if (scan.hasScanRows()) {
                sb.append(",r").append(scan.getScanRows());
            }
            if (scan.getTotalPartitions() != HboScanDescriptor.UNKNOWN
                    && !scan.isPartitionSelectionComplete()) {
                sb.append(",p").append(scan.getSelectedPartitions()).append('/')
                        .append(scan.getTotalPartitions());
            }
            rendered.add(sb.toString());
        }
        return String.join("; ", rendered);
    }

    /** The current visible version of a table, or {@link HboScanDescriptor#UNKNOWN}. */
    private static long visibleVersionOf(OlapTable table) {
        try {
            return table.getVisibleVersion();
        } catch (RpcException | RuntimeException e) {
            LOG.debug("failed to read the visible version of {}", table.getName(), e);
            return HboScanDescriptor.UNKNOWN;
        }
    }

    /** Resolve {@code [catalog.]db.table} to the olap table, or null when it does not exist. */
    private static OlapTable resolveTable(String fullName) {
        String[] parts = fullName.split("\\.");
        try {
            if (parts.length < 2 || parts.length > 3) {
                return null;
            }
            String catalogName = parts.length == 3 ? parts[0] : null;
            String dbName = parts[parts.length - 2];
            String tableName = parts[parts.length - 1];
            CatalogIf<?> catalog = catalogName == null
                    ? Env.getCurrentEnv().getCurrentCatalog()
                    : Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName.toLowerCase(Locale.ROOT));
            if (catalog == null) {
                return null;
            }
            DatabaseIf<?> database = catalog.getDbNullable(dbName);
            if (database == null) {
                return null;
            }
            TableIf table = database.getTableNullable(tableName);
            return table instanceof OlapTable ? (OlapTable) table : null;
        } catch (RuntimeException e) {
            // an entry whose table cannot be resolved is reported as unknown instead of failing
            // the SHOW / DELETE statement
            LOG.debug("failed to resolve {} for a hbo entry", fullName, e);
            return null;
        }
    }
}
