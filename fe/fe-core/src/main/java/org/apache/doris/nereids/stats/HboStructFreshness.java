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
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.hbo.PlanStatistics;
import org.apache.doris.statistics.hbo.ScanPlanStatistics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 *   <li>{@link #STATE_LIVE}: the data the entry was measured on is still the current one, the entry
 *       is applied;</li>
 *   <li>{@link #STATE_DRIFTED}: the data moved, but by at most
 *       {@code Config.hbo_row_count_change_ratio} of the recorded row count (10% by default): the
 *       entry is applied, which is the point of the tolerance - a table which merely keeps growing
 *       (e.g. a few new rows in a partition) does not invalidate a measured row count;</li>
 *   <li>{@link #STATE_STALE}: the data moved further than that: the entry is <b>not</b> applied, and
 *       the query falls back to the optimizer estimation;</li>
 *   <li>{@link #STATE_UNKNOWN}: nothing can be compared (the entry records no row count, or the
 *       current one cannot be read / was obtained a different way), so there is nothing to judge: the
 *       entry is applied as it always was.</li>
 * </ul>
 * The recorded row count is never scaled: an entry either applies with the value the user measured
 * or does not apply at all ("soft strategy" - a user who needs the exact recorded state in any case
 * should not inject hbo statistics for that query). Setting
 * {@code hbo_row_count_change_ratio} to 0 or less switches the verdict back to the strict semantics
 * hbo had before the baseline existed: the recorded visible version has to still be the current one.
 *
 * <p>The rows are compared <b>before</b> the version, and only against a number which was obtained
 * the same way ({@link HboScanDescriptor.RowsKind}): the visible version is a property of the table,
 * so two different partition selections of one table share it while reading completely different
 * amounts of data, and a measured row count must not be compared with one that was derived from the
 * table average. Whenever nothing can be compared - a hand written struct literal, an entry whose
 * rows were never measured, a current row count which cannot be read, or two numbers of different
 * kinds - the state is {@link #STATE_UNKNOWN} and the entry is applied: a transient failure to read
 * the catalog must not drop an entry the user injected. The version is only decisive in strict mode
 * ({@code hbo_row_count_change_ratio <= 0}).
 *
 * <p>A scan is judged by the rows of its <b>selected</b> partitions, so pruning decides which data
 * the entry depends on: an unrelated partition which grows does not invalidate a pruned entry.
 * {@code HBO SHOW STATISTICS} and {@code HBO DELETE STALE STATISTICS} cannot run the query of an
 * entry, so there the recorded state is compared with the table as a whole; an entry whose struct
 * prunes partitions can therefore only be judged by rows when it was recorded as a whole table scan
 * (its state is reported as unknown otherwise, and such an entry is kept unless the user asks for
 * {@code OLDER_THAN}).
 *
 * <p>Learned (profile collected) entries carry the input table statistics of the run they were
 * measured in, so they are judged by the same tolerance over those recorded rows; an entry injected
 * by {@code HBO SET LEARNED STATISTICS} carries no such statistics and is applied as it always was.
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
     * Whether the entry records any data state at all. An entry which does not (a struct literal
     * typed by hand, or one copied from a plan whose tables were never reported) is always applied,
     * so a caller does not have to compute the state of the current query to find that out.
     */
    public static boolean hasRecordedDataState(String recordedCanonical) {
        for (HboScanDescriptor scan : HboScanDescriptor.parseAll(recordedCanonical)) {
            if (scan.hasVisibleVersion() || scan.hasScanRows()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Judge an entry against the current query: the data state recorded in {@code recordedCanonical}
     * (the struct info of the entry) against the state of {@code liveCanonical} (the struct info the
     * read side just computed for the group). This is the decision of the read side.
     */
    public static HboStructFreshness between(String recordedCanonical, GroupStructInfo liveStructInfo) {
        return between(recordedCanonical, liveStructInfo.getScans());
    }

    /** The same comparison for a caller which has the scans of the current query only. */
    static HboStructFreshness between(String recordedCanonical, List<HboScanDescriptor> liveScans) {
        List<HboScanDescriptor> recordedScans = HboScanDescriptor.parseAll(recordedCanonical);
        String liveText = renderScans(liveScans);
        if (recordedScans.isEmpty()) {
            // an entry without a baseline (a hand written struct literal): nothing to judge
            return new HboStructFreshness(STATE_UNKNOWN, "", liveText,
                    HboScanDescriptor.UNKNOWN, HboScanDescriptor.UNKNOWN);
        }
        if (recordedScans.size() != liveScans.size()) {
            // the entry describes a different number of scans than the group: cannot be compared
            return new HboStructFreshness(STATE_UNKNOWN, renderScans(recordedScans), liveText,
                    HboScanDescriptor.UNKNOWN, HboScanDescriptor.UNKNOWN);
        }
        String state = STATE_LIVE;
        String recordedText = renderScans(recordedScans);

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
        // the current state of every table, de duplicated like the recorded side (a self join reads
        // the same table twice, which would only repeat the same numbers)
        Set<String> liveTexts = new LinkedHashSet<>();
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
            if (table.getIndexIdList().size() > 1 && !recorded.isPartitionSelectionComplete()) {
                // the table has more than one index (a rollup) and the entry records only a partition
                // count, so this statement cannot tell which index its row count came from: judging it
                // against the base index could be wrong in both directions, so it is left to a query
                liveTexts.add(recorded.getTable() + ":-");
                if (worse(STATE_UNKNOWN, state).equals(STATE_UNKNOWN)) {
                    state = STATE_UNKNOWN;
                }
                continue;
            }
            long currentVersion = visibleVersionOf(table);
            // the rows of the whole table, computed the same way the recorded rows of a whole table
            // scan were computed, so the two numbers describe the same thing
            HboScanDescriptor.Rows liveRowsOfTable = HboScanDescriptor.rowsOf(table,
                    new ArrayList<>(table.getPartitionIds()), table.getBaseIndexId());
            long currentRows = liveRowsOfTable.getRows();
            HboScanDescriptor.RowsKind currentRowsKind = liveRowsOfTable.getKind();
            liveTexts.add(recorded.getTable() + ":v" + currentVersion
                    + (currentRows == HboScanDescriptor.UNKNOWN ? ""
                            : (currentRowsKind == HboScanDescriptor.RowsKind.MEASURED ? ",r" : ",e")
                                    + currentRows));
            String scanState;
            if (!recorded.hasVisibleVersion() && !recorded.hasScanRows()) {
                // nothing was recorded for this scan: it applies as it always was, nothing to judge
                scanState = STATE_UNKNOWN;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (threshold() <= 0) {
                scanState = recorded.hasVisibleVersion() && currentVersion == recorded.getVisibleVersion()
                        ? STATE_LIVE : STATE_STALE;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (recorded.hasVisibleVersion() && currentVersion == recorded.getVisibleVersion()) {
                // the visible version is a property of the whole table, so an unchanged version means
                // that nothing was loaded into any of its partitions: the recorded state is still the
                // current one, whatever the entry was measured on. (The read side can not use this
                // shortcut: it knows which partitions the entry was measured on and which ones the
                // query reads, and those can differ under one unchanged version.)
                scanState = STATE_LIVE;
            } else if (!recorded.hasScanRows() || !recorded.isPartitionSelectionComplete()) {
                // the entry records no row count at all (a struct literal of an older format, for
                // example), or it records the rows of pruned partitions only, which are not the rows
                // of the table: the state can only be judged by a query of that entry
                scanState = STATE_UNKNOWN;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (currentRows == HboScanDescriptor.UNKNOWN
                    || currentRowsKind != recorded.getRowsKind()) {
                // the current row count cannot be read or was obtained differently than the recorded
                // one: the version is the only comparable signal
                scanState = recorded.hasVisibleVersion() && currentVersion == recorded.getVisibleVersion()
                        ? STATE_LIVE : STATE_STALE;
                currentRows = HboScanDescriptor.UNKNOWN;
            } else if (recorded.getScanRows() == currentRows) {
                scanState = STATE_LIVE;
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
     * Judge a learned (profile collected) entry: the rows its input tables had when it was measured
     * against the rows of the scans of the current query. The caller must only use this for an entry
     * of a node whose recorded rows are the rows of the data it reads (a scan node: see
     * {@code HboStatsCalculator.applyLearnedStats}). The input tables are aggregated by table name, because
     * a learned entry describes the scans of the node it was published for in plan order while the
     * struct info orders them canonically.
     *
     * <p>This is a one way guard: it only rejects an entry when a comparable pair of row counts
     * proves that the data moved beyond the tolerance. Everything which cannot be compared (an
     * injected entry without input statistics, a different number of tables, a row count of a
     * different kind) leaves the entry applicable, so a learned lookup which worked before this
     * check keeps working.
     */
    public static HboStructFreshness ofLearnedEntry(List<PlanStatistics> recordedInputStatistics,
            GroupStructInfo liveStructInfo) {
        return ofLearnedEntry(recordedInputStatistics, liveStructInfo.getScans());
    }

    /** The same guard for a caller which has the scans of the current query only. */
    static HboStructFreshness ofLearnedEntry(List<PlanStatistics> recordedInputStatistics,
            List<HboScanDescriptor> liveScans) {
        Map<String, Long> recordedRowsByTable = new LinkedHashMap<>();
        if (recordedInputStatistics != null) {
            for (PlanStatistics input : recordedInputStatistics) {
                if (!(input instanceof ScanPlanStatistics)) {
                    continue;
                }
                PhysicalOlapScan scan = ((ScanPlanStatistics) input).getScan();
                if (scan == null || scan.getTable() == null) {
                    continue;
                }
                String table = scan.getTable().getNameWithFullQualifiers();
                // the input rows of a scan node are the rows it read, i.e. the number a catalog row
                // count (which also ignores the predicates) can be compared with
                recordedRowsByTable.merge(table, input.getInputRows(), Long::sum);
            }
        }
        if (recordedRowsByTable.isEmpty()) {
            // an injected learned entry carries no input table statistics: nothing to compare
            return new HboStructFreshness(STATE_UNKNOWN, "", renderScans(liveScans),
                    HboScanDescriptor.UNKNOWN, HboScanDescriptor.UNKNOWN);
        }
        return ofLearnedRows(recordedRowsByTable, liveScans);
    }

    /**
     * The comparison of a learned entry: the rows it recorded for each input table against the rows
     * of the scans of the current query. Only measured row counts are comparable with the recorded
     * ones (a learned number is a measurement of a real run), and a table whose rows cannot be
     * compared leaves the entry applicable.
     */
    static HboStructFreshness ofLearnedRows(Map<String, Long> recordedRowsByTable,
            List<HboScanDescriptor> liveScans) {
        Map<String, long[]> liveByTable = new LinkedHashMap<>();
        Map<String, HboScanDescriptor.RowsKind> liveKinds = new LinkedHashMap<>();
        for (HboScanDescriptor scan : liveScans) {
            long[] rows = liveByTable.computeIfAbsent(scan.getTable(), key -> new long[] {0});
            if (scan.hasScanRows()) {
                rows[0] += scan.getScanRows();
            } else {
                rows[0] = HboScanDescriptor.UNKNOWN;
            }
            HboScanDescriptor.RowsKind kind = liveKinds.get(scan.getTable());
            // a table which is scanned twice is only comparable when both scans agree on the kind
            liveKinds.put(scan.getTable(), kind == null || kind == scan.getRowsKind()
                    ? scan.getRowsKind() : HboScanDescriptor.RowsKind.UNKNOWN);
        }
        String state = STATE_LIVE;
        boolean compared = false;
        long recordedRows = HboScanDescriptor.UNKNOWN;
        long liveRows = HboScanDescriptor.UNKNOWN;
        for (Map.Entry<String, Long> entry : recordedRowsByTable.entrySet()) {
            long[] live = liveByTable.get(entry.getKey());
            long recorded = entry.getValue();
            if (live == null || recorded == HboScanDescriptor.UNKNOWN
                    || live[0] == HboScanDescriptor.UNKNOWN
                    || liveKinds.get(entry.getKey()) != HboScanDescriptor.RowsKind.MEASURED) {
                // the learned numbers are measured row counts, so they are only compared with a
                // measured one: a derived number could be off by a large factor
                continue;
            }
            compared = true;
            String tableState = recorded == live[0] ? STATE_LIVE
                    : changeRatio(recorded, live[0]) <= threshold() ? STATE_DRIFTED : STATE_STALE;
            if (worse(tableState, state).equals(tableState)) {
                state = tableState;
                recordedRows = recorded;
                liveRows = live[0];
            }
        }
        if (!compared) {
            // nothing could be compared, so the entry is applied as it always was
            return new HboStructFreshness(STATE_UNKNOWN, renderTables(recordedRowsByTable),
                    renderTables(liveByTable), HboScanDescriptor.UNKNOWN, HboScanDescriptor.UNKNOWN);
        }
        return new HboStructFreshness(state, renderTables(recordedRowsByTable), renderTables(liveByTable),
                recordedRows, liveRows);
    }

    /** Render the aggregated row counts of a learned entry, e.g. {@code hbo_test.t1:r1000}. */
    private static String renderTables(Map<String, ? extends Object> rowsByTable) {
        List<String> rendered = new ArrayList<>();
        for (Map.Entry<String, ? extends Object> entry : rowsByTable.entrySet()) {
            long rows = entry.getValue() instanceof long[] ? ((long[]) entry.getValue())[0]
                    : (Long) entry.getValue();
            // a learned number is a measurement of a real run, so it is rendered as one
            rendered.add(entry.getKey() + (rows == HboScanDescriptor.UNKNOWN ? "" : ":r" + rows));
        }
        return String.join("; ", rendered);
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
        if (threshold() <= 0) {
            // strict mode: only the recorded data state may be reused - the semantics hbo had before
            // the baseline existed, when the visible version was part of the key
            return recorded.hasSameVersion(live) ? STATE_LIVE : STATE_STALE;
        }
        if (recorded.hasScanRows() && live.hasScanRows()) {
            if (recorded.getRowsKind() != live.getRowsKind()) {
                // the two numbers were obtained differently (one is a measurement of the partitions
                // the scan selected, the other one is derived from the whole table), so they are not
                // comparable: comparing them would reject entries whose data did not change at all,
                // and the version alone can not say whether the two describe the same data (the same
                // version covers every partition of the table). The state is therefore reported as
                // unknown and the entry is applied as it always was.
                return STATE_UNKNOWN;
            }
            // the rows are compared BEFORE the version: a different partition selection (or another
            // scan of the same table) can read a completely different amount of data under the very
            // same table version, so equal versions do not mean that the entry still fits
            if (recorded.getScanRows() == live.getScanRows()) {
                return STATE_LIVE;
            }
            return changeRatio(recorded.getScanRows(), live.getScanRows()) <= threshold()
                    ? STATE_DRIFTED : STATE_STALE;
        }
        // the recorded baseline was never a measurement, or the current row count cannot be read:
        // there is nothing to judge, so the entry is applied as it always was
        return STATE_UNKNOWN;
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
                sb.append(scan.getRowsKind() == HboScanDescriptor.RowsKind.MEASURED ? ",r" : ",e")
                        .append(scan.getScanRows());
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
