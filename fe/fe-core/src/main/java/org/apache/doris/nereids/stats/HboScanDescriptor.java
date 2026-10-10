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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.statistics.analysis.TableStatsMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * The data state of one scan of a hbo struct info: the table it reads, the table visible version,
 * the number of rows the scan reads (the sum of the row counts of its selected partitions) and which
 * part of the table partitions it selects.
 *
 * <p>These numbers are the <b>baseline</b> a hbo entry is judged against. The scan token of a
 * canonical struct info carries them as an annotation
 * ({@code S{db.t,v3,r1000,p1/5}}), so a user copies the state the entry was measured in together
 * with the struct info, and the read side compares them with the state of the current query to
 * decide whether the injected row count may still be applied (see {@link HboStructFreshness}).
 *
 * <p>None of these numbers takes part in the fingerprint:
 * {@link GroupStructInfo#stripScanBaseline} removes the whole baseline of every scan token before
 * the sha256 is taken, so a table which simply keeps growing neither changes the key of an entry nor
 * makes it unmatchable; whether the entry may still be applied is a read side decision.
 *
 * <p>A number which could not be determined is omitted from the token (and parsed back as
 * {@link #UNKNOWN}), e.g. the row count of a table whose row count was never reported by any
 * backend: such an entry cannot be compared by rows any more and falls back to the version check.
 */
public class HboScanDescriptor {
    /** A number which could not be determined; a row count of 0 is a known, meaningful value. */
    public static final long UNKNOWN = -1;

    private final String table;
    private final long visibleVersion;
    private final long scanRows;
    private final RowsKind rowsKind;
    private final long selectedPartitions;
    private final long totalPartitions;

    HboScanDescriptor(String table, long visibleVersion, long scanRows, RowsKind rowsKind,
            long selectedPartitions, long totalPartitions) {
        this.table = table;
        this.visibleVersion = visibleVersion;
        this.scanRows = scanRows;
        this.rowsKind = rowsKind;
        this.selectedPartitions = selectedPartitions;
        this.totalPartitions = totalPartitions;
    }

    /**
     * How the row count of a scan was obtained.
     */
    public enum RowsKind {
        /**
         * Every selected partition reported its own row count, so the number is the physical size of
         * the data the scan reads.
         */
        MEASURED,
        /**
         * The row count of at least one selected partition is not available, so the number is
         * derived from the table (its last reported row count, or the statistics of the last
         * analysis plus the rows loaded since). Two such numbers are comparable with each other, but
         * a number of this kind must never be compared with a {@link #MEASURED} one: the derivation
         * can be off by a large factor, e.g. for the partitions of a skewed table.
         */
        ESTIMATED,
        /** No row count at all (the table was neither analyzed nor reported by a backend). */
        UNKNOWN
    }

    /** A row count and how it was obtained. */
    public static class Rows {
        private static final Rows UNKNOWN_ROWS = new Rows(UNKNOWN, RowsKind.UNKNOWN);

        private final long rows;
        private final RowsKind kind;

        Rows(long rows, RowsKind kind) {
            this.rows = rows;
            this.kind = kind;
        }

        public long getRows() {
            return rows;
        }

        public RowsKind getKind() {
            return kind;
        }

        public boolean isKnown() {
            return kind != RowsKind.UNKNOWN;
        }
    }

    /** The state of a scan node: which table it reads, and how much of it. */
    public static HboScanDescriptor of(OlapScan scan) throws RpcException {
        OlapTable table = scan.getTable();
        List<Long> selectedPartitionIds = scan.getSelectedPartitionIds();
        Rows rows = rowsOf(table, selectedPartitionIds, scan.getSelectedIndexId());
        return new HboScanDescriptor(table.getNameWithFullQualifiers(), table.getVisibleVersion(),
                rows.getRows(), rows.getKind(), selectedPartitionIds.size(),
                table.getPartitionNames().size());
    }

    /**
     * The rows the given partitions of a table hold, computed the way the optimizer computes the
     * input size of a scan (see {@code StatsCalculator.getOlapTableRowCount} /
     * {@code getSelectedPartitionRowCount}): the row count of a partition as the table statistics
     * report it, the rows loaded since the last analysis as the fallback for the whole table, and -
     * for a partition whose own row count is not available (e.g. right after a load, before the
     * backend reported it again) - the average of the table.
     *
     * <p>An entry records this number and the read side computes it again, so both sides describe
     * the same quantity (the size of the input the row count was estimated on) instead of a
     * physical row count which would be read from a different place on each side.
     * {@link #UNKNOWN} is returned only when the table has no row count at all (never analyzed and
     * never reported by a backend).
     */
    public static Rows rowsOf(OlapTable table, List<Long> partitionIds, long indexId) {
        long rows = 0;
        long unknownPartitions = 0;
        for (long partitionId : partitionIds) {
            long partitionRows = reportedRowsOf(table, partitionId, indexId);
            if (partitionRows == UNKNOWN) {
                unknownPartitions++;
            } else {
                rows += partitionRows;
            }
        }
        if (unknownPartitions == 0) {
            return new Rows(rows, RowsKind.MEASURED);
        }
        long tableRows = tableRowsOf(table, indexId);
        if (tableRows == UNKNOWN) {
            return Rows.UNKNOWN_ROWS;
        }
        // a partition whose row count is unknown holds at least one row, the rest is the average of
        // the table (the same rule the optimizer uses for the same situation). The result is an
        // estimate and is marked as one, so that the drift verdict never compares it with a measured
        // row count.
        return new Rows(rows + Math.max(unknownPartitions,
                tableRows * unknownPartitions / table.getPartitionNum()), RowsKind.ESTIMATED);
    }

    /**
     * The row count a backend reported for one partition, or {@link #UNKNOWN} when no backend
     * reported it (or when a newer load invalidated the report): only such a number is a
     * measurement of the partition, everything else is derived by {@link #rowsOf}.
     */
    private static long reportedRowsOf(OlapTable table, long partitionId, long indexId) {
        // the strict accessor is the only one which distinguishes an index no backend reported yet
        // (-1) from an empty one (0): the non strict one maps both to 0, which would make a never
        // reported partition look like a measurement of zero rows
        return table.getRowCountForPartitionIndex(partitionId, indexId, true);
    }

    /** The rows of a whole table, with the rows loaded since the last analysis as the fallback. */
    public static long tableRowsOf(OlapTable table, long indexId) {
        long rows = table.getRowCountForIndex(indexId, true);
        if (rows != UNKNOWN) {
            return rows;
        }
        TableStatsMeta tableMeta = Env.getCurrentEnv().getAnalysisManager()
                .findTableStatsStatus(table.getId());
        if (tableMeta == null || tableMeta.userInjected) {
            return tableMeta == null ? UNKNOWN : tableMeta.getRowCount(indexId);
        }
        long analyzedRows = tableMeta.getRowCount(indexId);
        return analyzedRows == UNKNOWN ? UNKNOWN
                : analyzedRows + tableMeta.getBaseIndexDeltaRowCount(table);
    }

    /**
     * Parse the content of a scan token, as printed by {@link #render()}. Missing parts (an unknown
     * number, or a selection which covers every partition) are {@link #UNKNOWN} / all partitions.
     */
    public static HboScanDescriptor parse(String header) {
        String[] parts = header.split(",");
        long version = UNKNOWN;
        long rows = UNKNOWN;
        RowsKind kind = RowsKind.UNKNOWN;
        long selected = UNKNOWN;
        long total = UNKNOWN;
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            if (part.startsWith("v")) {
                version = parseLong(part.substring(1));
            } else if (part.startsWith("r")) {
                rows = parseLong(part.substring(1));
                kind = RowsKind.MEASURED;
            } else if (part.startsWith("e")) {
                rows = parseLong(part.substring(1));
                kind = RowsKind.ESTIMATED;
            } else if (part.startsWith("p")) {
                int slash = part.indexOf('/');
                if (slash > 0) {
                    selected = parseLong(part.substring(1, slash));
                    total = parseLong(part.substring(slash + 1));
                }
            }
        }
        if (selected == UNKNOWN) {
            // no partition part: the scan selects every partition of the table
            selected = total;
        }
        return new HboScanDescriptor(parts[0], version, rows, kind, selected, total);
    }

    /** The content of the scan token of this scan: {@code db.t,v3,r1000,p1/5}. */
    public String render() {
        StringBuilder sb = new StringBuilder(table);
        if (visibleVersion != UNKNOWN) {
            sb.append(",v").append(visibleVersion);
        }
        // a measured row count is marked 'r', an estimated one 'e': the verdict only ever compares
        // two numbers of the same kind (see HboStructFreshness)
        if (rowsKind == RowsKind.MEASURED) {
            sb.append(",r").append(scanRows);
        } else if (rowsKind == RowsKind.ESTIMATED) {
            sb.append(",e").append(scanRows);
        }
        if (totalPartitions != UNKNOWN && selectedPartitions != totalPartitions) {
            sb.append(",p").append(selectedPartitions).append('/').append(totalPartitions);
        }
        return sb.toString();
    }

    /** Every scan of a canonical struct info, in the order the tokens appear in it. */
    public static List<HboScanDescriptor> parseAll(String canonicalStructInfo) {
        List<HboScanDescriptor> scans = new ArrayList<>();
        if (canonicalStructInfo == null || canonicalStructInfo.isEmpty()) {
            return scans;
        }
        int index = 0;
        while ((index = nextScanTokenStart(canonicalStructInfo, index)) >= 0) {
            int end = canonicalStructInfo.indexOf('}', index);
            if (end < 0) {
                break;
            }
            scans.add(parse(canonicalStructInfo.substring(index + 2, end)));
            index = end + 1;
        }
        return scans;
    }

    /**
     * The index of the next scan token at or after {@code from}, or -1. A scan token is an
     * {@code S} group, which always follows the start of the string, an opening parenthesis or a
     * separator, so a column whose name contains {@code S} cannot be mistaken for one.
     */
    public static int nextScanTokenStart(String canonicalStructInfo, int from) {
        int index = from;
        while ((index = canonicalStructInfo.indexOf("S{", index)) >= 0) {
            if (index == 0 || "();[".indexOf(canonicalStructInfo.charAt(index - 1)) >= 0) {
                return index;
            }
            index += 2;
        }
        return -1;
    }

    /** The table of this scan, as {@code [catalog.]db.table}. */
    public String getTable() {
        return table;
    }

    public long getVisibleVersion() {
        return visibleVersion;
    }

    /** The rows the scan reads, or {@link #UNKNOWN}. */
    public long getScanRows() {
        return scanRows;
    }

    public long getSelectedPartitions() {
        return selectedPartitions;
    }

    public long getTotalPartitions() {
        return totalPartitions;
    }

    /** Whether the scan reads the whole table (no partition was pruned away). */
    public boolean isPartitionSelectionComplete() {
        return selectedPartitions == totalPartitions;
    }

    /** Whether the version of the table was recorded. */
    public boolean hasVisibleVersion() {
        return visibleVersion != UNKNOWN;
    }

    /** How the recorded row count was obtained. */
    public RowsKind getRowsKind() {
        return rowsKind;
    }

    /** Whether the rows of the scan were recorded (measured or estimated). */
    public boolean hasScanRows() {
        return rowsKind != RowsKind.UNKNOWN;
    }

    /** Whether two row counts may be compared with each other. */
    public static boolean isRowCountComparable(HboScanDescriptor left, HboScanDescriptor right) {
        return left.hasScanRows() && right.hasScanRows() && left.getRowsKind() == right.getRowsKind();
    }

    /** Whether the recorded version equals the version of {@code other} (impossible when unknown). */
    public boolean hasSameVersion(HboScanDescriptor other) {
        return hasVisibleVersion() && visibleVersion == other.getVisibleVersion();
    }

    private static long parseLong(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return UNKNOWN;
        }
    }
}
