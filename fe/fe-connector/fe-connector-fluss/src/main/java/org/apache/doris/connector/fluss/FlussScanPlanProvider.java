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

package org.apache.doris.connector.fluss;

import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.metadata.KvSnapshots;
import org.apache.fluss.client.metadata.LakeSnapshot;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.DataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Turns a fluss table into the scan ranges that cover it.
 *
 * <p>A log table is read one bucket at a time, from the earliest offset fluss still holds up to the
 * offset the log had reached when planning ran. That stopping offset is taken once, for all of a
 * partition's buckets, so every bucket of a partition stops at the same view of the table; without it
 * a bucket planned later would read rows written after the query started while an earlier bucket did
 * not. A bucket that has never been written to (stopping offset 0) yields no range at all.
 *
 * <p>A primary-key table cannot be read that way: its log is a change log, so replaying it verbatim
 * returns superseded and deleted rows. Each bucket is read instead as its latest kv snapshot plus the
 * change log that followed, which the scanner merges by key. A bucket fluss has never snapshotted is
 * rebuilt by replaying its whole change log, which is equally correct and only slower.
 *
 * <p>Planning only reads metadata — partition lists, offsets and snapshot ids — so it is safe to run
 * for an {@code EXPLAIN}, which does reach {@code planScan}. (There is no explain-only signal on the
 * SPI in this branch; the point is that fluss does not need one. A future change that takes a snapshot
 * lease during planning would.)
 *
 * <p>A table that is tiered into a lake is read as the union of the two: the lake at the snapshot fluss
 * says is readable, plus each bucket's log from where that snapshot ended. The lake half is not planned
 * here — it is planned by the paimon sibling connector, pinned to that snapshot, and its ranges are mixed
 * into the same scan node, each wrapped ({@link FlussLakeRange}) so that every range of the scan carries
 * the one table format "fluss" and BE's fluss reader can dispatch it to the sibling's reader stack. That
 * keeps this plugin free of any paimon dependency and gives the lake half paimon's native readers,
 * deletion vectors and file cache for free. Refusing to serve such a table at all is the alternative to
 * avoid: a datalake table read as fluss-only silently returns just the rows tiering has not moved yet,
 * which looks like a working query. {@code fluss.union_read.mode=disabled} is how a user asks for that
 * fluss-only read on purpose.
 *
 * <p>A tiered PRIMARY-KEY table is read as a union too, but its halves overlap by KEY rather than meeting
 * at an offset: the log tail carries updates and deletes of rows the lake already holds. It is split into
 * three parts, per bucket. The lake splits of a bucket whose log has moved on are wrapped with the
 * offsets of that tail ({@code LAKE_SUPPRESS} rather than plain {@code LAKE}), and BE drops the lake rows
 * whose keys the tail names. The surviving state of the tail itself is contributed once, by a
 * {@code PK_TAIL} range. A bucket the lake has never seen is read whole from fluss, as {@code PK_FULL},
 * exactly as it would be without a lake. Every row is therefore produced exactly once, and the read
 * matches what fluss alone would return — which is what makes the fluss-only read the reference the
 * regression tests compare against.
 *
 * <p>Falling back to that fluss-only read is always safe for a primary-key table, in a way it is not for
 * a log table: fluss keeps such a table's state in full, so reading it alone returns the whole table,
 * only slower than reading the lake's columnar files would be. That is why {@code auto} answers every
 * question it cannot answer well — a key column it cannot compare exactly, a tail the log no longer
 * holds — by reading fluss alone rather than by failing, and why {@code required} answers the same
 * questions with an error: that mode exists to make "did this actually read the lake?" answerable in a
 * test.
 */
public class FlussScanPlanProvider implements ConnectorScanPlanProvider {

    /**
     * Prefix marking a node property that belongs to BE rather than to the engine. Everything under it
     * is copied verbatim into {@code TFileScanRangeParams.fluss_properties}; everything else (the
     * engine's own keys, e.g. the partition keys) is not.
     */
    static final String BE_PROPERTY_PREFIX = "fluss.";

    /**
     * Prefix for the fluss client configuration. The scanner strips it and hands the rest to fluss's
     * {@code Configuration}, so the connector never has to enumerate fluss's own option names.
     */
    static final String PROP_CLIENT_PREFIX = "fluss.client.";

    static final String PROP_DB_NAME = "fluss.db_name";
    static final String PROP_TABLE_NAME = "fluss.table_name";

    /**
     * Node property naming the key columns BE compares when it suppresses lake rows: the primary key
     * minus the partition columns, in Doris's own column names. Only the names — the types travel the
     * ordinary way, as the slot descriptors of columns that {@link #getMustReadColumns} kept in the
     * scan's tuple.
     */
    static final String PROP_UNION_PK_NAMES = "fluss.union.pk_names";

    /** Node property carrying {@link FlussCatalogProperties#UNION_READ_MAX_TAIL_ROWS} to both readers. */
    static final String PROP_UNION_MAX_TAIL_ROWS = "fluss.union.max_tail_rows";

    /**
     * The property a lake split carries to say which bucket it holds. Written by the paimon connector
     * (from {@code DataSplit.bucket()}) for this connector's sake, and part of the contract between the
     * two: a fluss table's lake table is bucketed identically, so a lake split can be matched with the
     * log tail of the SAME bucket. Its absence is never treated as "this bucket has no tail" — that would
     * turn a version mismatch into duplicated rows — see {@link #lakeSplitBucket}.
     */
    private static final String LAKE_BUCKET_PROPERTY = "paimon.bucket";

    /** The only lake format that can be delegated today; fluss also defines iceberg / lance / hudi. */
    private static final String PAIMON_LAKE_FORMAT = "paimon";

    /**
     * The SCAN-LEVEL table format, read by BE's scanner selection before it fetches any range. Every reader
     * this connector needs lives in {@code FileScannerV2} only, so a scan planned here must use it whatever
     * {@code enable_file_scanner_v2} says; the legacy scanner's JNI dispatch has no fluss branch and would
     * fail the query with {@code Not supported create reader for table format}. Same literal as the per-range
     * marker ({@link FlussScanRange#getTableFormatType}), and the mirror image of the {@code
     * transactional_hive} stamp, which uses this same channel to force the OPPOSITE choice.
     */
    private static final String SCAN_LEVEL_TABLE_FORMAT = FlussScanRange.TABLE_FORMAT;

    private final FlussAdminOps adminOps;
    private final FlussCatalogProperties catalogProperties;
    private final Function<Map<String, String>, Connector> lakeSiblingFactory;

    /**
     * What the last {@link #planScan} produced, for the EXPLAIN line. Plain fields, not volatile: the
     * engine memoizes one provider instance per scan node and plans that node on the FE planning
     * thread, then renders EXPLAIN from the same thread afterwards. This connector declares neither
     * batch scan nor streaming splits, which are what would move planning off that thread — enabling
     * either means revisiting this (the ES provider carries the same caveat).
     */
    private int plannedLogRanges;
    private int plannedPkRanges;
    private int plannedPkTailRanges;
    private int plannedLakeSplits;
    private int plannedSuppressedLakeSplits;
    private boolean plannedUnionRead;
    private String plannedReadMode = READ_MODE_DEFAULT;

    /** EXPLAIN spellings of {@link FlussTableHandle.ReadMode}, which is a plan anchor and not a Java name. */
    private static final String READ_MODE_DEFAULT = "default";
    private static final String READ_MODE_LOG = "log";

    /**
     * This scan node's lake half, resolved at most once (see {@link #resolveUnionRead}). Same threading
     * argument as the counters above; {@code unionResolved} distinguishes "not asked yet" from "asked, and
     * this table has no lake half".
     */
    private boolean unionResolved;
    private UnionRead unionRead;

    /**
     * Why this scan gave up its lake half, or null when it did not. Only {@code auto} can get here — the
     * same conditions are errors under {@code required} — and the plan that results is the fluss-only read
     * {@code disabled} would have produced, which for a primary-key table is the whole table. It shows up
     * in EXPLAIN because that is otherwise the only difference between "there is no lake to read" and
     * "there is one and this query could not use it".
     */
    private String degradedReason;

    /** {@link #degradedReason} when the log no longer holds the tail the lake snapshot stops before. */
    private static final String DEGRADED_TAIL_TRUNCATED = "tail-truncated";

    /** {@link #degradedReason} when the tails this scan would have to hold are over their ceiling. */
    private static final String DEGRADED_TAIL_TOO_LARGE = "tail-too-large";

    /** {@link #degradedReason} when a key column's values cannot be compared exactly across the halves. */
    private static final String DEGRADED_KEY_TYPE = "key-type";

    /** {@link #degradedReason} when a partition column's values do not render the same way on both sides. */
    private static final String DEGRADED_PARTITION_TYPE = "partition-type";

    public FlussScanPlanProvider(FlussAdminOps adminOps, FlussCatalogProperties catalogProperties,
            Function<Map<String, String>, Connector> lakeSiblingFactory) {
        this.adminOps = adminOps;
        this.catalogProperties = catalogProperties;
        this.lakeSiblingFactory = lakeSiblingFactory;
    }

    /**
     * The lake half of a union read: the sibling connector that owns it, its scan planner, its table handle
     * already pinned to {@link #snapshotId}, and where that snapshot left each bucket's log.
     *
     * <p>A {@code $log} scan resolves the same snapshot for its offsets alone and never reads the lake, so
     * it gets a {@link #boundaryOnly} instance: the offsets and the snapshot id, and no sibling. Everything
     * that reads the lake half asks {@link #hasLakeHalf()} first — the field is left null rather than
     * filled with an unused sibling so that a missed check fails immediately instead of quietly planning a
     * lake read into a scan that promised not to do one.
     */
    private static final class UnionRead {
        private final Connector sibling;
        private final ConnectorScanPlanProvider siblingProvider;
        private final ConnectorTableHandle pinnedLakeHandle;
        private final long snapshotId;
        private final Map<TableBucket, Long> logOffsets;

        private UnionRead(Connector sibling, ConnectorScanPlanProvider siblingProvider,
                ConnectorTableHandle pinnedLakeHandle, long snapshotId,
                Map<TableBucket, Long> logOffsets) {
            this.sibling = sibling;
            this.siblingProvider = siblingProvider;
            this.pinnedLakeHandle = pinnedLakeHandle;
            this.snapshotId = snapshotId;
            this.logOffsets = logOffsets;
        }

        /** Where the lake snapshot leaves each bucket's log, with no lake half to read. */
        private static UnionRead boundaryOnly(long snapshotId, Map<TableBucket, Long> logOffsets) {
            return new UnionRead(null, null, null, snapshotId, logOffsets);
        }

        private boolean hasLakeHalf() {
            return sibling != null;
        }
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        FlussTableHandle handle = (FlussTableHandle) request.getTableHandle();
        UnionRead union = resolveUnionRead(session, handle);

        List<Integer> buckets = allBuckets(handle.getBucketCount());
        List<ConnectorScanRange> ranges = union != null && handle.hasPrimaryKey()
                ? planPrimaryKeyUnion(session, handle, union, buckets, request)
                : planWithoutKeyMerging(session, handle, union, buckets, request);

        plannedReadMode = handle.isLogOnly() ? READ_MODE_LOG : READ_MODE_DEFAULT;
        plannedLogRanges = count(ranges, FlussScanRange.RangeType.LOG);
        plannedPkRanges = count(ranges, FlussScanRange.RangeType.PK_FULL);
        plannedPkTailRanges = count(ranges, FlussScanRange.RangeType.PK_TAIL);
        plannedLakeSplits = countLakeSplits(ranges);
        plannedSuppressedLakeSplits = countSuppressedLakeSplits(ranges);
        // Read back from the field, not from the local: a primary-key plan may have given up its lake half
        // half-way through, and EXPLAIN has to say what was actually planned.
        plannedUnionRead = unionRead != null;
        return ranges;
    }

    /**
     * A log table, or any table read from fluss alone: every bucket end to end, with the lake half — when
     * there is one — prepended.
     *
     * <p>A {@code $log} scan is this same plan with the lake half left out. Its log half is not merely
     * similar to a union read's, it IS one — the same bucket ranges, from the same snapshot offsets — which
     * is what makes {@code tbl$lake} and {@code tbl$log} add up to {@code tbl} rather than merely look as
     * though they should.
     */
    private List<ConnectorScanRange> planWithoutKeyMerging(ConnectorSession session,
            FlussTableHandle handle, UnionRead union, List<Integer> buckets,
            ConnectorScanRequest request) {
        List<ConnectorScanRange> ranges = new ArrayList<>();
        // The lake half first, so its ranges lead the list the way they lead the table's history. It is
        // planned once for the whole table: the sibling prunes partitions from the pushed-down filter, not
        // from the engine's pruned partition list (which it does not consume). No key overlaps the log
        // half here - the two halves meet at an offset - so every lake split is wrapped plain.
        if (union != null && union.hasLakeHalf()) {
            for (ConnectorScanRange lakeSplit : planLakeRanges(session, union, request)) {
                ranges.add(FlussLakeRange.plain(lakeSplit));
            }
        }

        if (handle.isPartitioned()) {
            for (PartitionInfo partition : selectedPartitions(handle, request.getRequiredPartitions())) {
                // fluss's own partition name ("20260101$cn"), not the Doris one: this is a fluss API.
                appendPartitionRanges(ranges, handle, union,
                        FlussPartitions.toScanPartition(partition, handle.getPartitionKeys()),
                        buckets, partition.getPartitionName());
            }
        } else {
            appendPartitionRanges(ranges, handle, union, FlussScanRange.Partition.NONE, buckets, null);
        }
        return ranges;
    }

    /**
     * A primary-key table read as its lake plus the log written since: lake splits bound to the tail of
     * their own bucket, plus one range per bucket for what the lake does not hold.
     *
     * <p>The offsets are read BEFORE the lake half is planned, and that ordering carries the last guard of
     * the design (D17): a tail the log no longer holds cannot be read at all, and a primary-key table
     * cannot fetch it from the lake instead. Finding that out after asking the sibling to plan would mean
     * throwing its work away; finding it out here means the fluss-only read that replaces it is planned
     * from the very same offsets, and is the plan {@code disabled} would have produced.
     */
    private List<ConnectorScanRange> planPrimaryKeyUnion(ConnectorSession session,
            FlussTableHandle handle, UnionRead union, List<Integer> buckets,
            ConnectorScanRequest request) {
        List<PartitionState> states = new ArrayList<>();
        if (handle.isPartitioned()) {
            for (PartitionInfo partition : selectedPartitions(handle, request.getRequiredPartitions())) {
                states.add(readPartitionState(handle, union,
                        FlussPartitions.toScanPartition(partition, handle.getPartitionKeys()),
                        buckets, partition.getPartitionName()));
            }
        } else {
            states.add(readPartitionState(handle, union, FlussScanRange.Partition.NONE, buckets, null));
        }

        String truncated = firstTruncatedTail(states, buckets);
        if (truncated != null) {
            if (catalogProperties.getUnionReadMode()
                    == FlussCatalogProperties.UnionReadMode.REQUIRED) {
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' cannot be read as its lake plus its log: " + truncated
                        + ". Fluss has already deleted part of the log the lake snapshot stops before, and a"
                        + " primary-key table's log cannot be re-read from the lake. Set '"
                        + FlussCatalogProperties.UNION_READ_MODE + "' to auto or disabled to read the"
                        + " table from fluss alone, which still returns every row.");
            }
            degradeToFlussOnly(DEGRADED_TAIL_TRUNCATED);
            return pkRangesFromFlussAlone(states, buckets);
        }

        // The offsets fluss just reported say exactly how many log records each tail holds, so a read too
        // large to hold in memory can be answered by a plan that does not need to hold it -- reading the
        // table from fluss alone returns every row without caching a single tail. Doing it here rather
        // than at read time also means the ceilings are reported once, from the numbers that tripped them,
        // instead of by whichever bucket happened to reach its limit first on some BE.
        String tooLarge = firstTailOverBudget(states, buckets);
        if (tooLarge != null) {
            if (catalogProperties.getUnionReadMode()
                    == FlussCatalogProperties.UnionReadMode.REQUIRED) {
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' cannot be read as its lake plus its log: " + tooLarge
                        + ". Set '" + FlussCatalogProperties.UNION_READ_MODE + "' to auto or disabled to"
                        + " read the table from fluss alone, wait for tiering to move the tail into the"
                        + " lake, or raise the ceiling.");
            }
            degradeToFlussOnly(DEGRADED_TAIL_TOO_LARGE);
            return pkRangesFromFlussAlone(states, buckets);
        }

        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (ConnectorScanRange lakeSplit : planLakeRanges(session, union, request)) {
            ranges.add(bindTailToLakeSplit(handle, lakeSplit, states));
        }
        for (PartitionState state : states) {
            for (int bucket : buckets) {
                BucketState bucketState = state.buckets.get(bucket);
                if (bucketState.lakeEnd == null) {
                    // Never tiered: the lake holds nothing of this bucket, so it is read exactly as it
                    // would be with no lake at all, and no lake split of it can be suppressed.
                    appendPkFullRange(ranges, state, bucket);
                } else if (bucketState.lakeEnd < bucketState.stop) {
                    ranges.add(FlussScanRange.pkTail(state.partition, bucket,
                            bucketState.lakeEnd, bucketState.stop));
                }
                // lakeEnd == stop: the lake holds this bucket entirely and fluss adds nothing.
            }
        }
        return ranges;
    }

    /** What planning read about one partition of a primary-key table, or about an unpartitioned one. */
    private static final class PartitionState {
        private final FlussScanRange.Partition partition;
        private final KvSnapshots snapshots;
        private final Map<Integer, BucketState> buckets;

        private PartitionState(FlussScanRange.Partition partition, KvSnapshots snapshots,
                Map<Integer, BucketState> buckets) {
            this.partition = partition;
            this.snapshots = snapshots;
            this.buckets = buckets;
        }
    }

    /** Where one bucket's log begins, ends, and how much of it the lake already holds. */
    private static final class BucketState {
        /** First offset NOT in the lake, or null when the lake snapshot does not mention this bucket. */
        private final Long lakeEnd;
        /** Where planning saw the log end; 0 for a bucket nothing has ever been written to. */
        private final long stop;
        /** The earliest offset fluss still holds, or null when it did not answer for this bucket. */
        private final Long earliest;

        private BucketState(Long lakeEnd, long stop, Long earliest) {
            this.lakeEnd = lakeEnd;
            this.stop = stop;
            this.earliest = earliest;
        }
    }

    /**
     * Everything planning needs about one partition, read in the one order that is safe: kv snapshots,
     * then the offsets that bound them, then the earliest offsets the guard compares against.
     *
     * <p>The kv snapshots are read even when every bucket turns out to be tiered and none of them is used.
     * That is deliberate: giving up the lake half is decided AFTER the offsets are in hand, and the
     * fluss-only plan that replaces it needs the snapshots — read at that point they would be newer than
     * the offsets that bound them, which is the one ordering this planner exists to prevent.
     */
    private PartitionState readPartitionState(FlussTableHandle handle, UnionRead union,
            FlussScanRange.Partition partition, List<Integer> buckets, String flussPartitionName) {
        TablePath tablePath = handle.toTablePath();
        KvSnapshots snapshots = latestKvSnapshots(tablePath, flussPartitionName);
        Map<Integer, Long> stopping = latestOffsets(tablePath, flussPartitionName, buckets);
        // Last, and that order is what makes it conservative: the earliest offset only moves forward, so
        // one read later can only make the guard stricter, never let a truncated tail through.
        Map<Integer, Long> earliest = earliestOffsets(tablePath, flussPartitionName, buckets);

        Map<Integer, BucketState> byBucket = new LinkedHashMap<>();
        for (int bucket : buckets) {
            TableBucket tableBucket = partition.isPartitioned()
                    ? new TableBucket(handle.getTableId(), partition.getId(), bucket)
                    : new TableBucket(handle.getTableId(), bucket);
            Long stop = stopping.get(bucket);
            byBucket.put(bucket, new BucketState(union.logOffsets.get(tableBucket),
                    stop == null ? 0L : stop, earliest.get(bucket)));
        }
        return new PartitionState(partition, snapshots, byBucket);
    }

    /**
     * The first bucket whose tail fluss can no longer serve, described for an error message, or null when
     * every tail is intact.
     *
     * <p>A tail is the log from where the lake snapshot ended up to where planning saw the log. Fluss
     * deletes old log segments on a timer that does not wait for tiering (only for the kv snapshot), so on
     * a cluster without remote log storage the beginning of that tail can be gone. Reading it anyway
     * returns fewer rows than the table holds, and unlike a log table there is nowhere else to get them
     * from — the lake's copy stops exactly where the missing tail begins.
     *
     * <p>A bucket fluss did not answer for at all counts as truncated: the check is the only thing
     * standing between a deleted tail and a silently short answer, and "could not verify" is not "fine".
     */
    private static String firstTruncatedTail(List<PartitionState> states, List<Integer> buckets) {
        for (PartitionState state : states) {
            for (int bucket : buckets) {
                BucketState bucketState = state.buckets.get(bucket);
                if (bucketState.lakeEnd == null || bucketState.lakeEnd >= bucketState.stop) {
                    continue;
                }
                String where = (state.partition.isPartitioned()
                        ? "partition '" + state.partition.getName() + "', " : "") + "bucket " + bucket;
                if (bucketState.earliest == null) {
                    return where + " (fluss did not report an earliest offset for it)";
                }
                if (bucketState.earliest > bucketState.lakeEnd) {
                    return where + " (the lake ends at offset " + bucketState.lakeEnd
                            + ", but the log now starts at " + bucketState.earliest + ")";
                }
            }
        }
        return null;
    }

    /**
     * The first ceiling a tail of this scan would breach, described for a message, or null when the scan
     * fits under both.
     *
     * <p>Two ceilings, because they answer two different questions. The per-bucket one is what the readers
     * enforce while they replay a tail, and reaching it means tiering has stalled for that bucket. The
     * scan-wide one has no per-reader equivalent: BE holds the keys of every tail it has touched until the
     * scan ends, so a table with many partitions and many buckets can hold far more than any one of them
     * would ever report. Summed over the whole scan rather than per BE because planning does not decide
     * which BE gets which split, and over-counting a scan that fits is the safe direction.
     */
    private String firstTailOverBudget(List<PartitionState> states, List<Integer> buckets) {
        long maxTailRows = catalogProperties.getMaxTailRows();
        long total = 0;
        for (PartitionState state : states) {
            for (int bucket : buckets) {
                BucketState bucketState = state.buckets.get(bucket);
                if (bucketState.lakeEnd == null || bucketState.lakeEnd >= bucketState.stop) {
                    continue;
                }
                // Offsets of a bucket's log are consecutive per record, so this IS the record count, and
                // it is the same yardstick the readers apply while replaying the range.
                long rows = bucketState.stop - bucketState.lakeEnd;
                String where = (state.partition.isPartitioned()
                        ? "partition '" + state.partition.getName() + "', " : "") + "bucket " + bucket;
                if (rows > maxTailRows) {
                    return where + " has a log tail of " + rows + " records, over the "
                            + maxTailRows + " allowed by '"
                            + FlussCatalogProperties.UNION_READ_MAX_TAIL_ROWS + "'";
                }
                total += rows;
            }
        }
        long maxTotal = catalogProperties.getMaxTotalTailRows();
        if (total > maxTotal) {
            return "its log tails hold " + total + " records in total, over the " + maxTotal
                    + " allowed by '" + FlussCatalogProperties.UNION_READ_MAX_TOTAL_TAIL_ROWS + "'";
        }
        return null;
    }

    /** Every bucket read whole from fluss: the plan {@code disabled} produces, and the safe fallback. */
    private static List<ConnectorScanRange> pkRangesFromFlussAlone(List<PartitionState> states,
            List<Integer> buckets) {
        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (PartitionState state : states) {
            for (int bucket : buckets) {
                appendPkFullRange(ranges, state, bucket);
            }
        }
        return ranges;
    }

    /**
     * Gives up this scan's lake half for {@code reason}. Everything asked afterwards — the node
     * properties, the scan-level params, EXPLAIN — then answers as a fluss-only read, because the field
     * the answers come from is the one being cleared here.
     *
     * <p>One question was asked earlier and cannot be taken back: {@link #getMustReadColumns}, at plan
     * translation time, may already have kept the key columns in the scan's tuple. That is harmless in
     * this direction — they are read and then dropped by the projection above the scan — and it is the
     * reason this guard is allowed to run so late. The opposite order would not be harmless, which is why
     * the conditions that CAN be decided before translation (a key column's type) are decided there.
     */
    private void degradeToFlussOnly(String reason) {
        degradedReason = reason;
        unionRead = null;
    }

    /**
     * The lake split, wrapped with the log tail of its own bucket when that tail holds anything.
     *
     * <p>Which bucket a split belongs to is the sibling's answer ({@code paimon.bucket}); which partition
     * it belongs to is compared by the rendered partition values, which is sound because a union read of a
     * partitioned table is refused unless the partition columns are strings. Three things can go wrong,
     * and none of them may be waved through: a split with no bucket at all means the paimon connector does
     * not write the property this contract is built on; a bucket number this table does not have means the
     * two tables are not bucketed alike; a bucket the lake holds files for but fluss records no tiering
     * offset for means their metadata disagrees. Each would silently duplicate rows.
     */
    private ConnectorScanRange bindTailToLakeSplit(FlussTableHandle handle, ConnectorScanRange lakeSplit,
            List<PartitionState> states) {
        PartitionState state = matchingPartition(lakeSplit, states);
        if (state == null) {
            // A partition of the lake that this scan does not read from fluss: either one fluss has since
            // dropped, or one the engine pruned away. Nothing of it can be superseded by a tail this plan
            // does not read, so the split is wrapped plain.
            return FlussLakeRange.plain(lakeSplit);
        }
        int bucket = lakeSplitBucket(handle, lakeSplit);
        BucketState bucketState = state.buckets.get(bucket);
        if (bucketState == null) {
            throw new DorisConnectorException("The lake table of fluss table '" + handle.getDatabaseName()
                    + "." + handle.getTableName() + "' has a split in bucket " + bucket + ", but the fluss"
                    + " table has only " + handle.getBucketCount() + " buckets. The two are not bucketed"
                    + " alike, so their rows cannot be matched by bucket");
        }
        if (bucketState.lakeEnd == null) {
            throw new DorisConnectorException("The lake table of fluss table '" + handle.getDatabaseName()
                    + "." + handle.getTableName() + "' holds data for bucket " + bucket
                    + (state.partition.isPartitioned()
                            ? " of partition '" + state.partition.getName() + "'" : "")
                    + ", but fluss records no tiering offset for that bucket. Their metadata disagrees;"
                    + " reading them as one would return the rows of that bucket twice");
        }
        if (bucketState.lakeEnd >= bucketState.stop) {
            // The lake holds this bucket up to where its log ends: nothing can supersede it.
            return FlussLakeRange.plain(lakeSplit);
        }
        return FlussLakeRange.suppressed(lakeSplit, new FlussLakeRange.Tail(
                state.partition, bucket, bucketState.lakeEnd, bucketState.stop));
    }

    /**
     * The partition this scan planned that {@code lakeSplit} belongs to, or null when it planned none.
     * Matched on the partition values as a whole rather than on a rendered name, so that there is exactly
     * one place in this connector that turns partition values into a name (see {@link FlussPartitions}).
     */
    private static PartitionState matchingPartition(ConnectorScanRange lakeSplit,
            List<PartitionState> states) {
        Map<String, String> values = lakeSplit.getPartitionValues();
        for (PartitionState state : states) {
            if (state.partition.getValues().equals(values)) {
                return state;
            }
        }
        return null;
    }

    /** The bucket a lake split holds, per the contract in {@link #LAKE_BUCKET_PROPERTY}. */
    private static int lakeSplitBucket(FlussTableHandle handle, ConnectorScanRange lakeSplit) {
        String bucket = lakeSplit.getProperties().get(LAKE_BUCKET_PROPERTY);
        if (bucket == null) {
            throw new DorisConnectorException("A lake split of fluss table '" + handle.getDatabaseName()
                    + "." + handle.getTableName() + "' does not say which bucket it holds ('"
                    + LAKE_BUCKET_PROPERTY + "' is missing). Reading a primary-key table together with its"
                    + " lake needs it, to bind each split to the log tail of the same bucket; the paimon"
                    + " connector plugin is older than this fluss connector plugin. Split: " + lakeSplit);
        }
        try {
            return Integer.parseInt(bucket);
        } catch (NumberFormatException e) {
            throw new DorisConnectorException("A lake split of fluss table '" + handle.getDatabaseName()
                    + "." + handle.getTableName() + "' reports bucket '" + bucket + "', which is not a"
                    + " number", e);
        }
    }

    /**
     * The lake half, planned by the sibling connector on the handle already pinned to the readable snapshot.
     *
     * <p>Only the filter is carried over. The row limit is deliberately dropped — applying it to one half of
     * a union would silently drop rows from the other — and so is the {@code COUNT(*)} signal, because a
     * per-range row count is not this table's count once the log half is added. The engine's pruned
     * partition list is not passed either: the sibling does not consume it (it re-plans from the filter),
     * and pretending otherwise would hide that the two halves prune by different means.
     */
    private List<ConnectorScanRange> planLakeRanges(ConnectorSession session, UnionRead union,
            ConnectorScanRequest request) {
        List<ConnectorColumnHandle> lakeColumns =
                lakeColumns(session, union, request.getColumns(), request.getTableHandle());
        ConnectorScanRequest lakeRequest =
                ConnectorScanRequest.builder(union.pinnedLakeHandle, lakeColumns)
                        .filter(request.getFilter())
                        .build();
        return LakeSibling.call(union.sibling,
                () -> union.siblingProvider.planScan(session, lakeRequest));
    }

    /**
     * The sibling's own column handles for the columns this scan reads.
     *
     * <p>Needed because the sibling projects by ITS handle type and silently ignores anything else: handing
     * it fluss's handles would leave it with no projection at all. The lake table's columns are this table's
     * columns plus the three fluss system columns appended at the end ({@code __bucket} / {@code __offset} /
     * {@code __timestamp}), so every column asked for here exists there under the same name and the extra
     * three are simply never asked for. A column that is missing is a real mismatch between the two schemas
     * — the lake table was not created by this fluss table's tiering — and fails loud rather than reading a
     * silently narrower row.
     */
    private List<ConnectorColumnHandle> lakeColumns(ConnectorSession session, UnionRead union,
            List<ConnectorColumnHandle> columns, ConnectorTableHandle handle) {
        Map<String, ConnectorColumnHandle> lakeHandles = LakeSibling.forward(session, union.sibling,
                metadata -> metadata.getColumnHandles(session, union.pinnedLakeHandle));
        List<ConnectorColumnHandle> mapped = new ArrayList<>(columns.size());
        for (ConnectorColumnHandle column : columns) {
            String name = ((FlussColumnHandle) column).getName();
            ConnectorColumnHandle lakeHandle = lakeHandles.get(name);
            if (lakeHandle == null) {
                FlussTableHandle flussHandle = (FlussTableHandle) handle;
                throw new DorisConnectorException("Column '" + name + "' of fluss table '"
                        + flussHandle.getDatabaseName() + "." + flussHandle.getTableName()
                        + "' does not exist in its lake table, so the two cannot be read as one");
            }
            mapped.add(lakeHandle);
        }
        return mapped;
    }

    /**
     * This scan node's lake half, or null when the table is read from fluss alone. Resolved at most once per
     * scan node because two entry points need it — {@link #planScan} and {@link #getScanNodeProperties} —
     * and the engine may call either first.
     *
     * <p>Resolving it once is also what keeps the two halves from overlapping. The lake snapshot is read
     * HERE, before {@link #planScan} asks fluss where each bucket's log currently ends; log offsets only
     * move forward, so the snapshot can only be at or behind those stopping offsets. Read the other way
     * round, a snapshot committed in between would cover rows past the point the log half stops at, and the
     * bucket's log range would start after it ends.
     */
    private UnionRead resolveUnionRead(ConnectorSession session, FlussTableHandle handle) {
        if (unionResolved) {
            return unionRead;
        }
        unionResolved = true;
        unionRead = resolveUnionReadUncached(session, handle);
        return unionRead;
    }

    private UnionRead resolveUnionReadUncached(ConnectorSession session, FlussTableHandle handle) {
        // A $log scan asks a different question of the same resolution, so it reads the mode differently.
        // The union-read mode chooses a PATH for reading a whole table, and $log is not a whole table: it
        // is defined AS "the part past the lake snapshot", so there is no path here to choose and the mode
        // does not apply. Where a whole-table read may quietly settle for reading fluss alone, $log has to
        // fail instead — the fluss-only read returns the whole table, which is a different row set under
        // the same name.
        if (handle.isLogOnly()) {
            return resolveLakeBoundary(handle);
        }
        FlussCatalogProperties.UnionReadMode mode = catalogProperties.getUnionReadMode();
        if (!handle.isDataLakeEnabled() || mode == FlussCatalogProperties.UnionReadMode.DISABLED) {
            // Not a lake table, or the user asked for the fluss-only read explicitly.
            return null;
        }
        if (handle.hasPrimaryKey()) {
            // Whether the two halves can be matched by key at all is decided HERE, before the lake snapshot
            // is asked for, because it depends only on the table's schema — and because this is the one
            // decision that must be the same at plan-translation time (when getMustReadColumns keeps the key
            // columns in the scan's tuple) as it is later, when the ranges are planned. Everything that can
            // only be known from the offsets is decided in planScan, where giving up is still safe.
            String rejection = keyColumnRejection(handle);
            String reason = DEGRADED_KEY_TYPE;
            if (rejection == null) {
                rejection = partitionColumnRejection(handle);
                reason = DEGRADED_PARTITION_TYPE;
            }
            if (rejection != null) {
                if (mode == FlussCatalogProperties.UnionReadMode.REQUIRED) {
                    throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                            + handle.getTableName() + "' cannot be read as its lake plus its change log: "
                            + rejection + ". Reading it from fluss alone still returns every row, so set '"
                            + FlussCatalogProperties.UNION_READ_MODE + "' to auto or disabled.");
                }
                degradedReason = reason;
                return null;
            }
        }
        LakeSnapshot snapshot;
        try {
            snapshot = adminOps.getReadableLakeSnapshot(handle.toTablePath());
        } catch (LakeTableSnapshotNotExistException e) {
            if (mode == FlussCatalogProperties.UnionReadMode.REQUIRED) {
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' has no readable lake snapshot yet, and '"
                        + FlussCatalogProperties.UNION_READ_MODE + "=required' forbids falling back to "
                        + "a fluss-only read. Wait for the tiering service to commit, or set the property "
                        + "to auto or disabled.", e);
            }
            // Nothing is in the lake, so the log holds everything: the fluss-only read is the whole table.
            return null;
        }
        String lakeFormat = handle.getDataLakeFormat();
        if (lakeFormat == null || !PAIMON_LAKE_FORMAT.equalsIgnoreCase(lakeFormat)) {
            throw new DorisConnectorException("Cannot read table '" + handle.getDatabaseName() + "."
                    + handle.getTableName() + "': its table.datalake.format is '" + lakeFormat
                    + "', and the fluss connector currently supports only '" + PAIMON_LAKE_FORMAT + "'");
        }

        Connector sibling = lakeSiblingFactory.apply(
                PaimonSiblingProperties.synthesize(handle.getProperties()));
        ConnectorTableHandle lakeHandle = LakeSibling.forward(session, sibling,
                metadata -> metadata.getTableHandle(
                        session, handle.getDatabaseName(), handle.getTableName()))
                .orElseThrow(() -> new DorisConnectorException("Fluss reports a readable lake snapshot for '"
                        + handle.getDatabaseName() + "." + handle.getTableName() + "' but its lake table"
                        + " does not exist. The lake warehouse and the fluss cluster disagree; check the"
                        + " table's table.datalake.* settings"));
        // The lake half's ranges are planned by the sibling and mixed into this node's range list, where
        // they are told apart from fluss's by which connector owns them. Checked at birth; see requireOwned.
        LakeSibling.requireOwned(sibling, lakeHandle);
        // The pin is expressed in the SPI's own terms — a snapshot id and no connector options — so the
        // sibling translates it into whatever its SDK calls a snapshot. Nothing paimon-specific is named
        // here. The id needs no mapping either: what fluss records as the lake snapshot IS the id the lake
        // returned when tiering committed it.
        ConnectorMvccSnapshot pin = ConnectorMvccSnapshot.builder()
                .snapshotId(snapshot.getSnapshotId())
                .build();
        ConnectorTableHandle pinnedHandle = LakeSibling.forward(session, sibling,
                metadata -> metadata.applySnapshot(session, lakeHandle, pin));
        ConnectorScanPlanProvider siblingProvider = LakeSibling.call(sibling,
                () -> sibling.getScanPlanProvider(pinnedHandle));
        return new UnionRead(sibling, siblingProvider, pinnedHandle, snapshot.getSnapshotId(),
                snapshot.getTableBucketsOffset());
    }

    /**
     * Where the lake ends, for a {@code $log} scan — which is the whole of what such a scan needs from the
     * lake, and it needs it absolutely.
     *
     * <p>Both failures below are the same failure seen twice: {@code $log} names the log PAST the lake, so
     * without a lake there is no such segment. Answering with the whole log instead would be the one
     * mistake that cannot be noticed downstream — the plan looks exactly like a correct one, and the query
     * returns every row of the table under a name that promised a part of them.
     */
    private UnionRead resolveLakeBoundary(FlussTableHandle handle) {
        if (!handle.isDataLakeEnabled()) {
            // Reachable: the lake can be turned off between resolving the name and planning the scan.
            throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                    + handle.getTableName() + "' is no longer tiered into a lake (table.datalake.enabled),"
                    + " so it has no '$log' part. Query '" + handle.getTableName() + "' itself.");
        }
        LakeSnapshot snapshot;
        try {
            snapshot = adminOps.getReadableLakeSnapshot(handle.toTablePath());
        } catch (LakeTableSnapshotNotExistException e) {
            throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                    + handle.getTableName() + "' has no readable lake snapshot yet, so '$log' has no point"
                    + " to start from: nothing has been tiered, and the whole table is still in the log."
                    + " Query '" + handle.getTableName() + "' itself, or wait for the tiering service to"
                    + " commit.", e);
        }
        return UnionRead.boundaryOnly(snapshot.getSnapshotId(), snapshot.getTableBucketsOffset());
    }

    /**
     * Why this table's rows cannot be matched across the two halves by their keys, or null when they can.
     * The key is the primary key minus the partition columns, because a bucket lives inside one partition
     * and the partition columns are equal for every row in it.
     */
    private static String keyColumnRejection(FlussTableHandle handle) {
        for (String column : handle.getPhysicalPrimaryKeys()) {
            DataType type = handle.getKeyColumnTypes().get(column);
            if (type == null) {
                // The handle names a key column the schema it was built from does not have.
                return "its primary key names column '" + column + "', which the table does not have";
            }
            String rejection = FlussUnionKeyTypes.keyColumnRejection(type);
            if (rejection != null) {
                return "primary-key column '" + column + "' has type " + type + ", and " + rejection;
            }
        }
        return null;
    }

    /** The same question for the partition columns, which decide which bucket a lake split belongs to. */
    private static String partitionColumnRejection(FlussTableHandle handle) {
        for (String column : handle.getPartitionKeys()) {
            DataType type = handle.getKeyColumnTypes().get(column);
            if (type == null) {
                return "it is partitioned by column '" + column + "', which the table does not have";
            }
            String rejection = FlussUnionKeyTypes.partitionColumnRejection(type);
            if (rejection != null) {
                return "partition column '" + column + "' has type " + type + ", and " + rejection;
            }
        }
        return null;
    }

    /**
     * The key columns BE has to read whether or not the query asked for them, so that it can tell which
     * lake rows the log tail supersedes. Empty for every other read, which is every read but this one.
     *
     * <p>Asked by the engine while it is translating the plan, well before {@link #planScan}, and answered
     * from the SAME memoized resolution — that is a correctness requirement, not a saving. If the two
     * answers could differ, a scan whose tuple was pruned as a fluss-only read could still be planned as a
     * union read, and BE would look for a key column that is not in its projection.
     *
     * <p>Only a primary-key table is asked further, so that a log table's scan does not pull the lake
     * snapshot's round trip forward into plan translation for nothing.
     */
    @Override
    public Set<String> getMustReadColumns(ConnectorSession session, ConnectorTableHandle handle) {
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        if (!flussHandle.hasPrimaryKey() || resolveUnionRead(session, flussHandle) == null) {
            return Collections.emptySet();
        }
        return new LinkedHashSet<>(flussHandle.getPhysicalPrimaryKeys());
    }

    /**
     * The ranges covering one partition of a table, or the whole of an unpartitioned one
     * ({@code flussPartitionName} is null, which is also how the two admin overloads are told apart).
     */
    private void appendPartitionRanges(List<ConnectorScanRange> ranges, FlussTableHandle handle,
            UnionRead union, FlussScanRange.Partition partition, List<Integer> buckets,
            String flussPartitionName) {
        TablePath tablePath = handle.toTablePath();
        if (!handle.hasPrimaryKey()) {
            Map<Integer, Long> stopping = latestOffsets(tablePath, flussPartitionName, buckets);
            if (union == null) {
                appendLogRanges(ranges, partition, buckets, stopping);
            } else {
                appendUnionLogRanges(ranges, handle, union, partition, buckets, stopping);
            }
            return;
        }
        // Snapshots BEFORE offsets, and the order is load-bearing. A snapshot committed between the two
        // calls ends past the offset planning stopped at; that bucket would then be read from a snapshot
        // already containing rows written after the query started, while every other bucket stopped
        // where planning saw it. Log offsets only move forward, so asking in this order keeps every
        // snapshot at or behind the stopping offset.
        KvSnapshots snapshots = latestKvSnapshots(tablePath, flussPartitionName);
        appendPkRanges(ranges, partition, buckets, snapshots,
                latestOffsets(tablePath, flussPartitionName, buckets));
    }

    private KvSnapshots latestKvSnapshots(TablePath tablePath, String flussPartitionName) {
        return flussPartitionName == null
                ? adminOps.getLatestKvSnapshots(tablePath)
                : adminOps.getLatestKvSnapshots(tablePath, flussPartitionName);
    }

    private Map<Integer, Long> latestOffsets(TablePath tablePath, String flussPartitionName,
            List<Integer> buckets) {
        return flussPartitionName == null
                ? adminOps.listOffsets(tablePath, buckets, new OffsetSpec.LatestSpec())
                : adminOps.listOffsets(tablePath, flussPartitionName, buckets, new OffsetSpec.LatestSpec());
    }

    /** The earliest offset each bucket's log still holds — what a tail has to still begin at or after. */
    private Map<Integer, Long> earliestOffsets(TablePath tablePath, String flussPartitionName,
            List<Integer> buckets) {
        return flussPartitionName == null
                ? adminOps.listOffsets(tablePath, buckets, new OffsetSpec.EarliestSpec())
                : adminOps.listOffsets(tablePath, flussPartitionName, buckets,
                        new OffsetSpec.EarliestSpec());
    }

    /**
     * The partitions to scan: those the engine's pruning left, or all of them when it pruned nothing.
     * A pruned name that fluss no longer lists is simply absent from the result — the partition was
     * dropped between pruning and planning, and there is nothing left to read.
     */
    private List<PartitionInfo> selectedPartitions(FlussTableHandle handle, List<String> requiredPartitions) {
        List<PartitionInfo> partitions = adminOps.listPartitionInfos(handle.toTablePath());
        if (requiredPartitions.isEmpty()) {
            return partitions;
        }
        Set<String> required = new HashSet<>(requiredPartitions);
        List<PartitionInfo> selected = new ArrayList<>(partitions.size());
        for (PartitionInfo partition : partitions) {
            // Matched on the DORIS name, which is what the engine pruned over: FlussPartitions renders
            // both this and the metadata listing the engine pruned, so the two cannot disagree.
            if (required.contains(
                    FlussPartitions.toScanPartition(partition, handle.getPartitionKeys()).getName())) {
                selected.add(partition);
            }
        }
        return selected;
    }

    private static void appendLogRanges(List<ConnectorScanRange> ranges,
            FlussScanRange.Partition partition, List<Integer> buckets, Map<Integer, Long> stopping) {
        for (int bucket : buckets) {
            Long stop = stopping.get(bucket);
            if (stop == null || stop <= 0) {
                // Never written to: no range at all rather than an empty one for BE to open and close.
                // A bucket whose records have all aged out of the log is NOT this case — its latest
                // offset stayed where it was — and still gets a range that reads nothing, which costs
                // one scanner rather than an extra round trip for every bucket to find out.
                continue;
            }
            ranges.add(FlussScanRange.log(partition, bucket, LogScanner.EARLIEST_OFFSET, stop));
        }
    }

    /**
     * The log half of a union read: each bucket from where the lake snapshot left off, up to where planning
     * saw the log. The snapshot's offset is exclusive — it is the first offset NOT in the lake — so the two
     * halves meet exactly, with no row read twice and none skipped.
     *
     * <p>A bucket the snapshot does not mention has never been tiered, so its log is read from the earliest
     * offset fluss still holds; a bucket whose snapshot offset has caught up with the stopping offset has
     * nothing left outside the lake and yields no range at all.
     */
    private static void appendUnionLogRanges(List<ConnectorScanRange> ranges, FlussTableHandle handle,
            UnionRead union, FlussScanRange.Partition partition, List<Integer> buckets,
            Map<Integer, Long> stopping) {
        for (int bucket : buckets) {
            Long stop = stopping.get(bucket);
            if (stop == null || stop <= 0) {
                continue;
            }
            TableBucket tableBucket = partition.isPartitioned()
                    ? new TableBucket(handle.getTableId(), partition.getId(), bucket)
                    : new TableBucket(handle.getTableId(), bucket);
            Long lakeEnd = union.logOffsets.get(tableBucket);
            if (lakeEnd == null) {
                ranges.add(FlussScanRange.log(partition, bucket, LogScanner.EARLIEST_OFFSET, stop));
            } else if (lakeEnd < stop) {
                ranges.add(FlussScanRange.log(partition, bucket, lakeEnd, stop));
            }
        }
    }

    /**
     * One range per bucket that holds anything: its latest kv snapshot, plus the change log from where
     * that snapshot ended up to where planning saw the log. A bucket fluss has never snapshotted gets
     * {@code -1} and the earliest sentinel, and its state is rebuilt by replaying the whole change log
     * — correct, because a primary-key table's log carries every change, just slower.
     */
    private static void appendPkRanges(List<ConnectorScanRange> ranges,
            FlussScanRange.Partition partition, List<Integer> buckets, KvSnapshots snapshots,
            Map<Integer, Long> stopping) {
        for (int bucket : buckets) {
            Long stop = stopping.get(bucket);
            appendPkFullRange(ranges, partition, bucket, snapshots, stop == null ? 0L : stop);
        }
    }

    /** The same, for a bucket of a table whose lake half is being read too. */
    private static void appendPkFullRange(List<ConnectorScanRange> ranges, PartitionState state,
            int bucket) {
        appendPkFullRange(ranges, state.partition, bucket, state.snapshots, state.buckets.get(bucket).stop);
    }

    private static void appendPkFullRange(List<ConnectorScanRange> ranges,
            FlussScanRange.Partition partition, int bucket, KvSnapshots snapshots, long logStop) {
        long snapshotId = snapshots.getSnapshotId(bucket).orElse(FlussScanRange.NO_KV_SNAPSHOT);
        long logStart = snapshots.getLogOffset(bucket).orElse(LogScanner.EARLIEST_OFFSET);
        if (snapshotId == FlussScanRange.NO_KV_SNAPSHOT && logStop <= 0) {
            // Nothing snapshotted and nothing logged: the bucket is empty. A bucket WITH a snapshot
            // is planned even when its log has caught up, because the snapshot still holds rows.
            return;
        }
        ranges.add(FlussScanRange.pkFull(partition, bucket, snapshotId, logStart, logStop));
    }

    /**
     * How many of {@code ranges} are fluss ranges of this kind. The type test is not defensive: on a union
     * read the list also holds the lake half's ranges, which are the sibling's own type and would fail a
     * cast.
     */
    private static int count(List<ConnectorScanRange> ranges, FlussScanRange.RangeType rangeType) {
        int found = 0;
        for (ConnectorScanRange range : ranges) {
            if (range instanceof FlussScanRange && ((FlussScanRange) range).getRangeType() == rangeType) {
                found++;
            }
        }
        return found;
    }

    /** Ranges the sibling planned; every one of them is wrapped, suppressed or plain. */
    private static int countLakeSplits(List<ConnectorScanRange> ranges) {
        int found = 0;
        for (ConnectorScanRange range : ranges) {
            if (range instanceof FlussLakeRange) {
                found++;
            }
        }
        return found;
    }

    private static int countSuppressedLakeSplits(List<ConnectorScanRange> ranges) {
        int found = 0;
        for (ConnectorScanRange range : ranges) {
            if (range instanceof FlussLakeRange && ((FlussLakeRange) range).getTail() != null) {
                found++;
            }
        }
        return found;
    }

    private static List<Integer> allBuckets(int bucketCount) {
        List<Integer> buckets = new ArrayList<>(bucketCount);
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            buckets.add(bucket);
        }
        return buckets;
    }

    /**
     * What every range of this scan shares. The {@code fluss.}-prefixed entries are the BE half and are
     * forwarded by {@link #populateScanLevelParams}; {@code path_partition_keys} is the engine's, and
     * declaring it is what keeps the partition columns out of the scanner's projection so BE
     * materializes them from each range instead (see {@link FlussScanRange}).
     */
    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
        FlussTableHandle flussHandle = (FlussTableHandle) handle;
        Map<String, String> props = new LinkedHashMap<>();
        if (flussHandle.isPartitioned()) {
            props.put(ScanNodePropertyKeys.PATH_PARTITION_KEYS,
                    String.join(",", flussHandle.getPartitionKeys()));
        }
        props.put(PROP_DB_NAME, flussHandle.getDatabaseName());
        props.put(PROP_TABLE_NAME, flussHandle.getTableName());
        catalogProperties.getFlussClientConfig()
                .forEach((key, value) -> props.put(PROP_CLIENT_PREFIX + key, value));

        UnionRead union = resolveUnionRead(session, flussHandle);
        // A $log scan resolved the lake only to find where the log starts; it plans no lake range, so
        // sending the lake's scan-node properties to BE would configure a reader for ranges that are
        // not there.
        if (union != null && union.hasLakeHalf()) {
            if (flussHandle.hasPrimaryKey()) {
                // What BE needs to suppress lake rows by key: which columns the key is made of, and how
                // large a tail it may hold in memory while doing so. Both are node-level because both are
                // the same for every range of the scan.
                props.put(PROP_UNION_PK_NAMES, String.join(",", flussHandle.getPhysicalPrimaryKeys()));
                props.put(PROP_UNION_MAX_TAIL_ROWS,
                        String.valueOf(catalogProperties.getMaxTailRows()));
            }
            List<ConnectorColumnHandle> lakeColumns = lakeColumns(session, union, columns, handle);
            mergeLakeProperties(props, LakeSibling.call(union.sibling,
                    () -> union.siblingProvider.getScanNodeProperties(
                            session, union.pinnedLakeHandle, lakeColumns, filter)));
        }
        return props;
    }

    /**
     * Folds the lake half's node properties into this scan node's, which is what makes one node able to
     * serve both kinds of range: the engine calls {@code populateScanLevelParams} once, with one map, and
     * the sibling reads its own entries back out of it.
     *
     * <p>The two connectors share three keys, and all three must already agree, so a difference is a real
     * mismatch and is raised rather than resolved by picking a side. {@code path_partition_keys} is the one
     * that matters: the split between file columns and partition columns is decided ONCE for the node, so
     * two halves that disagree about which columns come from the range would read different columns from
     * the same tuple. They cannot legitimately disagree — a fluss table's lake table is created with its
     * partition keys — which is exactly why a disagreement means something is wrong upstream.
     */
    private static void mergeLakeProperties(Map<String, String> props, Map<String, String> lakeProps) {
        for (Map.Entry<String, String> entry : lakeProps.entrySet()) {
            String existing = props.get(entry.getKey());
            if (existing == null) {
                props.put(entry.getKey(), entry.getValue());
            } else if (!existing.equals(entry.getValue())) {
                throw new DorisConnectorException("The fluss table and its lake table disagree about scan"
                        + " property '" + entry.getKey() + "' ('" + existing + "' vs '" + entry.getValue()
                        + "'), so they cannot be read as one");
            }
        }
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        Map<String, String> beProperties = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : nodeProperties.entrySet()) {
            // Prefix-gated rather than copied wholesale: the map also carries the engine's own keys, the
            // synthetic ones it injects for EXPLAIN and (on a union read) the lake half's, none of which
            // mean anything to the scanner.
            if (entry.getKey().startsWith(BE_PROPERTY_PREFIX)) {
                beProperties.put(entry.getKey(), entry.getValue());
            }
        }
        params.setFlussProperties(beProperties);

        // The lake half's turn at the same params. Its ranges are useless without it — the paimon reader
        // fails outright on a missing serialized table — and only the sibling knows which of the merged
        // entries are its own.
        UnionRead union = unionRead;
        if (union != null) {
            LakeSibling.call(union.sibling, () -> {
                union.siblingProvider.populateScanLevelParams(params, nodeProperties);
                return null;
            });
        }

        // Last, so the sibling's turn at the same params cannot take the marker away. The node is stamped
        // for being planned HERE rather than for the ranges it ended up with: a plan whose buckets are all
        // tiered holds only lake splits, but which scanner reads them is decided before any range is
        // fetched, and a rule that depended on what planScan produced would go quiet exactly when the
        // planning order changed.
        TTableFormatFileDesc scanLevelFormat = new TTableFormatFileDesc();
        scanLevelFormat.setTableFormatType(SCAN_LEVEL_TABLE_FORMAT);
        params.setTableFormatParams(scanLevelFormat);
    }

    /**
     * Forwarded so the VERBOSE EXPLAIN counts the lake half's merge-on-read delete files. A fluss range
     * carries none, and the sibling reads them off its own per-range descriptor, so asking it about every
     * range is both harmless and the only way to ask without knowing which range is whose.
     */
    @Override
    public List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) {
        UnionRead union = unionRead;
        if (union == null || !union.hasLakeHalf()) {
            return Collections.emptyList();
        }
        return LakeSibling.call(union.sibling,
                () -> union.siblingProvider.getDeleteFiles(tableFormatParams));
    }

    /**
     * The line a regression test reads to tell which way a scan was actually planned. {@code auto}
     * silently falls back to a fluss-only read, so "did this query read the lake?" is otherwise
     * invisible in the plan and a union-read test would pass without having tested anything. Log and
     * primary-key ranges are counted apart for the same reason: they are read by different code, and a
     * single total cannot tell a primary-key table planned the right way from one planned the wrong way.
     *
     * <p>{@code readMode} says WHICH PART of the table was asked for and {@code mode} says which PATH was
     * taken to read it — two different questions that a single field would blur. They also fail
     * differently, and only together are the failures visible: a {@code $log} scan reports
     * {@code readMode=log} with {@code unionRead=yes} and {@code lakeSplits=0}, so the one silent way it
     * could go wrong — reading the whole log instead of the part past the lake — shows up here as
     * {@code unionRead=no}, since the lake snapshot is exactly what supplies its starting offsets.
     */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix)
                .append("flussScan: readMode=").append(plannedReadMode)
                .append(", unionRead=").append(plannedUnionRead ? "yes" : "no")
                .append(", lakeSplits=").append(plannedLakeSplits)
                .append(", suppressedLakeSplits=").append(plannedSuppressedLakeSplits)
                .append(", logRanges=").append(plannedLogRanges)
                .append(", pkRanges=").append(plannedPkRanges)
                .append(", pkTailRanges=").append(plannedPkTailRanges)
                .append(", mode=")
                .append(catalogProperties.getUnionReadMode().propertyValue());
        if (degradedReason != null) {
            // Only reachable under 'auto'. Without it, a table whose lake this query could not use looks
            // exactly like a table that has no lake at all.
            output.append(", degraded=").append(degradedReason);
        }
        output.append("\n");
    }
}
