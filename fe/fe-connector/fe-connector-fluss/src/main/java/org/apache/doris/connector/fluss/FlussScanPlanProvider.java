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

import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.scan.ScanNodePropertyKeys;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 * into the same scan node (BE builds a reader per range, so the two kinds coexist). That keeps this plugin
 * free of any paimon dependency and gives the lake half paimon's native readers, deletion vectors and file
 * cache for free. Refusing to serve such a table at all is the alternative to avoid: a datalake table read
 * as fluss-only silently returns just the rows tiering has not moved yet, which looks like a working
 * query. {@code fluss.union_read.mode=disabled} is how a user asks for that fluss-only read on purpose.
 *
 * <p>What is NOT here yet: the union of a PRIMARY-KEY table's lake with its log. That one cannot be a
 * plain concatenation — the two halves have to be merged by key — so it is refused loudly.
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

    /** The only lake format that can be delegated today; fluss also defines iceberg / lance / hudi. */
    private static final String PAIMON_LAKE_FORMAT = "paimon";

    private final FlussAdminOps adminOps;
    private final Map<String, String> catalogProperties;
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
    private int plannedLakeSplits;
    private boolean plannedUnionRead;

    /**
     * This scan node's lake half, resolved at most once (see {@link #resolveUnionRead}). Same threading
     * argument as the counters above; {@code unionResolved} distinguishes "not asked yet" from "asked, and
     * this table has no lake half".
     */
    private boolean unionResolved;
    private UnionRead unionRead;

    public FlussScanPlanProvider(FlussAdminOps adminOps, Map<String, String> catalogProperties,
            Function<Map<String, String>, Connector> lakeSiblingFactory) {
        this.adminOps = adminOps;
        this.catalogProperties = catalogProperties;
        this.lakeSiblingFactory = lakeSiblingFactory;
    }

    /**
     * The lake half of a union read: the sibling connector that owns it, its scan planner, its table handle
     * already pinned to {@link #snapshotId}, and where that snapshot left each bucket's log.
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
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        FlussTableHandle handle = (FlussTableHandle) request.getTableHandle();
        UnionRead union = resolveUnionRead(session, handle);

        List<Integer> buckets = allBuckets(handle.getBucketCount());
        List<ConnectorScanRange> ranges = new ArrayList<>();

        // The lake half first, so its ranges lead the list the way they lead the table's history. It is
        // planned once for the whole table: the sibling prunes partitions from the pushed-down filter, not
        // from the engine's pruned partition list (which it does not consume).
        if (union != null) {
            ranges.addAll(planLakeRanges(session, union, request));
        }
        plannedLakeSplits = ranges.size();

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

        plannedLogRanges = count(ranges, FlussScanRange.RangeType.LOG);
        plannedPkRanges = count(ranges, FlussScanRange.RangeType.PK_FULL);
        plannedUnionRead = union != null;
        return ranges;
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
        FlussConnectorProperties.UnionReadMode mode =
                FlussConnectorProperties.unionReadMode(catalogProperties);
        if (!handle.isDataLakeEnabled() || mode == FlussConnectorProperties.UnionReadMode.DISABLED) {
            // Not a lake table, or the user asked for the fluss-only read explicitly.
            return null;
        }
        LakeSnapshot snapshot;
        try {
            snapshot = adminOps.getReadableLakeSnapshot(handle.toTablePath());
        } catch (LakeTableSnapshotNotExistException e) {
            if (mode == FlussConnectorProperties.UnionReadMode.REQUIRED) {
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' has no readable lake snapshot yet, and '"
                        + FlussConnectorProperties.UNION_READ_MODE + "=required' forbids falling back to "
                        + "a fluss-only read. Wait for the tiering service to commit, or set the property "
                        + "to auto or disabled.", e);
            }
            // Nothing is in the lake, so the log holds everything: the fluss-only read is the whole table.
            return null;
        }
        if (handle.hasPrimaryKey()) {
            throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                    + handle.getTableName() + "' is a primary-key table tiered into a lake. Reading it"
                    + " requires merging the lake with the change log by key, which is not supported yet."
                    + " Set '" + FlussConnectorProperties.UNION_READ_MODE + "=disabled' to read only what"
                    + " the fluss log still holds, which is NOT the whole table.");
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
            long snapshotId = snapshots.getSnapshotId(bucket).orElse(FlussScanRange.NO_KV_SNAPSHOT);
            long logStart = snapshots.getLogOffset(bucket).orElse(LogScanner.EARLIEST_OFFSET);
            Long stop = stopping.get(bucket);
            long logStop = stop == null ? 0L : stop;
            if (snapshotId == FlussScanRange.NO_KV_SNAPSHOT && logStop <= 0) {
                // Nothing snapshotted and nothing logged: the bucket is empty. A bucket WITH a snapshot
                // is planned even when its log has caught up, because the snapshot still holds rows.
                continue;
            }
            ranges.add(FlussScanRange.pkFull(partition, bucket, snapshotId, logStart, logStop));
        }
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
        FlussConnectorProperties.toFlussClientConfig(catalogProperties)
                .forEach((key, value) -> props.put(PROP_CLIENT_PREFIX + key, value));

        UnionRead union = resolveUnionRead(session, flussHandle);
        if (union != null) {
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
    }

    /**
     * Forwarded so the VERBOSE EXPLAIN counts the lake half's merge-on-read delete files. A fluss range
     * carries none, and the sibling reads them off its own per-range descriptor, so asking it about every
     * range is both harmless and the only way to ask without knowing which range is whose.
     */
    @Override
    public List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) {
        UnionRead union = unionRead;
        if (union == null) {
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
     */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix)
                .append("flussScan: unionRead=").append(plannedUnionRead ? "yes" : "no")
                .append(", lakeSplits=").append(plannedLakeSplits)
                .append(", logRanges=").append(plannedLogRanges)
                .append(", pkRanges=").append(plannedPkRanges)
                .append(", mode=")
                .append(FlussConnectorProperties.unionReadMode(catalogProperties).propertyValue())
                .append("\n");
    }
}
