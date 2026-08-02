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

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.connector.api.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;

import org.apache.fluss.client.admin.OffsetSpec;
import org.apache.fluss.client.table.scanner.log.LogScanner;
import org.apache.fluss.exception.LakeTableSnapshotNotExistException;
import org.apache.fluss.metadata.PartitionInfo;
import org.apache.fluss.metadata.TablePath;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Turns a fluss table into the scan ranges that cover it.
 *
 * <p>A log table is read one bucket at a time, from the earliest offset fluss still holds up to the
 * offset the log had reached when planning ran. That stopping offset is taken once, for all of a
 * partition's buckets, so every bucket of a partition stops at the same view of the table; without it
 * a bucket planned later would read rows written after the query started while an earlier bucket did
 * not. A bucket that has never been written to (stopping offset 0) yields no range at all.
 *
 * <p>Planning only reads metadata — partition lists and offsets — so it is safe to run for an
 * {@code EXPLAIN}, which does reach {@code planScan}. (There is no explain-only signal on the SPI in
 * this branch; the point is that fluss does not need one. A future change that takes a snapshot lease
 * during planning would.)
 *
 * <p>What is NOT here yet: primary-key tables, and the union of a table's lake with its log. Both are
 * refused loudly rather than served as a partial answer — a datalake table read as fluss-only would
 * silently return just the rows that have not been tiered away, which looks like a working query.
 * {@code fluss.union_read.mode=disabled} is how a user asks for the fluss-only read on purpose.
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

    private final FlussAdminOps adminOps;
    private final Map<String, String> catalogProperties;

    /**
     * What the last {@link #planScan} produced, for the EXPLAIN line. Plain fields, not volatile: the
     * engine memoizes one provider instance per scan node and plans that node on the FE planning
     * thread, then renders EXPLAIN from the same thread afterwards. This connector declares neither
     * batch scan nor streaming splits, which are what would move planning off that thread — enabling
     * either means revisiting this (the ES provider carries the same caveat).
     */
    private int plannedLogRanges;
    private boolean plannedUnionRead;

    public FlussScanPlanProvider(FlussAdminOps adminOps, Map<String, String> catalogProperties) {
        this.adminOps = adminOps;
        this.catalogProperties = catalogProperties;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        FlussTableHandle handle = (FlussTableHandle) request.getTableHandle();
        FlussConnectorProperties.UnionReadMode mode =
                FlussConnectorProperties.unionReadMode(catalogProperties);
        rejectWhatIsNotImplemented(handle, mode);

        TablePath tablePath = handle.toTablePath();
        List<Integer> buckets = allBuckets(handle.getBucketCount());
        List<ConnectorScanRange> ranges = new ArrayList<>();

        if (handle.isPartitioned()) {
            for (PartitionInfo partition : selectedPartitions(handle, request.getRequiredPartitions())) {
                // fluss's own partition name ("20260101$cn"), not the Doris one: this is a fluss API.
                Map<Integer, Long> stopping = adminOps.listOffsets(
                        tablePath, partition.getPartitionName(), buckets, new OffsetSpec.LatestSpec());
                appendLogRanges(ranges,
                        FlussPartitions.toScanPartition(partition, handle.getPartitionKeys()),
                        buckets, stopping);
            }
        } else {
            Map<Integer, Long> stopping =
                    adminOps.listOffsets(tablePath, buckets, new OffsetSpec.LatestSpec());
            appendLogRanges(ranges, FlussScanRange.Partition.NONE, buckets, stopping);
        }

        plannedLogRanges = ranges.size();
        plannedUnionRead = false;
        return ranges;
    }

    /**
     * Refuses the reads this connector cannot serve yet, naming what would have to change. Serving them
     * partially is the failure mode to avoid: a datalake table planned as fluss-only returns whatever
     * the log still holds and drops everything tiering has already moved into the lake, which is a
     * successful query with missing rows.
     */
    private void rejectWhatIsNotImplemented(FlussTableHandle handle,
            FlussConnectorProperties.UnionReadMode mode) {
        if (handle.hasPrimaryKey()) {
            throw new DorisConnectorException("Reading the fluss primary-key table '"
                    + handle.getDatabaseName() + "." + handle.getTableName()
                    + "' is not supported yet; only log tables can be read.");
        }
        if (!handle.isDataLakeEnabled() || mode == FlussConnectorProperties.UnionReadMode.DISABLED) {
            // Not a lake table, or the user asked for the fluss-only read explicitly.
            return;
        }
        try {
            adminOps.getReadableLakeSnapshot(handle.toTablePath());
        } catch (LakeTableSnapshotNotExistException e) {
            if (mode == FlussConnectorProperties.UnionReadMode.REQUIRED) {
                throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                        + handle.getTableName() + "' has no readable lake snapshot yet, and '"
                        + FlussConnectorProperties.UNION_READ_MODE + "=required' forbids falling back to "
                        + "a fluss-only read. Wait for the tiering service to commit, or set the property "
                        + "to auto or disabled.", e);
            }
            // Nothing is in the lake, so the log holds everything: the fluss-only read is the whole table.
            return;
        }
        throw new DorisConnectorException("Table '" + handle.getDatabaseName() + "."
                + handle.getTableName() + "' is tiered into a lake and reading it requires combining the "
                + "lake with the fluss log, which is not supported yet. Set '"
                + FlussConnectorProperties.UNION_READ_MODE + "=disabled' to read only what the fluss log "
                + "still holds, which is NOT the whole table.");
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
        return props;
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        Map<String, String> beProperties = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : nodeProperties.entrySet()) {
            // Prefix-gated rather than copied wholesale: the map also carries the engine's own keys and
            // the synthetic ones it injects for EXPLAIN, none of which mean anything to the scanner.
            if (entry.getKey().startsWith(BE_PROPERTY_PREFIX)) {
                beProperties.put(entry.getKey(), entry.getValue());
            }
        }
        params.setFlussProperties(beProperties);
    }

    /**
     * The line a regression test reads to tell which way a scan was actually planned. {@code auto}
     * silently falls back to a fluss-only read, so "did this query read the lake?" is otherwise
     * invisible in the plan and a union-read test would pass without having tested anything.
     */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix)
                .append("flussScan: unionRead=").append(plannedUnionRead ? "yes" : "no")
                .append(", lakeSplits=0")
                .append(", logRanges=").append(plannedLogRanges)
                .append(", mode=")
                .append(FlussConnectorProperties.unionReadMode(catalogProperties).propertyValue())
                .append("\n");
    }
}
