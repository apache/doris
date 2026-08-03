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
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorPartitionInfo;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.ConnectorTableStatistics;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.api.scan.ConnectorScanRange;
import org.apache.doris.connector.api.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileScanRangeParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A stand-in for the embedded paimon sibling connector, hand written because this module bans mock
 * frameworks. It plays the part that matters for the gateway: it owns a handle type of its OWN
 * ({@link Handle}) that the fluss code can never recognize by class, and it records every call it is
 * forwarded so a test can assert the forward actually happened — and reached the sibling, not fluss.
 *
 * <p>Every answer is a distinctive constant, so an assertion that finds it can only have come through the
 * sibling; a test that passed because fluss answered instead would see fluss's own values.
 */
final class RecordingLakeSibling implements Connector {

    /** Fixture shorthand: this handle has not been pinned to a snapshot. */
    static final long UNPINNED = Long.MIN_VALUE;

    /**
     * The sibling's own handle type: the analogue of {@code PaimonTableHandle}. It carries the pin the
     * same way a real one does — as a NEW handle returned by {@code applySnapshot} — so a test can tell a
     * scan that was planned on the pinned handle from one planned on the raw one.
     */
    static final class Handle implements ConnectorTableHandle {
        private static final long serialVersionUID = 1L;

        final String dbName;
        final String tableName;
        final long pinnedSnapshotId;

        Handle(String dbName, String tableName) {
            this(dbName, tableName, UNPINNED);
        }

        Handle(String dbName, String tableName, long pinnedSnapshotId) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.pinnedSnapshotId = pinnedSnapshotId;
        }
    }

    /** The table name reported in the schema; a value fluss's own path could never produce. */
    static final String SCHEMA_TABLE_NAME = "lake_side_table";
    static final String FORMAT_TYPE = "PAIMON";
    static final String COLUMN_NAME = "lake_only_column";
    static final String PARTITION_NAME = "dt=lake";
    static final long ROW_COUNT = 4242L;

    /** The scan plan provider this sibling hands out; identity is what the routing test asserts. */
    final ConnectorScanPlanProvider scanPlanProvider = new ScanPlanProvider();

    final Map<String, String> properties;
    final List<String> calls = new ArrayList<>();

    /** When false, the sibling reports the lake table as absent (nothing tiered to it yet). */
    boolean lakeTableExists = true;

    /**
     * The columns the lake table reports, by name. Defaults to the single lake-only column the gateway
     * tests assert on; a union test replaces it with the fluss table's columns plus the three system
     * columns tiering appends, which is what a real lake table looks like.
     */
    Map<String, ConnectorColumnHandle> lakeColumns = defaultLakeColumns();

    /** The ranges this sibling's scan planner returns, so a union test can recognize the lake half. */
    List<ConnectorScanRange> lakeRanges = Collections.emptyList();

    /** The node properties this sibling's scan planner reports, to be merged into the scan node's. */
    Map<String, String> lakeNodeProperties = Collections.emptyMap();

    /** The handle the scan planner was last asked to plan; proves WHICH handle reached the lake half. */
    Handle plannedHandle;

    /** The columns the scan planner was last asked to plan, in order. */
    List<ConnectorColumnHandle> plannedColumns = Collections.emptyList();

    /** The node properties last handed to {@code populateScanLevelParams}. */
    Map<String, String> populatedNodeProperties;

    private static Map<String, ConnectorColumnHandle> defaultLakeColumns() {
        Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
        handles.put(COLUMN_NAME, new LakeColumn(COLUMN_NAME));
        return handles;
    }

    /** The sibling's own column-handle type, which fluss can only pass through, never build. */
    static final class LakeColumn implements ConnectorColumnHandle {
        private static final long serialVersionUID = 1L;

        final String name;

        LakeColumn(String name) {
            this.name = name;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof LakeColumn && name.equals(((LakeColumn) other).name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return "LakeColumn{" + name + "}";
        }
    }

    /**
     * The sibling's own range type. Deliberately NOT a {@link FlussScanRange}: on a union read both kinds
     * share one list, and a fixture that handed back fluss ranges would hide every place that assumes the
     * list is homogeneous.
     */
    static final class LakeRange implements ConnectorScanRange {
        private static final long serialVersionUID = 1L;

        /** What the paimon connector calls the bucket a split holds; the cross-connector contract. */
        static final String BUCKET_PROPERTY = "paimon.bucket";

        private final Map<String, String> properties;
        private final Map<String, String> partitionValues;

        LakeRange() {
            this(Collections.emptyMap(), Collections.emptyMap());
        }

        private LakeRange(Map<String, String> properties, Map<String, String> partitionValues) {
            this.properties = properties;
            this.partitionValues = partitionValues;
        }

        /** A split of {@code bucket} of an unpartitioned table. */
        static LakeRange inBucket(int bucket) {
            return inBucket(bucket, Collections.emptyMap());
        }

        /** A split of {@code bucket} of the partition with these values. */
        static LakeRange inBucket(int bucket, Map<String, String> partitionValues) {
            return new LakeRange(
                    Collections.singletonMap(BUCKET_PROPERTY, String.valueOf(bucket)), partitionValues);
        }

        /** A split that does not say which bucket it holds — what an older paimon plugin produces. */
        static LakeRange withoutABucket() {
            return new LakeRange();
        }

        @Override
        public String getTableFormatType() {
            return "paimon";
        }

        @Override
        public Map<String, String> getProperties() {
            return properties;
        }

        @Override
        public Map<String, String> getPartitionValues() {
            return partitionValues;
        }

        @Override
        public String toString() {
            return "LakeRange" + properties + partitionValues;
        }
    }

    /** Records what the lake half was asked to plan and answers with this sibling's canned values. */
    private final class ScanPlanProvider implements ConnectorScanPlanProvider {

        @Override
        public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
            calls.add("planScan");
            plannedHandle = (Handle) request.getTableHandle();
            plannedColumns = new ArrayList<>(request.getColumns());
            return lakeRanges;
        }

        @Override
        public Map<String, String> getScanNodeProperties(ConnectorSession session,
                ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
                Optional<ConnectorExpression> filter) {
            calls.add("getScanNodeProperties");
            plannedColumns = new ArrayList<>(columns);
            return lakeNodeProperties;
        }

        @Override
        public void populateScanLevelParams(TFileScanRangeParams params,
                Map<String, String> nodeProperties) {
            calls.add("populateScanLevelParams");
            populatedNodeProperties = new LinkedHashMap<>(nodeProperties);
        }
    }

    /** How many metadata instances this sibling was asked to build (the per-statement funnel's proof). */
    int metadataBuilds;
    boolean closed;

    RecordingLakeSibling(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session) {
        metadataBuilds++;
        return new Metadata();
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider() {
        return scanPlanProvider;
    }

    @Override
    public ConnectorScanPlanProvider getScanPlanProvider(ConnectorTableHandle handle) {
        calls.add("getScanPlanProvider");
        return scanPlanProvider;
    }

    /**
     * Set false to imitate a sibling that never overrode {@link Connector#ownsHandle} and so inherits the
     * SPI default. That is what a real connector looks like before it is first used as a sibling, and it is
     * invisible to a stand-in that always answers correctly.
     */
    boolean claimsItsOwnHandles = true;

    @Override
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return claimsItsOwnHandles && handle instanceof Handle;
    }

    @Override
    public void close() {
        closed = true;
    }

    /** The sibling's metadata: records the forward, then answers with its own distinctive values. */
    private final class Metadata implements ConnectorMetadata {

        @Override
        public Optional<ConnectorTableHandle> getTableHandle(
                ConnectorSession session, String dbName, String tableName) {
            calls.add("getTableHandle:" + dbName + "." + tableName);
            return lakeTableExists ? Optional.of(new Handle(dbName, tableName)) : Optional.empty();
        }

        @Override
        public ConnectorTableSchema getTableSchema(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("getTableSchema");
            List<ConnectorColumn> columns = Collections.singletonList(
                    new ConnectorColumn(COLUMN_NAME, ConnectorType.of("INT"), "", true, null, true));
            return new ConnectorTableSchema(
                    SCHEMA_TABLE_NAME, columns, FORMAT_TYPE, Collections.emptyMap());
        }

        @Override
        public Map<String, ConnectorColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("getColumnHandles");
            return lakeColumns;
        }

        /**
         * Threads the pin the way a real MVCC connector does: a NEW handle carrying the snapshot. The
         * empty-properties latest-pin form is the only one fluss can send, so that is the only one
         * answered here — a caller that invented connector-specific options would go unnoticed otherwise.
         */
        @Override
        public ConnectorTableHandle applySnapshot(ConnectorSession session,
                ConnectorTableHandle handle, ConnectorMvccSnapshot snapshot) {
            calls.add("applySnapshot:" + snapshot.getSnapshotId() + ":" + snapshot.getProperties());
            Handle lakeHandle = (Handle) handle;
            return new Handle(lakeHandle.dbName, lakeHandle.tableName, snapshot.getSnapshotId());
        }

        @Override
        public List<String> listPartitionNames(ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("listPartitionNames");
            return Collections.singletonList(PARTITION_NAME);
        }

        @Override
        public List<ConnectorPartitionInfo> listPartitions(ConnectorSession session,
                ConnectorTableHandle handle, Optional<ConnectorExpression> filter) {
            calls.add("listPartitions");
            return Collections.singletonList(new ConnectorPartitionInfo(
                    PARTITION_NAME, Collections.singletonMap("dt", "lake"), Collections.emptyMap(),
                    Collections.singletonList("lake"), Collections.emptyList()));
        }

        @Override
        public Optional<ConnectorTableStatistics> getTableStatistics(
                ConnectorSession session, ConnectorTableHandle handle) {
            calls.add("getTableStatistics");
            return Optional.of(new ConnectorTableStatistics(ROW_COUNT, -1));
        }

        @Override
        public List<String> listSupportedSysTables(ConnectorSession session,
                ConnectorTableHandle baseTableHandle) {
            calls.add("listSupportedSysTables");
            return Collections.singletonList("snapshots");
        }

        @Override
        public Optional<ConnectorTableHandle> getSysTableHandle(ConnectorSession session,
                ConnectorTableHandle baseTableHandle, String sysName) {
            calls.add("getSysTableHandle:" + sysName);
            return Optional.empty();
        }

        @Override
        public boolean isPartitionValuesSysTable(ConnectorSession session,
                ConnectorTableHandle baseTableHandle, String sysName) {
            calls.add("isPartitionValuesSysTable:" + sysName);
            return true;
        }
    }
}
