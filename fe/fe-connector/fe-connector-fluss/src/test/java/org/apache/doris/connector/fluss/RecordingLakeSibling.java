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
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

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

    /** The sibling's own handle type: the analogue of {@code PaimonTableHandle}. */
    static final class Handle implements ConnectorTableHandle {
        private static final long serialVersionUID = 1L;

        final String dbName;
        final String tableName;

        Handle(String dbName, String tableName) {
            this.dbName = dbName;
            this.tableName = tableName;
        }
    }

    /** The table name reported in the schema; a value fluss's own path could never produce. */
    static final String SCHEMA_TABLE_NAME = "lake_side_table";
    static final String FORMAT_TYPE = "PAIMON";
    static final String COLUMN_NAME = "lake_only_column";
    static final String PARTITION_NAME = "dt=lake";
    static final long ROW_COUNT = 4242L;

    /** The scan plan provider this sibling hands out; identity is what the routing test asserts. */
    final ConnectorScanPlanProvider scanPlanProvider = (session, request) -> Collections.emptyList();

    final Map<String, String> properties;
    final List<String> calls = new ArrayList<>();

    /** When false, the sibling reports the lake table as absent (nothing tiered to it yet). */
    boolean lakeTableExists = true;

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
    public boolean ownsHandle(ConnectorTableHandle handle) {
        return handle instanceof Handle;
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
            Map<String, ConnectorColumnHandle> handles = new LinkedHashMap<>();
            handles.put(COLUMN_NAME, new FlussColumnHandle(COLUMN_NAME, 0));
            return handles;
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
