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

package org.apache.doris.connector.delta;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.schema.external.TSchema;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Plans native Parquet file ranges from a pinned Delta snapshot. */
public final class DeltaScanPlanProvider implements ConnectorScanPlanProvider {
    private final DeltaCatalogAdapter catalogAdapter;
    private final Map<String, String> properties;

    public DeltaScanPlanProvider(DeltaCatalogAdapter catalogAdapter,
            Map<String, String> properties) {
        this.catalogAdapter = catalogAdapter;
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public boolean ignorePartitionPruneShortCircuit() {
        return true;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        return planScan(session, request.getTableHandle(), request.getColumns(), request.getFilter());
    }

    List<ConnectorScanRange> planScan(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        DeltaTableHandle deltaHandle = (DeltaTableHandle) handle;
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot(deltaHandle);
        List<ConnectorScanRange> ranges = new ArrayList<>(snapshot.getActiveFiles().size());
        for (DeltaScanFile file : snapshot.getActiveFiles()) {
            if (filter.isEmpty() || DeltaPartitionPruner.mayMatch(
                    file.getPartitionValues(), filter.get())) {
                ranges.add(new DeltaScanRange(
                        catalogAdapter.getBackendScanFile(deltaHandle, file)));
            }
        }
        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        DeltaKernelSnapshot snapshot = catalogAdapter.loadSnapshot((DeltaTableHandle) handle);
        Map<String, String> scanProperties = new LinkedHashMap<>();
        scanProperties.put("file_format_type", "parquet");
        if (!snapshot.getPartitionColumnNames().isEmpty()) {
            scanProperties.put("path_partition_keys",
                    String.join(",", snapshot.getPartitionColumnNames()));
        }
        for (Map.Entry<String, String> entry : DeltaStorageProperties
                .toBackendProperties(properties).entrySet()) {
            scanProperties.put("location." + entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : catalogAdapter
                .getBackendStorageProperties((DeltaTableHandle) handle).entrySet()) {
            scanProperties.put("location." + entry.getKey(), entry.getValue());
        }
        DeltaVendedCredentialLifetime.validateScan(session, scanProperties,
                DeltaConnectorProperties.positiveLongProperty(properties,
                        DeltaConnectorProperties.UNITY_CREDENTIAL_MIN_LIFETIME_MS,
                        DeltaConnectorProperties.DEFAULT_UNITY_CREDENTIAL_MIN_LIFETIME_MS));
        String serializedSchema = DeltaSchemaInfo.serializeIfMapped(snapshot);
        if (serializedSchema != null) {
            scanProperties.put(DeltaSchemaInfo.SERIALIZED_SCHEMA_PROPERTY, serializedSchema);
            scanProperties.put(DeltaSchemaInfo.SCHEMA_VERSION_PROPERTY,
                    String.valueOf(snapshot.getVersion()));
        }
        return scanProperties;
    }

    @Override
    public void populateScanLevelParams(TFileScanRangeParams params,
            Map<String, String> nodeProperties) {
        String serializedSchema = nodeProperties.get(DeltaSchemaInfo.SERIALIZED_SCHEMA_PROPERTY);
        if (serializedSchema == null) {
            return;
        }
        String schemaVersion = nodeProperties.get(DeltaSchemaInfo.SCHEMA_VERSION_PROPERTY);
        if (schemaVersion == null) {
            throw new DorisConnectorException(
                    "Delta mapped schema is missing its pinned snapshot version");
        }
        TSchema schema = new TSchema();
        try {
            new TDeserializer().deserialize(schema, Base64.getDecoder().decode(serializedSchema));
            params.addToHistorySchemaInfo(schema);
            params.setCurrentSchemaId(Long.parseLong(schemaVersion));
            params.setExternalScanSemanticsVersion(
                    DeltaSchemaInfo.EXTERNAL_SCAN_SEMANTICS_VERSION);
        } catch (TException | IllegalArgumentException e) {
            throw new DorisConnectorException(
                    "Failed to deserialize Delta mapped schema for scan", e);
        }
    }

    static boolean isBackendStorageProperty(String key) {
        return DeltaStorageProperties.isBackendProperty(key);
    }
}
