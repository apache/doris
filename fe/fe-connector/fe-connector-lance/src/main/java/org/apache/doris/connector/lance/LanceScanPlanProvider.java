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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceScanParams;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Plans a Lance catalog scan: one range per fragment of the statement's pinned version.
 *
 * <p>BE reads the data through lance-c; FE only lists the version's fragments and hands BE the options it
 * opens the dataset with. Those options travel once per scan in {@code TLanceScanParams}, not once per
 * range, so credentials are not repeated for every fragment.
 */
final class LanceScanPlanProvider implements ConnectorScanPlanProvider {

    /** Names the BE reader; {@code PluginDrivenScanNode.mapFileFormatType} maps it to FORMAT_LANCE. */
    static final String FILE_FORMAT = "lance";
    /**
     * The upgrade fence message, completed by the engine with which backend is a smooth-upgrade source.
     * The same text Lance tables have always reported.
     */
    static final String ADDITIONAL_TYPES_SEMANTICS = "Additional Lance types";

    /** Connector-private node-property keys: from {@link #getScanNodeProperties} to the params and EXPLAIN. */
    static final String PROP_STORAGE_OPTION_PREFIX = "lance.storage_option.";
    static final String PROP_CATALOG_TYPE = "lance.catalog_type";
    static final String PROP_VERSION = "lance.version";
    static final String PROP_FRAGMENTS = "lance.fragments";

    private final LanceCatalogClient client;
    private final String catalogType;
    private final Supplier<Map<String, String>> backendStorageProperties;

    /**
     * @param backendStorageProperties the catalog's storage keys in BE's spelling. lance-c ignores them -
     *         it opens the dataset with the Lance options - but BE's shared file-system layer and the file
     *         cache key read the scan's {@code properties} as they do for every file scan.
     */
    LanceScanPlanProvider(LanceCatalogClient client, String catalogType,
            Supplier<Map<String, String>> backendStorageProperties) {
        this.client = client;
        this.catalogType = catalogType;
        this.backendStorageProperties = backendStorageProperties;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        LanceTableMetadata metadata = metadata(session, lanceHandle(request.getTableHandle()));
        List<LanceFragmentInfo> fragments = metadata.getFragments();
        Set<Long> seen = new HashSet<>();
        long targetSplitSize = 1;
        for (LanceFragmentInfo fragment : fragments) {
            if (!seen.add(fragment.getId())) {
                throw new DorisConnectorException("Duplicate Lance fragment id " + fragment.getId()
                        + " at dataset version " + metadata.getVersion());
            }
            // Weights are proportional to the largest range of this plan.
            targetSplitSize = Math.max(targetSplitSize, fragment.getSchedulingWeight());
        }
        List<ConnectorScanRange> ranges = new ArrayList<>(fragments.size());
        for (LanceFragmentInfo fragment : fragments) {
            ranges.add(new LanceScanRange(metadata.getDatasetUri(), metadata.getVersion(),
                    Collections.singletonList(fragment.getId()), fragment.getSchedulingWeight(), targetSplitSize));
        }
        return ranges;
    }

    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter) {
        LanceTableMetadata metadata = metadata(session, lanceHandle(handle));
        Map<String, String> props = new LinkedHashMap<>();
        props.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, FILE_FORMAT);
        backendStorageProperties.get().forEach(
                (key, value) -> props.put(ScanNodePropertyKeys.LOCATION_PREFIX + key, value));
        metadata.getLanceStorageOptions().forEach(
                (key, value) -> props.put(PROP_STORAGE_OPTION_PREFIX + key, value));
        if (projectsCurrentReaderType(metadata, columns)) {
            props.put(ScanNodePropertyKeys.REQUIRED_CURRENT_BACKEND_SEMANTICS, ADDITIONAL_TYPES_SEMANTICS);
        }
        props.put(PROP_CATALOG_TYPE, catalogType);
        props.put(PROP_VERSION, String.valueOf(metadata.getVersion()));
        props.put(PROP_FRAGMENTS, String.valueOf(metadata.getFragments().size()));
        return props;
    }

    /** Sets the options BE opens the dataset with, once for the whole scan. */
    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        Map<String, String> storageOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : nodeProperties.entrySet()) {
            if (entry.getKey().startsWith(PROP_STORAGE_OPTION_PREFIX)) {
                storageOptions.put(entry.getKey().substring(PROP_STORAGE_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        if (!storageOptions.isEmpty()) {
            params.setLanceScanParams(new TLanceScanParams().setLanceStorageOptions(storageOptions));
        }
    }

    /** The snapshot a scan reads and how it was split; the storage options are never printed. */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix).append("lanceCatalogType=").append(nodeProperties.get(PROP_CATALOG_TYPE))
                .append("\n");
        output.append(prefix).append("lanceVersion=").append(nodeProperties.get(PROP_VERSION)).append("\n");
        output.append(prefix).append("lanceFragments=").append(nodeProperties.get(PROP_FRAGMENTS)).append("\n");
        output.append(prefix).append("lanceFragmentGrouping=FRAGMENT\n");
    }

    /**
     * Whether a projected column needs the BE Lance reader that came with the additional types; see
     * {@link LanceTypeMapping#requiresCurrentBeReader}. Only the projected columns count: an unread column
     * of such a type does not keep an older backend from serving the scan.
     */
    private static boolean projectsCurrentReaderType(LanceTableMetadata metadata,
            List<ConnectorColumnHandle> columns) {
        Set<String> projected = new HashSet<>();
        for (ConnectorColumnHandle column : columns) {
            projected.add(((NamedColumnHandle) column).getName().toLowerCase(Locale.ROOT));
        }
        for (Field field : metadata.getSchema().getFields()) {
            if (projected.contains(field.getName().toLowerCase(Locale.ROOT))
                    && LanceTypeMapping.requiresCurrentBeReader(field)) {
                return true;
            }
        }
        return false;
    }

    private LanceTableMetadata metadata(ConnectorSession session, LanceTableHandle handle) {
        return LanceStatementScope.sharedMetadata(session, handle,
                () -> client.loadTableMetadata(handle.getDbName(), handle.getTableName(), handle.getVersion()));
    }

    private static LanceTableHandle lanceHandle(ConnectorTableHandle handle) {
        if (handle instanceof LanceTableHandle) {
            return (LanceTableHandle) handle;
        }
        throw new DorisConnectorException("A lance catalog cannot scan through a "
                + handle.getClass().getSimpleName() + "; only its own tables are readable.");
    }
}
