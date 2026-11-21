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
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TLanceScanParams;

import org.apache.arrow.vector.types.pojo.Field;

import java.util.Base64;
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
 * Plans a Lance catalog scan of the statement's pinned version.
 *
 * <p>BE reads the data through lance-c; FE lists the version's fragments, decides which ranges read which of
 * them, and hands BE the options it opens the dataset with. Those options and the pushed filter travel once per
 * scan in {@code TLanceScanParams}, not once per range, so credentials are not repeated for every fragment.
 *
 * <p>What FE pushes into Lance: the conjuncts whose Doris and Lance semantics are identical, as one Substrait
 * filter (the engine then drops them from the Doris residuals); a LIMIT on every range once every conjunct is
 * pushed; COUNT(*) without a filter, from the manifest's row counts; and, for a pushed positive predicate that
 * one BTREE / BITMAP / LABEL_LIST index can drive, one range per index segment.
 *
 * <p>The scan properties (and so EXPLAIN) and the range list are planned separately from the same inputs: the
 * engine memoizes this provider by table handle, which the snapshot pin replaces, so no state is kept here.
 */
final class LanceScanPlanProvider implements ConnectorScanPlanProvider {

    /** Names the BE reader; {@code PluginDrivenScanNode.mapFileFormatType} maps it to FORMAT_LANCE. */
    static final String FILE_FORMAT = "lance";
    /**
     * The upgrade fence message, completed by the engine with which backend is a smooth-upgrade source.
     * The same text Lance tables have always reported.
     */
    static final String ADDITIONAL_TYPES_SEMANTICS = "Additional Lance types";
    /**
     * The fe-core session variable that groups fragments for debugging, byte-for-byte
     * {@code SessionVariable.LANCE_FRAGMENTS_PER_SPLIT}; absent or 0 plans ranges automatically.
     */
    static final String SESSION_FRAGMENTS_PER_SPLIT = "lance_fragments_per_split";

    /** Connector-private node-property keys: from {@link #getScanNodePropertiesResult} to the params and EXPLAIN. */
    static final String PROP_STORAGE_OPTION_PREFIX = "lance.storage_option.";
    static final String PROP_CATALOG_TYPE = "lance.catalog_type";
    static final String PROP_VERSION = "lance.version";
    static final String PROP_FRAGMENTS = "lance.fragments";
    /** The serialized Substrait filter, base64; absent when nothing is pushed. */
    static final String PROP_SUBSTRAIT_FILTER = "lance.substrait_filter";
    static final String PROP_PUSHDOWN_PREDICATE = "lance.pushdown_predicate";

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
        LanceTableHandle handle = lanceHandle(request.getTableHandle());
        LanceTableMetadata metadata = metadata(session, handle);
        List<ConnectorExpression> conjuncts = LancePredicateConverter.conjuncts(request.getFilter());
        // Any filter, pushed or not, makes the version's row count larger than the result.
        if (request.isCountPushdown() && conjuncts.isEmpty()) {
            return LanceScanPlanner.countRanges(metadata);
        }
        LancePredicateConverter converter = new LancePredicateConverter(metadata.getSchema());
        LancePredicateConverter.ConversionResult pushdown = converter.convert(conjuncts);
        LanceScanPlan plan = plan(session, handle, metadata, converter, pushdown);
        // A range may stop at the LIMIT only when Doris filters nothing after it. The engine passes no limit when
        // it kept a conjunct out of the filter; OFFSET is already folded in: the scan's limit is limit + offset.
        return plan.ranges(request.getLimit() >= 0 && pushdown.allPushed() ? request.getLimit() : -1);
    }

    /**
     * The scan's properties, the pushed Substrait filter among them; every conjunct it pushes is reported so,
     * and the engine drops those from the Doris residuals.
     */
    @Override
    public ScanNodePropertiesResult getScanNodePropertiesResult(ConnectorSession session,
            ConnectorTableHandle tableHandle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        LanceTableHandle handle = lanceHandle(tableHandle);
        LanceTableMetadata metadata = metadata(session, handle);
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

        LancePredicateConverter converter = new LancePredicateConverter(metadata.getSchema());
        LancePredicateConverter.ConversionResult pushdown =
                converter.convert(LancePredicateConverter.conjuncts(filter));
        if (pushdown.getSubstraitFilter().length > 0) {
            props.put(PROP_SUBSTRAIT_FILTER, Base64.getEncoder().encodeToString(pushdown.getSubstraitFilter()));
            props.put(PROP_PUSHDOWN_PREDICATE, pushdown.getDebugPredicate());
        }
        plan(session, handle, metadata, converter, pushdown).addExplainProperties(props);
        return ScanNodePropertiesResult.withPushdownTracking(props, pushdown.getNotPushedIndices());
    }

    /** Sets the options BE opens the dataset with and the pushed filter, once for the whole scan. */
    @Override
    public void populateScanLevelParams(TFileScanRangeParams params, Map<String, String> nodeProperties) {
        Map<String, String> storageOptions = new HashMap<>();
        for (Map.Entry<String, String> entry : nodeProperties.entrySet()) {
            if (entry.getKey().startsWith(PROP_STORAGE_OPTION_PREFIX)) {
                storageOptions.put(entry.getKey().substring(PROP_STORAGE_OPTION_PREFIX.length()), entry.getValue());
            }
        }
        String substraitFilter = nodeProperties.get(PROP_SUBSTRAIT_FILTER);
        if (storageOptions.isEmpty() && substraitFilter == null) {
            return;
        }
        TLanceScanParams lanceScanParams = new TLanceScanParams();
        if (!storageOptions.isEmpty()) {
            lanceScanParams.setLanceStorageOptions(storageOptions);
        }
        if (substraitFilter != null) {
            lanceScanParams.setLanceSubstraitFilter(Base64.getDecoder().decode(substraitFilter));
        }
        params.setLanceScanParams(lanceScanParams);
    }

    /** The snapshot a scan reads, how it was split and what was pushed; the storage options are never printed. */
    @Override
    public void appendExplainInfo(StringBuilder output, String prefix, Map<String, String> nodeProperties) {
        output.append(prefix).append("lanceCatalogType=").append(nodeProperties.get(PROP_CATALOG_TYPE))
                .append("\n");
        output.append(prefix).append("lanceVersion=").append(nodeProperties.get(PROP_VERSION)).append("\n");
        output.append(prefix).append("lanceFragments=").append(nodeProperties.get(PROP_FRAGMENTS)).append("\n");
        LanceScanPlan.appendExplain(output, prefix, nodeProperties);
        // The same condition planScan bounds the ranges by: a limit, and nothing left for Doris to filter.
        long limit = Long.parseLong(nodeProperties.getOrDefault(ScanNodePropertyKeys.SYNTHETIC_PUSHDOWN_LIMIT, "-1"));
        if (limit >= 0 && Boolean.parseBoolean(
                nodeProperties.get(ScanNodePropertyKeys.SYNTHETIC_ALL_CONJUNCTS_PUSHED))) {
            output.append(prefix).append("lanceLimit=").append(limit).append("\n");
        }
        String pushedPredicate = nodeProperties.get(PROP_PUSHDOWN_PREDICATE);
        if (pushedPredicate != null) {
            output.append(prefix).append("lancePushdownPredicate=").append(pushedPredicate).append("\n");
        }
    }

    private LanceScanPlan plan(ConnectorSession session, LanceTableHandle handle, LanceTableMetadata metadata,
            LancePredicateConverter converter, LancePredicateConverter.ConversionResult pushdown) {
        return LanceScanPlanner.plan(metadata, converter, pushdown.getPushedConjuncts(), fragmentsPerSplit(session),
                () -> LanceStatementScope.sharedIndexMetadata(session, handle, metadata,
                        () -> client.loadIndexMetadata(handle.getDbName(), handle.getTableName(),
                                metadata.getVersion())));
    }

    private static int fragmentsPerSplit(ConnectorSession session) {
        String value = session.getSessionProperties().get(SESSION_FRAGMENTS_PER_SPLIT);
        return value == null || value.isEmpty() ? 0 : Integer.parseInt(value.trim());
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
