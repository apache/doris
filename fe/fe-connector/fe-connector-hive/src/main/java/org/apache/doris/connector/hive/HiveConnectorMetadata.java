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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorTableSchema;
import org.apache.doris.connector.api.handle.ConnectorColumnHandle;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorFilterConstraint;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.FilterApplicationResult;
import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsPartitionInfo;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.hms.HmsTypeMapping;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@link ConnectorMetadata} implementation for Hive (HMS-based) catalogs.
 *
 * <p>Provides read-only metadata operations:
 * <ul>
 *   <li>List databases and tables</li>
 *   <li>Get table schema (columns + partition keys)</li>
 *   <li>Table format detection (HIVE/HUDI/ICEBERG)</li>
 *   <li>Partition name listing</li>
 *   <li>Column handle resolution for scan planning</li>
 *   <li>Partition pruning via {@code applyFilter}</li>
 * </ul>
 */
public class HiveConnectorMetadata implements ConnectorMetadata {

    private static final Logger LOG = LogManager.getLogger(HiveConnectorMetadata.class);

    private final HmsClient hmsClient;
    private final Map<String, String> properties;
    private final HmsTypeMapping.Options typeMappingOptions;

    public HiveConnectorMetadata(HmsClient hmsClient, Map<String, String> properties) {
        this.hmsClient = hmsClient;
        this.properties = properties;
        this.typeMappingOptions = buildTypeMappingOptions(properties);
    }

    // ========== ConnectorSchemaOps ==========

    @Override
    public List<String> listDatabaseNames(ConnectorSession session) {
        return hmsClient.listDatabases();
    }

    @Override
    public boolean databaseExists(ConnectorSession session, String dbName) {
        try {
            hmsClient.getDatabase(dbName);
            return true;
        } catch (HmsClientException e) {
            LOG.debug("Database '{}' not found: {}", dbName, e.getMessage());
            return false;
        }
    }

    // ========== ConnectorTableOps ==========

    @Override
    public List<String> listTableNames(ConnectorSession session, String dbName) {
        return hmsClient.listTables(dbName);
    }

    @Override
    public Optional<ConnectorTableHandle> getTableHandle(
            ConnectorSession session, String dbName, String tableName) {
        if (!hmsClient.tableExists(dbName, tableName)) {
            return Optional.empty();
        }
        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        HiveTableType tableType = HiveTableFormatDetector.detect(tableInfo);

        // Build partition key column names
        List<String> partKeyNames = Collections.emptyList();
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys != null && !partKeys.isEmpty()) {
            partKeyNames = partKeys.stream()
                    .map(ConnectorColumn::getName)
                    .collect(Collectors.toList());
        }

        HiveTableHandle handle = new HiveTableHandle.Builder(dbName, tableName, tableType)
                .inputFormat(tableInfo.getInputFormat())
                .serializationLib(tableInfo.getSerializationLib())
                .location(tableInfo.getLocation())
                .partitionKeyNames(partKeyNames)
                .sdParameters(tableInfo.getSdParameters())
                .tableParameters(tableInfo.getParameters())
                .build();
        return Optional.of(handle);
    }

    @Override
    public ConnectorTableSchema getTableSchema(
            ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        String dbName = hiveHandle.getDbName();
        String tableName = hiveHandle.getTableName();

        HmsTableInfo tableInfo = hmsClient.getTable(dbName, tableName);
        List<ConnectorColumn> columns = buildColumns(tableInfo);
        List<ConnectorColumn> partitionKeys = buildPartitionKeys(tableInfo);

        // Merge: regular columns + partition columns
        List<ConnectorColumn> allColumns = new ArrayList<>(columns.size() + partitionKeys.size());
        allColumns.addAll(columns);
        allColumns.addAll(partitionKeys);

        String formatType = detectFormatType(tableInfo);
        Map<String, String> tableProperties = tableInfo.getParameters() != null
                ? tableInfo.getParameters() : Collections.emptyMap();

        return new ConnectorTableSchema(tableName, allColumns, formatType, tableProperties);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    // ========== ConnectorTableOps: Column Handles ==========

    @Override
    public Map<String, ConnectorColumnHandle> getColumnHandles(
            ConnectorSession session, ConnectorTableHandle handle) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        HmsTableInfo tableInfo = hmsClient.getTable(
                hiveHandle.getDbName(), hiveHandle.getTableName());

        Set<String> partKeyNames = hiveHandle.getPartitionKeyNames() != null
                ? hiveHandle.getPartitionKeyNames().stream().collect(Collectors.toSet())
                : Collections.emptySet();

        Map<String, ConnectorColumnHandle> result = new LinkedHashMap<>();
        List<ConnectorColumn> allCols = new ArrayList<>();
        if (tableInfo.getColumns() != null) {
            allCols.addAll(tableInfo.getColumns());
        }
        if (tableInfo.getPartitionKeys() != null) {
            allCols.addAll(tableInfo.getPartitionKeys());
        }
        for (ConnectorColumn col : allCols) {
            boolean isPartKey = partKeyNames.contains(col.getName());
            result.put(col.getName(), new HiveColumnHandle(
                    col.getName(), col.getType().getTypeName(), isPartKey));
        }
        return result;
    }

    // ========== ConnectorPushdownOps: Filter Pushdown ==========

    @Override
    public Optional<FilterApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle handle,
            ConnectorFilterConstraint constraint) {
        HiveTableHandle hiveHandle = (HiveTableHandle) handle;
        List<String> partKeyNames = hiveHandle.getPartitionKeyNames();
        if (partKeyNames == null || partKeyNames.isEmpty()) {
            return Optional.empty();
        }

        // Extract equality predicates on partition columns from the expression
        Map<String, List<String>> partitionPredicates = extractPartitionPredicates(
                constraint.getExpression(), partKeyNames);
        if (partitionPredicates.isEmpty()) {
            return Optional.empty();
        }

        // Build partition name filter patterns for HMS
        List<String> allPartNames = hmsClient.listPartitionNames(
                hiveHandle.getDbName(), hiveHandle.getTableName(), 100000);
        List<String> matchedPartNames = prunePartitionNames(
                allPartNames, partKeyNames, partitionPredicates);

        if (matchedPartNames.size() == allPartNames.size()) {
            // No pruning effect
            return Optional.empty();
        }

        List<HmsPartitionInfo> prunedPartitions = matchedPartNames.isEmpty()
                ? Collections.emptyList()
                : hmsClient.getPartitions(hiveHandle.getDbName(),
                        hiveHandle.getTableName(), matchedPartNames);

        LOG.info("Partition pruning: {}.{} all={} pruned={}",
                hiveHandle.getDbName(), hiveHandle.getTableName(),
                allPartNames.size(), prunedPartitions.size());

        HiveTableHandle newHandle = hiveHandle.toBuilder()
                .prunedPartitions(prunedPartitions)
                .build();
        return Optional.of(new FilterApplicationResult<>(
                newHandle, constraint.getExpression(), false));
    }

    // ========== Internal helpers ==========

    private List<ConnectorColumn> buildColumns(HmsTableInfo tableInfo) {
        List<ConnectorColumn> spiColumns = tableInfo.getColumns();
        if (spiColumns == null || spiColumns.isEmpty()) {
            return Collections.emptyList();
        }
        // HmsTableInfo already returns ConnectorColumn with types mapped by HmsTypeMapping
        // during ThriftHmsClient.getTable(). Enrich with default values if available.
        Map<String, String> defaults = getDefaultValues(tableInfo);
        if (defaults.isEmpty()) {
            return spiColumns;
        }
        List<ConnectorColumn> enriched = new ArrayList<>(spiColumns.size());
        for (ConnectorColumn col : spiColumns) {
            String defaultVal = defaults.get(col.getName());
            if (defaultVal != null && col.getDefaultValue() == null) {
                enriched.add(new ConnectorColumn(
                        col.getName(), col.getType(), col.getComment(),
                        col.isNullable(), defaultVal));
            } else {
                enriched.add(col);
            }
        }
        return enriched;
    }

    private List<ConnectorColumn> buildPartitionKeys(HmsTableInfo tableInfo) {
        List<ConnectorColumn> partKeys = tableInfo.getPartitionKeys();
        if (partKeys == null) {
            return Collections.emptyList();
        }
        return partKeys;
    }

    private Map<String, String> getDefaultValues(HmsTableInfo tableInfo) {
        try {
            return hmsClient.getDefaultColumnValues(
                    tableInfo.getDbName(), tableInfo.getTableName());
        } catch (HmsClientException e) {
            LOG.debug("Could not get default column values for {}.{}: {}",
                    tableInfo.getDbName(), tableInfo.getTableName(), e.getMessage());
            return Collections.emptyMap();
        }
    }

    private String detectFormatType(HmsTableInfo tableInfo) {
        HiveTableType type = HiveTableFormatDetector.detect(tableInfo);
        switch (type) {
            case HIVE:
                return resolveHiveFileFormat(tableInfo.getInputFormat());
            case HUDI:
                return "HUDI";
            case ICEBERG:
                return "ICEBERG";
            default:
                return "UNKNOWN";
        }
    }

    /**
     * Resolve the Hive file format name from the input format class.
     */
    private static String resolveHiveFileFormat(String inputFormat) {
        if (inputFormat == null) {
            return "HIVE";
        }
        if (inputFormat.contains("Parquet") || inputFormat.contains("parquet")) {
            return "HIVE_PARQUET";
        }
        if (inputFormat.contains("Orc") || inputFormat.contains("orc")) {
            return "HIVE_ORC";
        }
        if (inputFormat.contains("Text") || inputFormat.contains("text")) {
            return "HIVE_TEXT";
        }
        return "HIVE";
    }

    private static HmsTypeMapping.Options buildTypeMappingOptions(Map<String, String> props) {
        boolean binaryAsString = Boolean.parseBoolean(
                props.getOrDefault(HiveConnectorProperties.ENABLE_MAPPING_BINARY_AS_STRING, "false"));
        boolean timestampTz = Boolean.parseBoolean(
                props.getOrDefault(HiveConnectorProperties.ENABLE_MAPPING_TIMESTAMP_TZ, "false"));
        return new HmsTypeMapping.Options(
                HmsTypeMapping.DEFAULT_TIME_SCALE, binaryAsString, timestampTz);
    }

    /**
     * Extracts equality predicates on partition columns from the expression tree.
     * Supports: col = 'value', col IN ('v1', 'v2', ...), AND combinations.
     */
    private Map<String, List<String>> extractPartitionPredicates(
            ConnectorExpression expr, List<String> partKeyNames) {
        Set<String> partKeySet = partKeyNames.stream().collect(Collectors.toSet());
        Map<String, List<String>> result = new HashMap<>();
        extractPredicatesRecursive(expr, partKeySet, result);
        return result;
    }

    private void extractPredicatesRecursive(ConnectorExpression expr,
            Set<String> partKeySet, Map<String, List<String>> result) {
        if (expr instanceof ConnectorAnd) {
            for (ConnectorExpression child : ((ConnectorAnd) expr).getConjuncts()) {
                extractPredicatesRecursive(child, partKeySet, result);
            }
        } else if (expr instanceof ConnectorComparison) {
            ConnectorComparison cmp = (ConnectorComparison) expr;
            if (cmp.getOperator() == ConnectorComparison.Operator.EQ) {
                String colName = extractColumnName(cmp.getLeft());
                String value = extractLiteralValue(cmp.getRight());
                if (colName != null && value != null && partKeySet.contains(colName)) {
                    result.computeIfAbsent(colName, k -> new ArrayList<>()).add(value);
                }
            }
        } else if (expr instanceof ConnectorIn) {
            ConnectorIn inExpr = (ConnectorIn) expr;
            if (!inExpr.isNegated()) {
                String colName = extractColumnName(inExpr.getValue());
                if (colName != null && partKeySet.contains(colName)) {
                    List<String> values = new ArrayList<>();
                    for (ConnectorExpression item : inExpr.getInList()) {
                        String val = extractLiteralValue(item);
                        if (val != null) {
                            values.add(val);
                        }
                    }
                    if (!values.isEmpty()) {
                        result.computeIfAbsent(colName, k -> new ArrayList<>()).addAll(values);
                    }
                }
            }
        }
    }

    private String extractColumnName(ConnectorExpression expr) {
        if (expr instanceof org.apache.doris.connector.api.pushdown.ConnectorColumnRef) {
            return ((org.apache.doris.connector.api.pushdown.ConnectorColumnRef) expr).getColumnName();
        }
        return null;
    }

    private String extractLiteralValue(ConnectorExpression expr) {
        if (expr instanceof ConnectorLiteral) {
            Object val = ((ConnectorLiteral) expr).getValue();
            return val != null ? String.valueOf(val) : null;
        }
        return null;
    }

    /**
     * Prunes partition names based on extracted equality predicates.
     * Partition names follow the Hive convention: key1=val1/key2=val2
     */
    private List<String> prunePartitionNames(List<String> allPartNames,
            List<String> partKeyNames, Map<String, List<String>> predicates) {
        List<String> matched = new ArrayList<>();
        for (String partName : allPartNames) {
            Map<String, String> partValues = parsePartitionName(partName, partKeyNames);
            if (matchesPredicates(partValues, predicates)) {
                matched.add(partName);
            }
        }
        return matched;
    }

    private Map<String, String> parsePartitionName(String partName,
            List<String> partKeyNames) {
        Map<String, String> values = new HashMap<>();
        String[] parts = partName.split("/");
        for (String part : parts) {
            int eq = part.indexOf('=');
            if (eq > 0) {
                values.put(part.substring(0, eq), part.substring(eq + 1));
            }
        }
        return values;
    }

    private boolean matchesPredicates(Map<String, String> partValues,
            Map<String, List<String>> predicates) {
        for (Map.Entry<String, List<String>> entry : predicates.entrySet()) {
            String colName = entry.getKey();
            List<String> allowedValues = entry.getValue();
            String actualValue = partValues.get(colName);
            if (actualValue == null || !allowedValues.contains(actualValue)) {
                return false;
            }
        }
        return true;
    }
}
