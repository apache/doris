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

import org.apache.doris.connector.hms.HmsClient;
import org.apache.doris.connector.hms.HmsClientException;
import org.apache.doris.connector.hms.HmsTableInfo;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorType;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * One effective Hive write generation: the raw HMS object plus the exact columns exposed to binding.
 */
final class HiveWriteMetadataSnapshot {

    private static final int MAX_VARCHAR_LENGTH = 65533;

    private final HmsTableInfo table;
    private final List<ConnectorColumn> dataColumns;
    private final List<ConnectorColumn> partitionColumns;
    private final String identity;

    private HiveWriteMetadataSnapshot(HmsTableInfo table, Map<String, String> defaults) {
        this.table = table;
        this.dataColumns = Collections.unmodifiableList(buildDataColumns(table, defaults));
        this.partitionColumns = Collections.unmodifiableList(buildPartitionColumns(table));
        this.identity = buildIdentity();
    }

    static HiveWriteMetadataSnapshot of(HmsTableInfo table, Map<String, String> defaults) {
        return new HiveWriteMetadataSnapshot(table,
                defaults == null ? Collections.emptyMap() : defaults);
    }

    static HiveWriteMetadataSnapshot loadFresh(HmsClient client, String dbName, String tableName) {
        HmsTableInfo table = client.getTableFresh(dbName, tableName);
        Map<String, String> defaults;
        try {
            defaults = client.getDefaultColumnValues(dbName, tableName);
        } catch (HmsClientException e) {
            // Some Hive versions cannot expose default constraints; match binding's empty-default fallback.
            defaults = Collections.emptyMap();
        }
        return of(table, defaults);
    }

    HmsTableInfo getTable() {
        return table;
    }

    List<ConnectorColumn> getDataColumns() {
        return dataColumns;
    }

    List<ConnectorColumn> getPartitionColumns() {
        return partitionColumns;
    }

    String getIdentity() {
        return identity;
    }

    private static List<ConnectorColumn> buildDataColumns(
            HmsTableInfo table, Map<String, String> defaults) {
        List<ConnectorColumn> raw = table.getColumns();
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }
        boolean openCsv = !isView(table)
                && HiveTextProperties.HIVE_OPEN_CSV_SERDE.equals(table.getSerializationLib());
        ConnectorType stringType = ConnectorType.of("STRING");
        List<ConnectorColumn> effective = new ArrayList<>(raw.size());
        for (ConnectorColumn column : raw) {
            String defaultValue = column.getDefaultValue();
            if (defaultValue == null) {
                defaultValue = defaults.get(column.getName());
            }
            ConnectorType type = openCsv ? stringType : column.getType();
            if (type == column.getType() && Objects.equals(defaultValue, column.getDefaultValue())) {
                effective.add(column);
            } else {
                effective.add(copyColumn(column, type, defaultValue));
            }
        }
        return effective;
    }

    private static List<ConnectorColumn> buildPartitionColumns(HmsTableInfo table) {
        List<ConnectorColumn> raw = table.getPartitionKeys();
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }
        List<ConnectorColumn> effective = new ArrayList<>(raw.size());
        for (ConnectorColumn column : raw) {
            if ("STRING".equals(column.getType().getTypeName())) {
                effective.add(copyColumn(column,
                        ConnectorType.of("VARCHAR", MAX_VARCHAR_LENGTH, -1), column.getDefaultValue()));
            } else {
                effective.add(column);
            }
        }
        return effective;
    }

    private static ConnectorColumn copyColumn(
            ConnectorColumn column, ConnectorType type, String defaultValue) {
        return new ConnectorColumn(column.getName(), type, column.getComment(), column.isNullable(),
                defaultValue, column.isKey(), column.isAutoInc(), column.isAggregated());
    }

    private static boolean isView(HmsTableInfo table) {
        return table.getViewOriginalText() != null || table.getViewExpandedText() != null
                || "VIRTUAL_VIEW".equalsIgnoreCase(table.getTableType());
    }

    private String buildIdentity() {
        MetadataDigest digest = new MetadataDigest();
        digest.add("hive-write-metadata-v2");
        digest.add(table.getDbName());
        digest.add(table.getTableName());
        // HMS has no portable UUID; createTime plus owner is its stable incarnation signal across alterations.
        digest.add(table.getCreateTime());
        digest.add(table.getOwner());
        digest.add(table.getTableType());
        digest.add(table.getLocation());
        digest.add(table.getInputFormat());
        digest.add(table.getOutputFormat());
        digest.add(table.getSerializationLib());
        addMap(digest, table.getParameters());
        addMap(digest, table.getSdParameters());
        addStrings(digest, table.getBucketCols());
        digest.add(table.getNumBuckets());
        addColumns(digest, "data", dataColumns);
        addColumns(digest, "partition", partitionColumns);
        return digest.finish();
    }

    private static void addColumns(
            MetadataDigest digest, String role, List<ConnectorColumn> columns) {
        digest.add(role);
        digest.add(columns.size());
        for (ConnectorColumn column : columns) {
            digest.add(column.getName().toLowerCase(Locale.ROOT));
            digest.add(column.isNullable());
            digest.add(column.getDefaultValue());
            addType(digest, column.getType());
        }
    }

    private static void addType(MetadataDigest digest, ConnectorType type) {
        digest.add(type.getTypeName().toUpperCase(Locale.ROOT));
        digest.add(type.getPrecision());
        digest.add(type.getScale());
        List<ConnectorType> children = type.getChildren();
        digest.add(children.size());
        for (int i = 0; i < children.size(); i++) {
            digest.add(i < type.getFieldNames().size()
                    ? type.getFieldNames().get(i).toLowerCase(Locale.ROOT) : null);
            digest.add(type.isChildNullable(i));
            addType(digest, children.get(i));
        }
    }

    private static void addMap(MetadataDigest digest, Map<String, String> values) {
        Map<String, String> ordered = values == null
                ? Collections.emptyMap() : new TreeMap<>(values);
        digest.add(ordered.size());
        for (Map.Entry<String, String> entry : ordered.entrySet()) {
            digest.add(entry.getKey());
            digest.add(entry.getValue());
        }
    }

    private static void addStrings(MetadataDigest digest, List<String> values) {
        List<String> safe = values == null ? Collections.emptyList() : values;
        digest.add(safe.size());
        for (String value : safe) {
            digest.add(value);
        }
    }

    private static final class MetadataDigest {
        private final MessageDigest digest;

        private MetadataDigest() {
            try {
                digest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("SHA-256 is unavailable", e);
            }
        }

        private void add(Object value) {
            if (value == null) {
                digest.update((byte) 0);
                return;
            }
            byte[] bytes = String.valueOf(value).getBytes(StandardCharsets.UTF_8);
            digest.update((byte) 1);
            digest.update((byte) (bytes.length >>> 24));
            digest.update((byte) (bytes.length >>> 16));
            digest.update((byte) (bytes.length >>> 8));
            digest.update((byte) bytes.length);
            digest.update(bytes);
        }

        private String finish() {
            try (Formatter formatter = new Formatter(Locale.ROOT)) {
                for (byte value : digest.digest()) {
                    formatter.format("%02x", value);
                }
                return formatter.toString();
            }
        }
    }
}
