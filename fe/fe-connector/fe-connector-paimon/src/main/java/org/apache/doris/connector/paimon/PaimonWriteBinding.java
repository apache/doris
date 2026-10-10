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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorWriteHandle;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.utils.InstantiationUtil;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Statement-scoped Paimon write target shared by sink planning and transaction commit. */
final class PaimonWriteBinding {

    private final String tableName;
    private final FileStoreTable table;
    private final String serializedTable;
    private final Map<String, String> hadoopConfig;
    private final boolean overwrite;
    private final Map<String, String> staticPartition;

    private PaimonWriteBinding(String tableName, FileStoreTable table,
            Map<String, String> hadoopConfig, boolean overwrite,
            Map<String, String> staticPartition) {
        this.tableName = tableName;
        this.table = table;
        // The FE committer keeps the catalog-aware table, but the BE writer must not deserialize
        // an HMS/DLF catalog loader whose metastore classes are absent from the BE plugin.
        this.serializedTable = serialize(PaimonScanPlanProvider.dropCatalogLoader(table));
        this.hadoopConfig = Collections.unmodifiableMap(new LinkedHashMap<>(hadoopConfig));
        this.overwrite = overwrite;
        this.staticPartition = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartition));
    }

    static PaimonWriteBinding create(PaimonTableHandle handle, FileStoreTable table,
            Map<String, String> hadoopConfig, ConnectorWriteHandle writeHandle, String sessionTimeZone) {
        Map<String, String> staticPartition = resolveStaticPartition(table, writeHandle, sessionTimeZone);
        FileStoreTable writeTable = configureTableForWrite(table, writeHandle.isOverwrite(), staticPartition);
        return new PaimonWriteBinding(handle.getDatabaseName() + "." + handle.getTableName(),
                writeTable, hadoopConfig, writeHandle.isOverwrite(), staticPartition);
    }

    static FileStoreTable configureTableForWrite(FileStoreTable table, boolean overwrite,
            Map<String, String> staticPartition) {
        if (!overwrite) {
            return table;
        }
        String dynamicOverwriteKey = CoreOptions.DYNAMIC_PARTITION_OVERWRITE.key();
        boolean explicitlyDynamic = staticPartition.isEmpty()
                && Boolean.parseBoolean(table.options().get(dynamicOverwriteKey));
        if (explicitlyDynamic) {
            return table;
        }
        return table.copy(Collections.singletonMap(dynamicOverwriteKey, Boolean.FALSE.toString()));
    }

    /**
     * The static partition as Paimon's static overwrite parses it: SQL NULL becomes the table's
     * {@code partition.default-name}, and every other value is the one the written rows carry, cast to the
     * column type.
     */
    static Map<String, String> resolveStaticPartition(FileStoreTable table, ConnectorWriteHandle writeHandle,
            String sessionTimeZone) {
        Map<String, String> canonicalNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String partitionKey : table.partitionKeys()) {
            canonicalNames.put(partitionKey, partitionKey);
        }
        String defaultPartitionName = CoreOptions.fromMap(table.options()).partitionDefaultName();
        Map<String, String> castValues = writeHandle.getCastStaticPartitionSpec();
        Map<String, String> result = new LinkedHashMap<>();
        for (String key : writeHandle.getStaticPartitionSpec().keySet()) {
            String canonicalName = canonicalNames.get(key);
            if (canonicalName == null) {
                throw new DorisConnectorException("Column '" + key
                        + "' is not a partition column of Paimon table");
            }
            if (writeHandle.getStaticPartitionNullKeys().contains(key)) {
                result.put(canonicalName, defaultPartitionName);
                continue;
            }
            String value = Objects.requireNonNull(castValues.get(key),
                    () -> "missing the cast value of static partition column " + key);
            if (table.rowType().getField(canonicalName).type().getTypeRoot()
                    == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                value = toSdkLocalTime(value, isTimestampTz(writeHandle, canonicalName), sessionTimeZone);
            }
            if (writeHandle.isOverwrite() && defaultPartitionName.equals(value)) {
                // Paimon's static overwrite reads this string as the NULL partition of any partition type, so
                // a value equal to it would overwrite the NULL partition instead.
                throw new DorisConnectorException("Static partition value for column '" + canonicalName
                        + "' equals Paimon partition.default-name '" + defaultPartitionName
                        + "' and cannot be represented in a static overwrite");
            }
            result.put(canonicalName, value);
        }
        return result;
    }

    private static boolean isTimestampTz(ConnectorWriteHandle writeHandle, String columnName) {
        for (ConnectorColumn column : writeHandle.getBoundTargetColumns()) {
            if (column.getName().equalsIgnoreCase(columnName)) {
                return "TIMESTAMPTZ".equalsIgnoreCase(column.getType().getTypeName());
            }
        }
        throw new DorisConnectorException("Paimon partition column is missing from the write schema: "
                + columnName);
    }

    /**
     * Paimon parses a static overwrite value of a TIMESTAMP WITH LOCAL TIME ZONE column as local time in the FE
     * JVM's default zone, so the value is moved there from the zone it was written in: the session zone for a
     * DATETIMEV2 value, UTC for a TIMESTAMPTZ value, which carries its offset. Paimon's parser takes a space,
     * not ISO's 'T', between the date and the time.
     */
    private static String toSdkLocalTime(String value, boolean timestampTz, String sessionTimeZone) {
        String isoValue = value.replace(' ', 'T');
        ZonedDateTime instant = timestampTz
                ? OffsetDateTime.parse(isoValue, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toZonedDateTime()
                : LocalDateTime.parse(isoValue, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                        .atZone(ZoneId.of(sessionTimeZone, PaimonConnectorMetadata.SESSION_TIME_ZONE_ALIASES));
        return instant.withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime()
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .replace('T', ' ');
    }

    private static String serialize(FileStoreTable table) {
        try {
            return Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(table));
        } catch (IOException e) {
            throw new DorisConnectorException("Failed to serialize Paimon write table", e);
        }
    }

    String tableName() {
        return tableName;
    }

    FileStoreTable getTable() {
        return table;
    }

    String getSerializedTable() {
        return serializedTable;
    }

    Map<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    boolean isOverwrite() {
        return overwrite;
    }

    Map<String, String> getStaticPartition() {
        return staticPartition;
    }
}
