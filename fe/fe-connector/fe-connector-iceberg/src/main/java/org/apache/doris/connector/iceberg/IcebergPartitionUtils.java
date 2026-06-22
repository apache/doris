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

package org.apache.doris.connector.iceberg;

import com.google.common.base.Preconditions;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.JsonUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Self-contained port of the legacy fe-core {@code IcebergUtils} partition helpers used by the scan path
 * (P6.2-T03). The connector cannot import fe-core, so {@code getIdentityPartitionColumns} /
 * {@code getIdentityPartitionInfoMap} / {@code getPartitionValues} / {@code getPartitionDataJson} /
 * {@code serializePartitionValue} are reproduced byte-faithfully against the iceberg SDK with two
 * deliberate, documented deltas:
 *
 * <ul>
 *   <li>The timezone argument is a resolved {@link ZoneId} (instead of legacy's raw {@code String} +
 *       {@code ZoneId.of(tz)}), so a non-canonical Doris session {@code time_zone} (e.g. {@code "CST"})
 *       cannot crash partition-timestamp rendering — consistent with the T02 alias-map fix
 *       ({@code IcebergScanPlanProvider.resolveSessionZone}).</li>
 *   <li>{@code getPartitionDataJson} renders the JSON array via iceberg's bundled Jackson
 *       ({@link JsonUtil#mapper()}) rather than fe-core {@code GsonUtils.GSON}. BE re-parses the JSON
 *       array back to {@code List<String>}, so the value content is identical; only the serializer differs.</li>
 * </ul>
 */
final class IcebergPartitionUtils {

    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);

    private IcebergPartitionUtils() {
    }

    /**
     * Ordered, lowercased, de-duplicated list of the identity partition column names across <b>all</b>
     * partition specs of the table (mirrors legacy {@code IcebergUtils.getIdentityPartitionColumns}). This
     * is the {@code path_partition_keys} payload: it tells FE which slots are partition columns so they are
     * excluded from the file-decode set (the CI #968880 double-fill guard). Non-identity transforms
     * (bucket/truncate/year/month/...) are excluded.
     */
    static List<String> getIdentityPartitionColumns(Table table) {
        LinkedHashSet<String> partitionColumns = new LinkedHashSet<>();
        for (PartitionSpec spec : table.specs().values()) {
            for (PartitionField partitionField : spec.fields()) {
                if (!partitionField.transform().isIdentity()) {
                    continue;
                }
                String columnName = table.schema().findColumnName(partitionField.sourceId());
                if (columnName != null) {
                    partitionColumns.add(columnName.toLowerCase(java.util.Locale.ROOT));
                }
            }
        }
        return new ArrayList<>(partitionColumns);
    }

    /**
     * Per-file map of identity partition column (lowercased) to serialized value, skipping non-identity
     * transforms and BINARY/FIXED columns (utf8 round-trip would corrupt those). Order-preserving
     * (LinkedHashMap, spec field order). Mirrors legacy {@code IcebergUtils.getIdentityPartitionInfoMap}.
     */
    static Map<String, String> getIdentityPartitionInfoMap(PartitionData partitionData,
            PartitionSpec partitionSpec, Table table, ZoneId zone) {
        Map<String, String> partitionInfoMap = new LinkedHashMap<>();
        List<NestedField> fields = partitionData.getPartitionType().asNestedType().fields();
        List<PartitionField> partitionFields = partitionSpec.fields();
        Preconditions.checkArgument(fields.size() == partitionFields.size(),
                "PartitionData fields size does not match PartitionSpec fields size");

        for (int i = 0; i < fields.size(); i++) {
            NestedField field = fields.get(i);
            PartitionField partitionField = partitionFields.get(i);
            if (!partitionField.transform().isIdentity()) {
                continue;
            }
            TypeID partitionTypeId = field.type().typeId();
            if (partitionTypeId == TypeID.BINARY || partitionTypeId == TypeID.FIXED) {
                continue;
            }

            String columnName = table.schema().findColumnName(partitionField.sourceId());
            if (columnName == null) {
                continue;
            }
            Object value = partitionData.get(i);
            try {
                partitionInfoMap.put(columnName.toLowerCase(java.util.Locale.ROOT),
                        serializePartitionValue(field.type(), value, zone));
            } catch (UnsupportedOperationException e) {
                LOG.warn("Failed to serialize Iceberg table partition value for field {}: {}", field.name(),
                        e.getMessage());
            }
        }
        return partitionInfoMap;
    }

    /**
     * The serialized value for <b>every</b> partition field (identity + transform), in spec order, used for
     * {@code partition_data_json}. Mirrors legacy {@code IcebergUtils.getPartitionValues}. A field whose
     * value cannot be serialized (BINARY/FIXED) yields a {@code null} entry (not dropped) to keep positional
     * alignment with the spec.
     */
    static List<String> getPartitionValues(PartitionData partitionData, PartitionSpec partitionSpec, ZoneId zone) {
        List<NestedField> fields = partitionData.getPartitionType().asNestedType().fields();
        Preconditions.checkArgument(fields.size() == partitionSpec.fields().size(),
                "PartitionData fields size does not match PartitionSpec fields size");

        List<String> partitionValues = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            NestedField field = fields.get(i);
            Object value = partitionData.get(i);
            try {
                partitionValues.add(serializePartitionValue(field.type(), value, zone));
            } catch (UnsupportedOperationException e) {
                LOG.warn("Failed to serialize Iceberg partition value for field {}: {}", field.name(),
                        e.getMessage());
                partitionValues.add(null);
            }
        }
        return partitionValues;
    }

    /**
     * The {@code partition_data_json} string: a JSON array of the serialized partition values. Rendered via
     * iceberg's bundled Jackson (see class javadoc) instead of fe-core Gson — BE re-parses it, so the value
     * content is identical.
     */
    static String getPartitionDataJson(PartitionData partitionData, PartitionSpec partitionSpec, ZoneId zone) {
        List<String> partitionValues = getPartitionValues(partitionData, partitionSpec, zone);
        try {
            return JsonUtil.mapper().writeValueAsString(partitionValues);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize iceberg partition data to JSON, error message is:"
                    + e.getMessage(), e);
        }
    }

    /**
     * Faithful port of legacy {@code IcebergUtils.serializePartitionValue}: render a single partition value
     * to its string form, dispatching on the iceberg {@link Type}. {@code null} values pass through as
     * {@code null}; BINARY/FIXED throw {@link UnsupportedOperationException} (a utf8 round-trip would corrupt
     * the bytes). Package-private for direct parity testing.
     */
    static String serializePartitionValue(Type type, Object value, ZoneId zone) {
        switch (type.typeId()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case STRING:
            case UUID:
            case DECIMAL:
                if (value == null) {
                    return null;
                }
                return value.toString();
            case FLOAT:
                if (value == null) {
                    return null;
                }
                return Float.toString((Float) value);
            case DOUBLE:
                if (value == null) {
                    return null;
                }
                return Double.toString((Double) value);
            // BINARY / FIXED are intentionally unsupported: returning a utf8 string may corrupt the data.
            case DATE:
                if (value == null) {
                    return null;
                }
                // Iceberg date is stored as days since epoch (1970-01-01).
                return LocalDate.ofEpochDay((Integer) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIME:
                if (value == null) {
                    return null;
                }
                // Iceberg time is stored as microseconds since midnight.
                long micros = (Long) value;
                return LocalTime.ofNanoOfDay(micros * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP:
                if (value == null) {
                    return null;
                }
                // Iceberg timestamp is stored as microseconds since epoch (1970-01-01T00:00:00).
                long timestampMicros = (Long) value;
                LocalDateTime timestamp = LocalDateTime.ofEpochSecond(
                        timestampMicros / 1_000_000, (int) (timestampMicros % 1_000_000) * 1000, ZoneOffset.UTC);
                // timestamptz when shouldAdjustToUTC() — render the stored UTC instant in the session zone.
                if (((TimestampType) type).shouldAdjustToUTC()) {
                    timestamp = timestamp.atZone(ZoneOffset.UTC).withZoneSameInstant(zone).toLocalDateTime();
                }
                return timestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            default:
                throw new UnsupportedOperationException("Unsupported type for serializePartitionValue: " + type);
        }
    }
}
