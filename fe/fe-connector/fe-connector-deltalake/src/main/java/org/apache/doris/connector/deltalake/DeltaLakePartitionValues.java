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

package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import java.math.BigDecimal;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/** Log partition values, in declaration order, with genuine nulls kept distinct from string values. */
final class DeltaLakePartitionValues {
    private static final String PHYSICAL_NAME = "delta.columnMapping.physicalName";

    private DeltaLakePartitionValues() {
    }

    static String partitionName(Map<String, String> values) {
        return values.entrySet().stream().map(entry -> escape(entry.getKey()) + "="
                + (entry.getValue() == null ? ConnectorPartitionValues.NULL_PARTITION_NAME
                        : escapeValue(entry.getValue()))).collect(Collectors.joining("/"));
    }

    private static String escapeValue(String value) {
        // A real string equal to the null marker must keep a different partition identity.
        return ConnectorPartitionValues.NULL_PARTITION_NAME.equals(value)
                ? "%5F" + value.substring(1) : escape(value);
    }

    private static String escape(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    static ZoneId sessionZone(ConnectorSession session) {
        return session == null ? ZoneOffset.UTC : ZoneId.of(session.getTimeZone());
    }

    static Map<String, String> normalize(Snapshot snapshot, Map<String, String> rawValues, ZoneId zone) {
        boolean mapped = DeltaLakeSchemaBuilder.hasColumnMapping(snapshot);
        return normalize(snapshot.getSchema(), snapshot.getPartitionColumnNames(), mapped, rawValues, zone,
                name -> { });
    }

    static Map<String, String> normalize(StructType schema, List<String> partitions,
            boolean mapped, Map<String, String> rawValues, ZoneId zone, Consumer<String> requireNormalizedFilter) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String name : partitions) {
            StructField field = schema.get(name);
            String key = logKey(field, mapped);
            if (!rawValues.containsKey(key)) {
                throw new DorisConnectorException("Delta file has no partition value for column " + name);
            }
            try {
                result.put(name, normalizeValue(field, rawValues.get(key), zone, requireNormalizedFilter));
            } catch (IllegalArgumentException | DateTimeException e) {
                throw new DorisConnectorException("Invalid Delta partition value for column " + name
                        + " of type " + field.getDataType(), e);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    static String logKey(StructField field, boolean mapped) {
        return mapped ? field.getMetadata().getString(PHYSICAL_NAME) : field.getName();
    }

    static boolean emptyStringIsNull(DataType type) {
        return type instanceof BooleanType || type instanceof ByteType || type instanceof ShortType
                || type instanceof IntegerType || type instanceof LongType || type instanceof FloatType
                || type instanceof DoubleType || type instanceof DecimalType || type instanceof DateType
                || type instanceof TimestampType || type instanceof TimestampNTZType;
    }

    static String normalizeValue(StructField field, String value, ZoneId zone,
            Consumer<String> requireNormalizedFilter) {
        DataType type = field.getDataType();
        if (value == null || type instanceof StringType || type instanceof BinaryType) {
            // Delta serializes binary partition values as UTF-8 strings, not base64. Do not
            // path-decode either type: percent signs, slashes, empty strings and sentinels are data.
            return value;
        }
        if (value.isEmpty() && emptyStringIsNull(type)) {
            requireNormalizedFilter.accept(field.getName());
            return null;
        }
        if (type instanceof TimestampType) {
            TemporalAccessor parsed = DateTimeFormatter.ISO_DATE_TIME.parseBest(value.replace(' ', 'T'),
                    OffsetDateTime::from, LocalDateTime::from);
            OffsetDateTime timestamp;
            if (parsed instanceof OffsetDateTime) {
                timestamp = (OffsetDateTime) parsed;
            } else {
                // Spark interprets legacy wall-clock strings in the reader's session zone. Kernel uses UTC.
                requireNormalizedFilter.accept(field.getName());
                timestamp = ((LocalDateTime) parsed).atZone(zone).toOffsetDateTime();
            }
            return timestamp.withOffsetSameInstant(ZoneOffset.UTC).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        if (type instanceof TimestampNTZType) {
            if (value.indexOf(':') < 0
                    && field.getTypeChanges().contains(DeltaLakeFeatureCheck.DATE_TO_TIMESTAMP_NTZ)) {
                // Old AddFiles keep their DATE partition string after widening. NTZ midnight has no time zone.
                return LocalDate.parse(value).atStartOfDay().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            }
            return LocalDateTime.parse(value.replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                    .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        }
        if (type instanceof DateType) {
            return LocalDate.parse(value).toString();
        }
        if (type instanceof BooleanType && !"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("Expected a boolean partition value");
        }
        Object literal = DeltaKernelCompat.partitionLiteral(type, value);
        return literal instanceof BigDecimal ? ((BigDecimal) literal).toPlainString() : literal.toString();
    }
}
