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

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;

import io.delta.kernel.Snapshot;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
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
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    static Map<String, String> normalize(Snapshot snapshot, Map<String, String> rawValues) {
        boolean mapped = !"none".equals(snapshot.getTableProperties().getOrDefault("delta.columnMapping.mode", "none"));
        return normalize(snapshot.getSchema(), snapshot.getPartitionColumnNames(), mapped, rawValues);
    }

    static Map<String, String> normalize(StructType schema, List<String> partitions,
            boolean mapped, Map<String, String> rawValues) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String name : partitions) {
            StructField field = schema.get(name);
            String key = mapped ? field.getMetadata().getString(PHYSICAL_NAME) : name;
            if (!rawValues.containsKey(key)) {
                throw new DorisConnectorException("Delta file has no partition value for column " + name);
            }
            try {
                result.put(name, normalizeValue(field.getDataType(), rawValues.get(key)));
            } catch (IllegalArgumentException | DateTimeException e) {
                throw new DorisConnectorException("Invalid Delta partition value for column " + name
                        + " of type " + field.getDataType(), e);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    static String normalizeValue(DataType type, String value) {
        if (value == null || type instanceof StringType || type instanceof BinaryType) {
            // Delta serializes binary partition values as UTF-8 strings, not base64. Do not
            // path-decode either type: percent signs, slashes, empty strings and sentinels are data.
            return value;
        }
        if (type instanceof TimestampType) {
            // The protocol's space-separated timestamps also mean UTC. java.sql.Timestamp's
            // JVM-default-zone parsing can normalize a DST gap, so use java.time directly.
            TemporalAccessor parsed = DateTimeFormatter.ISO_DATE_TIME.parseBest(value.replace(' ', 'T'),
                    OffsetDateTime::from, LocalDateTime::from);
            OffsetDateTime instant = parsed instanceof OffsetDateTime
                    ? ((OffsetDateTime) parsed).withOffsetSameInstant(ZoneOffset.UTC)
                    : ((LocalDateTime) parsed).atOffset(ZoneOffset.UTC);
            return instant.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        if (type instanceof TimestampNTZType) {
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
