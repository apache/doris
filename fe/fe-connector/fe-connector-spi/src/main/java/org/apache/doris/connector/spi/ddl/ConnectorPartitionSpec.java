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

package org.apache.doris.connector.spi.ddl;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Partition specification carried by {@link ConnectorCreateTableRequest}.
 *
 * <p>{@link Style} distinguishes the four supported partition flavors:</p>
 * <ul>
 *   <li>{@code IDENTITY} — Hive style: {@code PARTITIONED BY (col1, col2)}.</li>
 *   <li>{@code TRANSFORM} — Iceberg style: {@code PARTITIONED BY (bucket(16, c), year(d))}.</li>
 *   <li>{@code LIST} — Doris {@code PARTITION BY LIST} with explicit value definitions.</li>
 *   <li>{@code RANGE} — Doris {@code PARTITION BY RANGE} with [lower, upper) tuples.</li>
 * </ul>
 */
public final class ConnectorPartitionSpec {

    public enum Style {
        IDENTITY,
        TRANSFORM,
        LIST,
        RANGE,
    }

    private final Style style;
    private final List<ConnectorPartitionField> fields;
    private final boolean hasExplicitPartitionValues;

    public ConnectorPartitionSpec(Style style, List<ConnectorPartitionField> fields) {
        this(style, fields, false);
    }

    public ConnectorPartitionSpec(Style style,
            List<ConnectorPartitionField> fields,
            boolean hasExplicitPartitionValues) {
        this.style = Objects.requireNonNull(style, "style");
        this.fields = fields == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(fields);
        this.hasExplicitPartitionValues = hasExplicitPartitionValues;
    }

    public Style getStyle() {
        return style;
    }

    public List<ConnectorPartitionField> getFields() {
        return fields;
    }

    /**
     * Whether the CREATE TABLE declared explicit partition value definitions (e.g.
     * {@code PARTITION BY LIST(dt) (PARTITION p1 VALUES IN ('a'))}). The neutral converter does not carry
     * those value expressions across the SPI boundary at all, so this flag is the only thing that preserves
     * the information a connector needs to reject them: Hive external tables discover partitions from the
     * data layout and reject explicit partition values (legacy parity). Connectors that accept explicit
     * partition definitions ignore this flag.
     */
    public boolean hasExplicitPartitionValues() {
        return hasExplicitPartitionValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorPartitionSpec)) {
            return false;
        }
        ConnectorPartitionSpec that = (ConnectorPartitionSpec) o;
        return style == that.style
                && hasExplicitPartitionValues == that.hasExplicitPartitionValues
                && fields.equals(that.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(style, fields, hasExplicitPartitionValues);
    }

    @Override
    public String toString() {
        return "ConnectorPartitionSpec{style=" + style
                + ", fields=" + fields
                + ", hasExplicitPartitionValues=" + hasExplicitPartitionValues + "}";
    }
}
