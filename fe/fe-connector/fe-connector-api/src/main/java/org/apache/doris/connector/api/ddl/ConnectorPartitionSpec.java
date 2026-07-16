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

package org.apache.doris.connector.api.ddl;

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
 *
 * <p>{@code initialValues} is only meaningful for {@code LIST} / {@code RANGE} styles.</p>
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
    private final List<ConnectorPartitionValueDef> initialValues;
    private final boolean hasExplicitPartitionValues;

    public ConnectorPartitionSpec(Style style,
            List<ConnectorPartitionField> fields,
            List<ConnectorPartitionValueDef> initialValues) {
        this(style, fields, initialValues, false);
    }

    public ConnectorPartitionSpec(Style style,
            List<ConnectorPartitionField> fields,
            List<ConnectorPartitionValueDef> initialValues,
            boolean hasExplicitPartitionValues) {
        this.style = Objects.requireNonNull(style, "style");
        this.fields = fields == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(fields);
        this.initialValues = initialValues == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(initialValues);
        this.hasExplicitPartitionValues = hasExplicitPartitionValues;
    }

    public Style getStyle() {
        return style;
    }

    public List<ConnectorPartitionField> getFields() {
        return fields;
    }

    public List<ConnectorPartitionValueDef> getInitialValues() {
        return initialValues;
    }

    /**
     * Whether the CREATE TABLE declared explicit partition value definitions (e.g.
     * {@code PARTITION BY LIST(dt) (PARTITION p1 VALUES IN ('a'))}). The neutral converter does not lower
     * those value expressions into {@link #getInitialValues()} (it stays empty), so this flag preserves the
     * information a connector needs to reject them: Hive external tables discover partitions from the data
     * layout and reject explicit partition values (legacy parity). Connectors that accept explicit partition
     * definitions ignore this flag.
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
                && fields.equals(that.fields)
                && initialValues.equals(that.initialValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(style, fields, initialValues, hasExplicitPartitionValues);
    }

    @Override
    public String toString() {
        return "ConnectorPartitionSpec{style=" + style
                + ", fields=" + fields
                + ", initialValues=" + initialValues.size()
                + ", hasExplicitPartitionValues=" + hasExplicitPartitionValues + "}";
    }
}
