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

package org.apache.doris.connector.api;

import java.util.Objects;

/**
 * Describes a single column in a connector table.
 */
public final class ConnectorColumn {

    private final String name;
    private final ConnectorType type;
    private final String comment;
    private final boolean nullable;
    private final String defaultValue;
    private final boolean isKey;
    private final boolean isAutoInc;
    private final boolean isAggregated;
    // Marks a "with local time zone" timestamp column. fe-core's ConnectorColumnConverter translates
    // this into Column.setWithTZExtraInfo() so DESC shows the WITH_TIMEZONE "Extra" marker, matching
    // legacy PaimonExternalTable/PaimonSysExternalTable/IcebergUtils which set it from the SOURCE type
    // root regardless of the timestamp_tz mapping flag. Defaults false; set via withTimeZone().
    private final boolean withTimeZone;
    // Marks a hidden (non-visible) column. fe-core's ConnectorColumnConverter translates this into
    // Column.setIsVisible(false). Used by synthetic write columns a connector declares through the schema
    // SPI (e.g. iceberg's __DORIS_ICEBERG_ROWID_COL__ / v3 row-lineage), which must stay hidden. Defaults
    // true (visible); set via invisible().
    private final boolean visible;

    public ConnectorColumn(String name, ConnectorType type, String comment,
            boolean nullable, String defaultValue) {
        this(name, type, comment, nullable, defaultValue, false);
    }

    public ConnectorColumn(String name, ConnectorType type, String comment,
            boolean nullable, String defaultValue, boolean isKey) {
        this(name, type, comment, nullable, defaultValue, isKey, false);
    }

    public ConnectorColumn(String name, ConnectorType type, String comment,
            boolean nullable, String defaultValue, boolean isKey, boolean isAutoInc) {
        this(name, type, comment, nullable, defaultValue, isKey, isAutoInc, false);
    }

    public ConnectorColumn(String name, ConnectorType type, String comment,
            boolean nullable, String defaultValue, boolean isKey, boolean isAutoInc,
            boolean isAggregated) {
        this(name, type, comment, nullable, defaultValue, isKey, isAutoInc, isAggregated, false, true);
    }

    private ConnectorColumn(String name, ConnectorType type, String comment,
            boolean nullable, String defaultValue, boolean isKey, boolean isAutoInc,
            boolean isAggregated, boolean withTimeZone, boolean visible) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.comment = comment;
        this.nullable = nullable;
        this.defaultValue = defaultValue;
        this.isKey = isKey;
        this.isAutoInc = isAutoInc;
        this.isAggregated = isAggregated;
        this.withTimeZone = withTimeZone;
        this.visible = visible;
    }

    /**
     * Returns a copy of this column marked as a "with local time zone" timestamp. See
     * {@link #isWithTimeZone()}; the marker is intentionally orthogonal to the mapped {@link #getType()}
     * so it survives even when the column is mapped to a plain DATETIME (timestamp_tz mapping off).
     */
    public ConnectorColumn withTimeZone() {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, true, visible);
    }

    /**
     * Returns a copy of this column marked hidden (non-visible). See {@link #isVisible()}; used to declare
     * synthetic write columns through the schema SPI so the converter re-applies {@code setIsVisible(false)}.
     */
    public ConnectorColumn invisible() {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, withTimeZone, false);
    }

    public String getName() {
        return name;
    }

    public ConnectorType getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    public boolean isNullable() {
        return nullable;
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public boolean isKey() {
        return isKey;
    }

    public boolean isAutoInc() {
        return isAutoInc;
    }

    public boolean isAggregated() {
        return isAggregated;
    }

    public boolean isWithTimeZone() {
        return withTimeZone;
    }

    public boolean isVisible() {
        return visible;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorColumn)) {
            return false;
        }
        ConnectorColumn that = (ConnectorColumn) o;
        return nullable == that.nullable
                && isKey == that.isKey
                && isAutoInc == that.isAutoInc
                && isAggregated == that.isAggregated
                && withTimeZone == that.withTimeZone
                && visible == that.visible
                && name.equals(that.name)
                && type.equals(that.type)
                && Objects.equals(comment, that.comment)
                && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment, nullable, defaultValue, isKey, isAutoInc, isAggregated,
                withTimeZone, visible);
    }

    @Override
    public String toString() {
        return name + " " + type + (nullable ? " NULL" : " NOT NULL");
    }
}
