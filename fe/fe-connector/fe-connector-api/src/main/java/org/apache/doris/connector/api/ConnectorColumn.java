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

    /** Sentinel for "no reserved field id declared"; mirrors Doris Column's default uniqueId. */
    public static final int UNSET_UNIQUE_ID = -1;

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
    // Reserved Doris field id (Column uniqueId). fe-core's ConnectorColumnConverter re-applies it via
    // Column.setUniqueId() only when set (>= 0). Used by synthetic write columns whose Doris column identity
    // must equal a connector-reserved field id (e.g. iceberg v3 row-lineage _row_id=2147483540 /
    // _last_updated_sequence_number=2147483539, matched by field id BE-side). Defaults UNSET_UNIQUE_ID (-1),
    // leaving the Doris default untouched; set via withUniqueId().
    private final int uniqueId;
    // Marks a connector-reserved passthrough column: a synthetic column whose identity the ENGINE must
    // recognize generically (without knowing the connector's column names) — e.g. iceberg v3 row-lineage
    // (_row_id / _last_updated_sequence_number), which fe-core MERGE/UPDATE must pass through rather than treat
    // as user data. fe-core's ConnectorColumnConverter re-applies it via Column.setReservedPassthrough(true);
    // engine consumers then ask Column.isReservedPassthrough() instead of string-matching source column names.
    // Defaults false; set via reservedPassthrough().
    private final boolean reservedPassthrough;
    // #65329 "omit-preserves-metadata" markers for MODIFY COLUMN: whether the DDL explicitly stated a
    // nullability / a comment (as opposed to omitting it). fe-core's ConnectorColumnConverter.toConnectorColumn
    // populates these from the fe-catalog Column.isNullableSpecified()/isCommentSpecified(); the iceberg nested
    // MODIFY path reads them so an omitted nullability never widens a field and an omitted comment keeps the
    // field's current doc. Default false (unspecified); set via withSpecified(). Intentionally NOT part of
    // equals()/hashCode() — they are DDL-intent hints, not column identity, and were absent historically, so
    // excluding them keeps every pre-existing equality unchanged.
    private final boolean nullableSpecified;
    private final boolean commentSpecified;

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
        this(name, type, comment, nullable, defaultValue, isKey, isAutoInc, isAggregated, false, true,
                UNSET_UNIQUE_ID, false, false, false);
    }

    private ConnectorColumn(String name, ConnectorType type, String comment,
            boolean nullable, String defaultValue, boolean isKey, boolean isAutoInc,
            boolean isAggregated, boolean withTimeZone, boolean visible, int uniqueId,
            boolean reservedPassthrough, boolean nullableSpecified, boolean commentSpecified) {
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
        this.uniqueId = uniqueId;
        this.reservedPassthrough = reservedPassthrough;
        this.nullableSpecified = nullableSpecified;
        this.commentSpecified = commentSpecified;
    }

    /**
     * Returns a copy of this column marked as a "with local time zone" timestamp. See
     * {@link #isWithTimeZone()}; the marker is intentionally orthogonal to the mapped {@link #getType()}
     * so it survives even when the column is mapped to a plain DATETIME (timestamp_tz mapping off).
     */
    public ConnectorColumn withTimeZone() {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, true, visible, uniqueId, reservedPassthrough,
                nullableSpecified, commentSpecified);
    }

    /**
     * Returns a copy of this column marked hidden (non-visible). See {@link #isVisible()}; used to declare
     * synthetic write columns through the schema SPI so the converter re-applies {@code setIsVisible(false)}.
     */
    public ConnectorColumn invisible() {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, withTimeZone, false, uniqueId, reservedPassthrough,
                nullableSpecified, commentSpecified);
    }

    /**
     * Returns a copy of this column marked as a connector-reserved passthrough column. See
     * {@link #isReservedPassthrough()}; the converter re-applies it via {@code Column.setReservedPassthrough(true)}
     * so the engine can recognize a synthetic column (iceberg v3 row-lineage) generically, without knowing its
     * source name.
     */
    public ConnectorColumn reservedPassthrough() {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, withTimeZone, visible, uniqueId, true,
                nullableSpecified, commentSpecified);
    }

    /**
     * Returns a copy of this column carrying the #65329 nullability/comment "specified" markers. See
     * {@link #isNullableSpecified()} / {@link #isCommentSpecified()}; used by
     * {@code ConnectorColumnConverter.toConnectorColumn} to thread the fe-catalog Column flags across the SPI.
     */
    public ConnectorColumn withSpecified(boolean nullableSpecified, boolean commentSpecified) {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, withTimeZone, visible, uniqueId, reservedPassthrough,
                nullableSpecified, commentSpecified);
    }

    /**
     * Returns a copy of this column carrying the given reserved field id. See {@link #getUniqueId()}; the
     * converter re-applies it via {@code Column.setUniqueId()} only when set (&gt;= 0). Used to declare
     * synthetic write columns whose Doris column identity must equal a connector-reserved field id
     * (iceberg v3 row-lineage).
     */
    public ConnectorColumn withUniqueId(int uniqueId) {
        return new ConnectorColumn(name, type, comment, nullable, defaultValue,
                isKey, isAutoInc, isAggregated, withTimeZone, visible, uniqueId, reservedPassthrough,
                nullableSpecified, commentSpecified);
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

    public int getUniqueId() {
        return uniqueId;
    }

    public boolean isReservedPassthrough() {
        return reservedPassthrough;
    }

    /** Whether the DDL explicitly stated a nullability (#65329 MODIFY COLUMN omit-preserves). */
    public boolean isNullableSpecified() {
        return nullableSpecified;
    }

    /** Whether the DDL explicitly stated a comment (#65329 MODIFY COLUMN omit-preserves). */
    public boolean isCommentSpecified() {
        return commentSpecified;
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
                && uniqueId == that.uniqueId
                && reservedPassthrough == that.reservedPassthrough
                && name.equals(that.name)
                && type.equals(that.type)
                && Objects.equals(comment, that.comment)
                && Objects.equals(defaultValue, that.defaultValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment, nullable, defaultValue, isKey, isAutoInc, isAggregated,
                withTimeZone, visible, uniqueId, reservedPassthrough);
    }

    @Override
    public String toString() {
        return name + " " + type + (nullable ? " NULL" : " NOT NULL");
    }
}
