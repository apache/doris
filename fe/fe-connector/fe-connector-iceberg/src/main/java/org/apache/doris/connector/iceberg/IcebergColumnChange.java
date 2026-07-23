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

import org.apache.doris.connector.api.ConnectorType;

import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.types.Type;

import java.util.Objects;

/**
 * The already-built iceberg artifacts for a single {@code ADD COLUMN} / {@code MODIFY COLUMN}, passed from
 * {@link IcebergConnectorMetadata} to the {@link IcebergCatalogOps} seam.
 *
 * <p>Mirrors how B1's {@code createTable} hands the seam a fully-built iceberg {@code Schema}/{@code SortOrder}:
 * the metadata layer turns the neutral {@code ConnectorColumn} into an iceberg {@link Type} (+ parsed default
 * {@link Literal}) PURELY, outside the auth context, so the seam stays a thin delegation that only does the
 * remote {@code UpdateSchema} commit.</p>
 *
 * <p>{@code ADD COLUMN} uses every field; {@code MODIFY COLUMN} (scalar) uses {@code name}/{@code type}/
 * {@code comment}/{@code nullable} and ignores {@code defaultValue}.</p>
 */
public final class IcebergColumnChange {

    private final String name;
    private final Type type;
    private final String comment;
    // The parsed iceberg default literal, or null when no DEFAULT clause / not applicable (MODIFY).
    private final Literal<?> defaultValue;
    private final boolean nullable;
    // The neutral type this iceberg {@code type} was built from, carried ONLY for a complex-type MODIFY so the
    // field-by-field diff ({@link IcebergComplexTypeDiff}) can read each STRUCT field's commentSpecified flag
    // (absent from the iceberg type, which stores only a doc string). Null for ADD and scalar MODIFY, which do
    // not diff.
    private final ConnectorType sourceType;

    public IcebergColumnChange(String name, Type type, String comment, Literal<?> defaultValue, boolean nullable) {
        this(name, type, comment, defaultValue, nullable, null);
    }

    public IcebergColumnChange(String name, Type type, String comment, Literal<?> defaultValue, boolean nullable,
            ConnectorType sourceType) {
        this.name = Objects.requireNonNull(name, "name");
        this.type = Objects.requireNonNull(type, "type");
        this.comment = comment;
        this.defaultValue = defaultValue;
        this.nullable = nullable;
        this.sourceType = sourceType;
    }

    public String getName() {
        return name;
    }

    public Type getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    public Literal<?> getDefaultValue() {
        return defaultValue;
    }

    public boolean isNullable() {
        return nullable;
    }

    /** The neutral source type (for a complex MODIFY diff), or null for ADD / scalar MODIFY. */
    public ConnectorType getSourceType() {
        return sourceType;
    }

    @Override
    public String toString() {
        return name + " " + type + (nullable ? " NULL" : " NOT NULL")
                + (comment == null ? "" : " COMMENT '" + comment + "'");
    }
}
