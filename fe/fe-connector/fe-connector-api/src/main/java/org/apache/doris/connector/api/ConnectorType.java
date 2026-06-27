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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a data type in the connector's own type system.
 *
 * <p>A type is identified by its name (e.g. "INT", "VARCHAR", "DECIMAL"),
 * optional precision/scale parameters, and optional child types for
 * complex types like ARRAY or MAP.</p>
 *
 * <p><b>Per-child nullability + comment</b> ({@link #isChildNullable(int)} /
 * {@link #getChildComment(int)}): for complex types the type optionally carries the nullability and
 * comment of each child (STRUCT field, ARRAY element, MAP value), parallel to {@link #getChildren()}.
 * These are <em>additive</em> — the legacy factories ({@link #of}/{@link #arrayOf(ConnectorType)}/
 * {@link #mapOf(ConnectorType, ConnectorType)}/{@link #structOf(List, List)}) leave them unset, in which
 * case every child defaults to nullable with no comment. They let a connector preserve a NOT NULL declared
 * inside a complex type (e.g. iceberg CREATE TABLE / MODIFY COLUMN of a STRUCT field) and the per-field
 * comments needed to diff a complex MODIFY. They are intentionally <b>excluded from {@link #equals(Object)}/
 * {@link #hashCode()}</b>: type identity stays the structural shape (name/precision/scale/children/field
 * names), matching the legacy Doris {@code Type} comparison that drives schema-change detection (nullability
 * and comment are compared separately, field-by-field, by the consumer) and keeping every existing
 * equality-based caller/test unaffected.</p>
 */
public final class ConnectorType {

    private final String typeName;
    private final int precision;
    private final int scale;
    private final List<ConnectorType> children;
    private final List<String> fieldNames;
    // Per-child nullability, parallel to children (STRUCT field / ARRAY element / MAP value). Empty (or
    // shorter than children) means "unset" -> the missing entries default to nullable. NOT part of equals().
    private final List<Boolean> childrenNullable;
    // Per-child comment, parallel to children. Empty / shorter than children means "unset" -> null comment.
    // Only STRUCT fields carry meaningful comments today; ARRAY element / MAP value are left null (legacy
    // parity: the complex-MODIFY diff drops element/value comments). NOT part of equals().
    private final List<String> childrenComments;

    public ConnectorType(String typeName) {
        this(typeName, -1, -1, Collections.emptyList(),
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale) {
        this(typeName, precision, scale, Collections.emptyList(),
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children) {
        this(typeName, precision, scale, children,
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children, List<String> fieldNames) {
        this(typeName, precision, scale, children, fieldNames,
                Collections.emptyList(), Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children, List<String> fieldNames,
            List<Boolean> childrenNullable, List<String> childrenComments) {
        this.typeName = Objects.requireNonNull(typeName, "typeName");
        this.precision = precision;
        this.scale = scale;
        this.children = children == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(children);
        this.fieldNames = fieldNames == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(fieldNames);
        this.childrenNullable = childrenNullable == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(childrenNullable);
        this.childrenComments = childrenComments == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(childrenComments);
    }

    /** Factory: simple type with no parameters. */
    public static ConnectorType of(String typeName) {
        return new ConnectorType(typeName);
    }

    /** Factory: type with precision and scale. */
    public static ConnectorType of(String typeName,
            int precision, int scale) {
        return new ConnectorType(typeName, precision, scale);
    }

    /** Factory: ARRAY type with element type (element defaults to nullable). */
    public static ConnectorType arrayOf(ConnectorType elementType) {
        return new ConnectorType("ARRAY", -1, -1,
                Collections.singletonList(elementType));
    }

    /** Factory: ARRAY type with element type and element nullability. */
    public static ConnectorType arrayOf(ConnectorType elementType, boolean elementNullable) {
        return new ConnectorType("ARRAY", -1, -1,
                Collections.singletonList(elementType), Collections.emptyList(),
                Collections.singletonList(elementNullable), Collections.emptyList());
    }

    /** Factory: MAP type with key and value types (value defaults to nullable; iceberg keys are required). */
    public static ConnectorType mapOf(ConnectorType keyType,
            ConnectorType valueType) {
        return new ConnectorType("MAP", -1, -1,
                Arrays.asList(keyType, valueType));
    }

    /**
     * Factory: MAP type with key/value types and value nullability. The key is reported as non-nullable
     * (iceberg / Doris map keys are always required); only the value nullability is meaningful.
     */
    public static ConnectorType mapOf(ConnectorType keyType,
            ConnectorType valueType, boolean valueNullable) {
        return new ConnectorType("MAP", -1, -1,
                Arrays.asList(keyType, valueType), Collections.emptyList(),
                Arrays.asList(false, valueNullable), Collections.emptyList());
    }

    /** Factory: STRUCT type with named fields (every field defaults to nullable, no comment). */
    public static ConnectorType structOf(List<String> names,
            List<ConnectorType> fieldTypes) {
        return new ConnectorType("STRUCT", -1, -1, fieldTypes, names);
    }

    /** Factory: STRUCT type with named fields plus per-field nullability and comments (parallel lists). */
    public static ConnectorType structOf(List<String> names,
            List<ConnectorType> fieldTypes, List<Boolean> fieldNullable, List<String> fieldComments) {
        return new ConnectorType("STRUCT", -1, -1, fieldTypes, names, fieldNullable, fieldComments);
    }

    public String getTypeName() {
        return typeName;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public List<ConnectorType> getChildren() {
        return children;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    /** The full per-child nullability list (may be empty / shorter than children when unset). */
    public List<Boolean> getChildrenNullable() {
        return childrenNullable;
    }

    /** The full per-child comment list (may be empty / shorter than children when unset). */
    public List<String> getChildrenComments() {
        return childrenComments;
    }

    /**
     * Whether the child at {@code index} (STRUCT field / ARRAY element / MAP value) is nullable. Defaults to
     * {@code true} when the nullability was not carried for that index (legacy factories / older connectors).
     */
    public boolean isChildNullable(int index) {
        return index >= childrenNullable.size() || childrenNullable.get(index);
    }

    /**
     * The comment of the child at {@code index}, or {@code null} when none was carried for that index.
     */
    public String getChildComment(int index) {
        return index < childrenComments.size() ? childrenComments.get(index) : null;
    }

    @Override
    public String toString() {
        if (precision < 0) {
            return typeName;
        }
        if (scale < 0) {
            return typeName + "(" + precision + ")";
        }
        return typeName + "(" + precision + "," + scale + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConnectorType)) {
            return false;
        }
        ConnectorType that = (ConnectorType) o;
        return precision == that.precision
                && scale == that.scale
                && typeName.equals(that.typeName)
                && children.equals(that.children)
                && fieldNames.equals(that.fieldNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(typeName, precision, scale,
                children, fieldNames);
    }
}
