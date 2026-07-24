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
 * equality-based caller/test unaffected. A parallel {@link #isChildCommentSpecified(int)} flag (default
 * {@code true} when unset) records whether each STRUCT field's COMMENT was explicitly written — the one bit
 * the comment string cannot carry (an omitted COMMENT and {@code COMMENT ''} both store an empty string) —
 * so a connector's nested complex {@code MODIFY COLUMN} diff can preserve vs clear a field's current doc.</p>
 *
 * <p><b>Per-child field id</b> ({@link #getChildFieldId(int)}, set via {@link #withChildrenFieldIds(List)}):
 * the stable id of each child field, parallel to {@link #getChildren()}. Also <em>additive</em> and excluded
 * from {@link #equals(Object)}/{@link #hashCode()}. A connector that tracks a stable per-field id (iceberg
 * field-ids) carries them here so fe-core can stamp the Doris child column tree's {@code uniqueId}, which the
 * engine's nested access-path rewrite and the BE field-id scan path match nested leaves by.</p>
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
    // Per-child stable field id, parallel to children (STRUCT field / ARRAY element / MAP key+value). Empty
    // (or shorter than children) means "unset" -> the missing entries default to -1. NOT part of equals():
    // like childrenNullable/childrenComments it is metadata carried alongside the structural shape, not
    // identity. Used by connectors (iceberg) that track a stable per-field id so the engine can rewrite a
    // nested access path from field names to ids (SlotTypeReplacer) and the BE field-id scan path can match
    // nested leaves by id; ConnectorColumnConverter applies these onto the Doris child column tree's uniqueId.
    private final List<Integer> childrenFieldIds;
    // Per-child "was the comment explicitly specified?" flag, parallel to children (STRUCT fields). Empty (or
    // shorter than children) means "unset" -> the missing entries default to true (the carried comment is
    // authoritative), preserving legacy behavior for callers that never populate it. NOT part of equals().
    // This is the one bit {@link #childrenComments} cannot encode: a Doris STRUCT field stores an omitted
    // COMMENT as a non-null empty string, so "COMMENT omitted" and "COMMENT ''" collapse to the same comment
    // value and are distinguishable ONLY by this flag. A connector's nested complex {@code MODIFY COLUMN} diff
    // reads it to keep the field's CURRENT doc when the COMMENT was omitted vs clear it when it was "" (#65329
    // omit-preserves-metadata). Unused by CREATE / ADD (a new field has no prior doc), so those paths are
    // unaffected.
    private final List<Boolean> childrenCommentSpecified;

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
        this(typeName, precision, scale, children, fieldNames, childrenNullable, childrenComments,
                Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children, List<String> fieldNames,
            List<Boolean> childrenNullable, List<String> childrenComments,
            List<Integer> childrenFieldIds) {
        this(typeName, precision, scale, children, fieldNames, childrenNullable, childrenComments,
                childrenFieldIds, Collections.emptyList());
    }

    public ConnectorType(String typeName, int precision, int scale,
            List<ConnectorType> children, List<String> fieldNames,
            List<Boolean> childrenNullable, List<String> childrenComments,
            List<Integer> childrenFieldIds, List<Boolean> childrenCommentSpecified) {
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
        this.childrenFieldIds = childrenFieldIds == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(childrenFieldIds);
        this.childrenCommentSpecified = childrenCommentSpecified == null
                ? Collections.emptyList()
                : Collections.unmodifiableList(childrenCommentSpecified);
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

    /**
     * Factory: STRUCT type with named fields plus per-field nullability, comments, and comment-specified flags
     * (parallel lists). The {@code fieldCommentSpecified} flag lets a connector's nested complex {@code MODIFY
     * COLUMN} diff distinguish an omitted COMMENT (preserve the current doc) from {@code COMMENT ''} (clear it),
     * which {@code fieldComments} alone cannot encode. Additive / excluded from {@link #equals(Object)}.
     */
    public static ConnectorType structOf(List<String> names,
            List<ConnectorType> fieldTypes, List<Boolean> fieldNullable, List<String> fieldComments,
            List<Boolean> fieldCommentSpecified) {
        return new ConnectorType("STRUCT", -1, -1, fieldTypes, names, fieldNullable, fieldComments,
                Collections.emptyList(), fieldCommentSpecified);
    }

    /**
     * Returns a copy of this type carrying the given per-child field ids (parallel to {@link #getChildren()}:
     * STRUCT fields in order / ARRAY element / MAP key+value). Additive and excluded from equality — used by
     * connectors that track a stable per-field id (iceberg) so {@code ConnectorColumnConverter} can stamp the
     * Doris child column tree's {@code uniqueId} for the BE field-id scan path. The other facets
     * (children/fieldNames/nullability/comments) are preserved.
     */
    public ConnectorType withChildrenFieldIds(List<Integer> fieldIds) {
        return new ConnectorType(typeName, precision, scale, children, fieldNames,
                childrenNullable, childrenComments, fieldIds, childrenCommentSpecified);
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

    /** The full per-child field-id list (may be empty / shorter than children when unset). */
    public List<Integer> getChildrenFieldIds() {
        return childrenFieldIds;
    }

    /** The full per-child comment-specified list (may be empty / shorter than children when unset). */
    public List<Boolean> getChildrenCommentSpecified() {
        return childrenCommentSpecified;
    }

    /**
     * Whether the comment of the child at {@code index} was explicitly specified. Defaults to {@code true}
     * (the carried comment is authoritative) when not carried for that index (legacy factories / CREATE /
     * ADD), preserving prior behavior. A connector's nested complex MODIFY diff reads this to keep the field's
     * current doc when the COMMENT was omitted ({@code false}) instead of clearing it.
     */
    public boolean isChildCommentSpecified(int index) {
        return index >= childrenCommentSpecified.size() || childrenCommentSpecified.get(index);
    }

    /**
     * The stable field id of the child at {@code index} (STRUCT field / ARRAY element / MAP key|value), or
     * {@code -1} when none was carried for that index (legacy factories / connectors without field ids).
     */
    public int getChildFieldId(int index) {
        return index < childrenFieldIds.size() ? childrenFieldIds.get(index) : -1;
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
