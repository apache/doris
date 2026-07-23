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

import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorColumnPath;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Executes #65329 NESTED (dotted-path) column schema evolution on an iceberg {@link Table}: resolving a neutral
 * {@link ConnectorColumnPath} down a struct / list-element / map-value chain, then staging the matching
 * {@link UpdateSchema} operations and committing them.
 *
 * <p>A faithful, iron-law-clean port of the legacy fe-core {@code IcebergMetadataOps} nested-schema methods
 * (path resolver, nested struct-field validation, position application, identifier-field fixup, collection
 * pseudo-field comment rejection). The legacy code diffed iceberg-old vs Doris-new types; here the requested
 * type is pre-built into an iceberg {@link Type} by {@link IcebergSchemaBuilder#buildColumnType} (carried on the
 * {@link IcebergColumnChange}), so this class only ever touches iceberg + neutral SPI types — never a Doris type.
 * Every failure is raised as a {@link DorisConnectorException} (the caller maps it to a {@code DdlException}).</p>
 *
 * <p><b>No partial commit:</b> every {@code UpdateSchema.commit()} is the final statement of each entry point, so
 * any validation/resolution throw aborts the whole change before anything is committed (legacy parity).</p>
 */
public final class IcebergNestedColumnEvolution {

    private IcebergNestedColumnEvolution() {
    }

    /**
     * Adds a nested field at {@code path} (its parent struct plus the new leaf name). The parent must resolve to a
     * struct; the new leaf must not case-insensitively collide with an existing sibling. {@code position} places
     * the new field within its parent struct ({@code null} appends at the end).
     */
    public static void addColumn(Table table, ConnectorColumnPath path, IcebergColumnChange column,
            ConnectorColumnPosition position) {
        Schema schema = table.schema();
        ResolvedColumnPath parentPath = resolveColumnPath(schema, path.getParentPath(), "add");
        if (!parentPath.getType().isStructType()) {
            throw new DorisConnectorException("Parent column path '" + path.getParentPath().getFullPath()
                    + "' is not a struct in Iceberg table: " + table.name());
        }
        validateNoCaseInsensitiveSiblingCollision(parentPath.getType().asStructType(),
                parentPath.getFullPath(), path.getLeafName(), null, "add");

        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.addColumn(parentPath.getFullPath(), path.getLeafName(), column.getType(), column.getComment());
        if (position != null) {
            applyPosition(updateSchema, position, childPath(parentPath.getColumnPath(), path.getLeafName()),
                    schema, "add");
        }
        updateSchema.commit();
    }

    /** Drops the nested field at {@code path}; its parent must resolve to a struct that contains the leaf. */
    public static void dropColumn(Table table, ConnectorColumnPath path) {
        ResolvedColumnPath resolvedPath = validateNestedStructFieldPath(table.schema(), path, "drop");
        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.deleteColumn(resolvedPath.getFullPath());
        updateSchema.commit();
    }

    /**
     * Renames the nested field at {@code path} to {@code newName} (a leaf name). Rejects a case-insensitive
     * collision with an existing sibling and preserves iceberg identifier-field paths across the rename.
     */
    public static void renameColumn(Table table, ConnectorColumnPath path, String newName) {
        Schema schema = table.schema();
        ResolvedColumnPath resolvedPath = validateNestedStructFieldPath(schema, path, "rename");
        ResolvedColumnPath parentPath = resolveColumnPath(schema, path.getParentPath(), "rename");
        validateNoCaseInsensitiveSiblingCollision(parentPath.getType().asStructType(),
                parentPath.getFullPath(), newName, resolvedPath.getField(), "rename");

        UpdateSchema updateSchema = table.updateSchema();
        applyRenameColumn(schema, updateSchema, resolvedPath, newName);
        updateSchema.commit();
    }

    /**
     * Modifies the nested field at {@code path} (type / comment / nullability), optionally repositioning it.
     * A primitive change is restricted to an iceberg-representable promotion; a complex change is diffed
     * field-by-field by {@link IcebergComplexTypeDiff}. {@code nullableSpecified} / {@code commentSpecified}
     * carry the #65329 "omit-preserves-metadata" semantics: an omitted nullability never widens the field, and
     * an omitted comment keeps the field's current doc.
     */
    public static void modifyColumn(Table table, ConnectorColumnPath path, IcebergColumnChange column,
            boolean nullableSpecified, boolean commentSpecified, ConnectorColumnPosition position) {
        Schema schema = table.schema();
        ResolvedColumnPath resolvedPath = resolveColumnPath(schema, path, "modify");
        NestedField currentCol = resolvedPath.getField();
        validateCollectionPseudoFieldComment(schema, resolvedPath, column.getComment(), commentSpecified);
        if (position != null) {
            validatePositionTarget(schema, resolvedPath.getColumnPath(), "modify");
        }

        String columnPath = resolvedPath.getFullPath();
        String targetComment = commentSpecified ? column.getComment() : currentCol.doc();
        UpdateSchema updateSchema = table.updateSchema();
        Type newType = column.getType();
        if (newType.isPrimitiveType()) {
            if (!currentCol.type().isPrimitiveType()) {
                throw new DorisConnectorException(
                        "Modify column type from complex to primitive is not supported: " + columnPath);
            }
            if (currentCol.isOptional() && !column.isNullable()) {
                throw new DorisConnectorException(
                        "Can not change nullable column " + columnPath + " to not null");
            }
            Type.PrimitiveType targetType = newType.asPrimitiveType();
            Type.PrimitiveType currentType = currentCol.type().asPrimitiveType();
            if (!currentType.equals(targetType) && !TypeUtil.isPromotionAllowed(currentType, targetType)) {
                throw new DorisConnectorException("Cannot change column type: " + columnPath + ": "
                        + currentType + " -> " + targetType);
            }
            if (!currentType.equals(targetType)) {
                updateSchema.updateColumn(columnPath, targetType, targetComment);
            } else if (!Objects.equals(currentCol.doc(), targetComment)) {
                updateSchema.updateColumnDoc(columnPath, targetComment);
            }
        } else {
            if (currentCol.type().isPrimitiveType()) {
                throw new DorisConnectorException(
                        "Modify column type from non-complex to complex is not supported: " + columnPath);
            }
            if (currentCol.isOptional() && !column.isNullable()) {
                throw new DorisConnectorException(
                        "Cannot change nullable column " + columnPath + " to not null");
            }
            IcebergComplexTypeDiff.apply(updateSchema, columnPath, currentCol.type(), newType);
            if (!Objects.equals(currentCol.doc(), targetComment)) {
                updateSchema.updateColumnDoc(columnPath, targetComment);
            }
        }
        // #65329 omit-preserves: only widen NOT NULL -> nullable when nullability was explicitly specified.
        if (nullableSpecified && column.isNullable()) {
            updateSchema.makeColumnOptional(columnPath);
        }
        if (position != null) {
            applyPosition(updateSchema, position, resolvedPath.getColumnPath(), schema, "modify");
        }
        updateSchema.commit();
    }

    /**
     * Sets (or clears, with {@code null}/{@code ""}) the comment/doc of the field at {@code path}. Handles BOTH a
     * flat (single-part) and a nested path — this is the sole entry point for {@code MODIFY COLUMN ... COMMENT},
     * which has no flat SPI equivalent. A comment on a list-element or map-value pseudo-field is rejected.
     */
    public static void modifyColumnComment(Table table, ConnectorColumnPath path, String comment) {
        Schema schema = table.schema();
        ResolvedColumnPath resolvedPath = resolveColumnPath(schema, path, "modify comment");
        validateCollectionPseudoFieldComment(schema, resolvedPath, comment, true);

        UpdateSchema updateSchema = table.updateSchema();
        updateSchema.updateColumnDoc(resolvedPath.getFullPath(), comment == null ? "" : comment);
        updateSchema.commit();
    }

    // ------------------------------------------------------------------------------------------------------
    // Path resolution + validation (ported verbatim from legacy IcebergMetadataOps, ColumnPath -> the neutral
    // ConnectorColumnPath, UserException -> DorisConnectorException).
    // ------------------------------------------------------------------------------------------------------

    /**
     * Resolves {@code columnPath} against {@code schema}, descending struct fields (case-insensitive), list
     * elements (the {@code element} pseudo-field) and map values (the {@code value} pseudo-field; a map KEY
     * pseudo-field is rejected). Returns the canonical (case-preserved) dotted path, the resolved iceberg type
     * and the resolved {@link NestedField}. A missing part, a part under a primitive, or a mismatched
     * collection pseudo-field fails loud.
     */
    private static ResolvedColumnPath resolveColumnPath(Schema schema, ConnectorColumnPath columnPath,
            String operation) {
        Type currentType = schema.asStruct();
        NestedField currentField = null;
        String currentPath = "";
        List<String> canonicalParts = new ArrayList<>();
        for (String part : columnPath.getParts()) {
            if (!currentPath.isEmpty()) {
                currentPath += ".";
            }
            currentPath += part;

            if (currentType.isStructType()) {
                NestedField field = currentType.asStructType().caseInsensitiveField(part);
                if (field == null) {
                    throw new DorisConnectorException("Column path does not exist in Iceberg schema: "
                            + columnPath.getFullPath());
                }
                canonicalParts.add(field.name());
                currentField = field;
                currentType = field.type();
            } else if (currentType.isListType()) {
                Types.ListType listType = currentType.asListType();
                NestedField elementField = listType.field(listType.elementId());
                if (!elementField.name().equalsIgnoreCase(part)) {
                    throw new DorisConnectorException("Expected array element path at '" + currentPath
                            + "' for Iceberg column path: " + columnPath.getFullPath());
                }
                canonicalParts.add(elementField.name());
                currentField = elementField;
                currentType = listType.elementType();
            } else if (currentType.isMapType()) {
                Types.MapType mapType = currentType.asMapType();
                NestedField keyField = mapType.field(mapType.keyId());
                if (keyField.name().equalsIgnoreCase(part)) {
                    throw new DorisConnectorException("Cannot " + operation + " MAP key nested column: "
                            + columnPath.getFullPath());
                }
                NestedField valueField = mapType.field(mapType.valueId());
                if (!valueField.name().equalsIgnoreCase(part)) {
                    throw new DorisConnectorException("Expected map value path at '" + currentPath
                            + "' for Iceberg column path: " + columnPath.getFullPath());
                }
                canonicalParts.add(valueField.name());
                currentField = valueField;
                currentType = mapType.valueType();
            } else {
                throw new DorisConnectorException("Cannot resolve nested field under primitive column path: "
                        + columnPath.getFullPath());
            }
        }
        return new ResolvedColumnPath(ConnectorColumnPath.of(canonicalParts), currentType, currentField);
    }

    /**
     * Resolves {@code columnPath}'s parent (which must be a struct) and its leaf within that struct
     * (case-insensitive). Used by nested DROP / RENAME, which target an existing struct field.
     */
    private static ResolvedColumnPath validateNestedStructFieldPath(Schema schema, ConnectorColumnPath columnPath,
            String operation) {
        ResolvedColumnPath parentPath = resolveColumnPath(schema, columnPath.getParentPath(), operation);
        Type parentType = parentPath.getType();
        if (!parentType.isStructType()) {
            throw new DorisConnectorException("Parent column path '" + columnPath.getParentPath().getFullPath()
                    + "' is not a struct for Iceberg nested " + operation + ": " + columnPath.getFullPath());
        }
        NestedField field = parentType.asStructType().caseInsensitiveField(columnPath.getLeafName());
        if (field == null) {
            throw new DorisConnectorException(
                    "Column path does not exist in Iceberg schema: " + columnPath.getFullPath());
        }
        return new ResolvedColumnPath(childPath(parentPath.getColumnPath(), field.name()), field.type(), field);
    }

    private static ConnectorColumnPath childPath(ConnectorColumnPath parentPath, String childName) {
        List<String> parts = new ArrayList<>(parentPath.getParts());
        parts.add(childName);
        return ConnectorColumnPath.of(parts);
    }

    private static void validateNoCaseInsensitiveSiblingCollision(Types.StructType parentType, String parentPath,
            String targetName, NestedField sourceField, String operation) {
        NestedField conflictingField = parentType.caseInsensitiveField(targetName);
        if (conflictingField != null
                && (sourceField == null || conflictingField.fieldId() != sourceField.fieldId())) {
            String targetPath = parentPath.isEmpty() ? targetName : parentPath + "." + targetName;
            String conflictingPath = parentPath.isEmpty()
                    ? conflictingField.name() : parentPath + "." + conflictingField.name();
            String columnDescription = parentPath.isEmpty() ? "column" : "nested column";
            throw new DorisConnectorException("Cannot " + operation + " " + columnDescription + " '" + targetPath
                    + "': conflicts with existing Iceberg field '" + conflictingPath + "' (case-insensitive)");
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Column position (FIRST / AFTER) resolution against the pre-update schema.
    // ------------------------------------------------------------------------------------------------------

    private static void applyPosition(UpdateSchema updateSchema, ConnectorColumnPosition position,
            ConnectorColumnPath columnPath, Schema schema, String operation) {
        String columnName = columnPath.getFullPath();
        if (position.isFirst()) {
            updateSchema.moveFirst(columnName);
        } else {
            updateSchema.moveAfter(columnName, getPositionReferencePath(schema, columnPath, position, operation));
        }
    }

    private static String getPositionReferencePath(Schema schema, ConnectorColumnPath columnPath,
            ConnectorColumnPosition position, String operation) {
        if (position == null || position.isFirst()) {
            return null;
        }
        ConnectorColumnPath referencePath = columnPath.isNested()
                ? childPath(columnPath.getParentPath(), position.getAfterColumn())
                : ConnectorColumnPath.of(position.getAfterColumn());
        return resolveColumnPath(schema, referencePath, operation).getFullPath();
    }

    private static void validatePositionTarget(Schema schema, ConnectorColumnPath columnPath, String operation) {
        if (!columnPath.isNested()) {
            return;
        }
        ResolvedColumnPath parentPath = resolveColumnPath(schema, columnPath.getParentPath(), operation);
        if (!parentPath.getType().isStructType()) {
            throw new DorisConnectorException("Cannot apply column position to '" + columnPath.getFullPath()
                    + "': parent column path '" + parentPath.getFullPath() + "' is not a struct");
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Rename with iceberg identifier-field path fixup.
    // ------------------------------------------------------------------------------------------------------

    private static void applyRenameColumn(Schema schema, UpdateSchema updateSchema,
            ResolvedColumnPath oldPath, String newName) {
        String oldFullPath = oldPath.getFullPath();
        ConnectorColumnPath renamedPath = oldPath.getColumnPath().isNested()
                ? childPath(oldPath.getColumnPath().getParentPath(), newName)
                : ConnectorColumnPath.of(newName);
        String renamedFullPath = renamedPath.getFullPath();
        boolean identifierFieldRenamed = false;
        Set<String> renamedIdentifierFields = new TreeSet<>();
        int renamedFieldId = oldPath.getField().fieldId();
        // Iceberg does not preserve full identifier paths when an identifier field or one of its ancestors is
        // renamed. Use field identity so dotted sibling names are not mistaken for descendants.
        for (int identifierFieldId : schema.identifierFieldIds()) {
            String identifierField = schema.findColumnName(identifierFieldId);
            boolean isRenamedField = identifierFieldId == renamedFieldId;
            boolean isDescendant = TypeUtil.ancestorFields(schema, identifierFieldId).stream()
                    .anyMatch(field -> field.fieldId() == renamedFieldId);
            if (isRenamedField || isDescendant) {
                renamedIdentifierFields.add(renamedFullPath + identifierField.substring(oldFullPath.length()));
                identifierFieldRenamed = true;
            } else {
                renamedIdentifierFields.add(identifierField);
            }
        }

        updateSchema.renameColumn(oldFullPath, newName);
        if (identifierFieldRenamed) {
            updateSchema.setIdentifierFields(renamedIdentifierFields);
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Collection pseudo-field comment rejection.
    // ------------------------------------------------------------------------------------------------------

    private static void validateCollectionPseudoFieldComment(Schema schema, ResolvedColumnPath resolvedPath,
            String comment, boolean commentSpecified) {
        if (!resolvedPath.getColumnPath().isNested()
                || (!commentSpecified && (comment == null || comment.isEmpty()))) {
            return;
        }
        ResolvedColumnPath parentPath = resolveColumnPath(
                schema, resolvedPath.getColumnPath().getParentPath(), "modify comment");
        if (parentPath.getType().isListType() || parentPath.getType().isMapType()) {
            throw new DorisConnectorException(
                    "Iceberg does not support comments on collection element or value fields: "
                            + resolvedPath.getFullPath());
        }
    }

    /**
     * The canonical (case-preserved) dotted path, the resolved iceberg type and {@link NestedField} for a
     * resolved {@link ConnectorColumnPath}. Mirrors the legacy {@code IcebergMetadataOps.ResolvedColumnPath}.
     */
    private static final class ResolvedColumnPath {
        private final ConnectorColumnPath columnPath;
        private final Type type;
        private final NestedField field;

        private ResolvedColumnPath(ConnectorColumnPath columnPath, Type type, NestedField field) {
            this.columnPath = columnPath;
            this.type = type;
            this.field = field;
        }

        private ConnectorColumnPath getColumnPath() {
            return columnPath;
        }

        private String getFullPath() {
            return columnPath.getFullPath();
        }

        private Type getType() {
            return type;
        }

        private NestedField getField() {
            return field;
        }
    }
}
