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
import org.apache.doris.connector.api.DorisConnectorException;

import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Stages a complex-type {@code MODIFY COLUMN} onto an iceberg {@link UpdateSchema} by recursively diffing the
 * table's CURRENT iceberg type against the requested NEW iceberg type (built by
 * {@link IcebergSchemaBuilder#buildColumnType} from the neutral {@code ConnectorType}, which now carries the
 * per-element nullability + per-STRUCT-field comments needed to drive the diff). The same neutral
 * {@code ConnectorType} is passed alongside as {@code newConn} so the walk can read each STRUCT field's
 * {@code commentSpecified} flag — the one datum the iceberg type cannot carry (it stores only a doc string,
 * and a Doris field records an omitted COMMENT as an empty string, identical to {@code COMMENT ''}) — and thus
 * preserve a nested field's CURRENT doc when its COMMENT was omitted instead of clearing it.
 *
 * <p>Connector-internal, pure (no remote calls — it only stages {@code UpdateSchema} operations; the seam
 * commits). It is a faithful port of the legacy fe-core {@code IcebergMetadataOps.applyStruct/List/MapChange}
 * (which diffed iceberg-old vs Doris-new), re-expressed as iceberg-old vs iceberg-new so the connector never
 * touches a Doris {@code Type}. The structural guards that legacy ran up-front in
 * {@code ColumnType.checkSupportSchemaChangeForComplexType} (struct may only grow / field names must match
 * by position / added fields must be nullable + not conflict / nested primitive promotions are restricted)
 * are folded into the same walk — equivalent because nothing is committed until the caller's
 * {@code UpdateSchema.commit()}, so any guard throwing aborts the whole change atomically.</p>
 *
 * <p><b>Supported shape changes (legacy parity):</b> widen an existing nested field's primitive type (only the
 * iceberg-representable safe promotions int&rarr;long, float&rarr;double, or an exact match), change a nested
 * field's comment, widen a NOT NULL nested field to nullable, and append new (nullable) STRUCT fields. The
 * category of every nested level must stay the same (struct/array/map); struct fields may not be renamed,
 * reordered, dropped, or narrowed to NOT NULL; a MAP key type may not change.</p>
 */
public final class IcebergComplexTypeDiff {

    private IcebergComplexTypeDiff() {
    }

    /**
     * Stages the diff of {@code newType} over {@code oldType} (both rooted at {@code path}) onto
     * {@code updateSchema}. {@code oldType} must be a complex type; {@code newType} must be the SAME category.
     *
     * <p>{@code newConn} is the neutral type {@code newType} was built from (structurally identical — same
     * child order / field names), carried so the diff can read each STRUCT field's {@code commentSpecified}
     * flag, which the iceberg {@code newType} (only a doc string per field) cannot represent. When it is
     * {@code null} (legacy callers) every field is treated as comment-specified, i.e. the prior behavior of
     * taking the new field's doc verbatim.</p>
     *
     * @throws DorisConnectorException for any unsupported / illegal change (the caller maps it to a DdlException)
     */
    public static void apply(UpdateSchema updateSchema, String path, Type oldType, Type newType,
            ConnectorType newConn) {
        switch (oldType.typeId()) {
            case STRUCT:
                requireSameCategory(oldType, newType);
                applyStructChange(updateSchema, path, oldType.asStructType(), newType.asStructType(), newConn);
                break;
            case LIST:
                requireSameCategory(oldType, newType);
                applyListChange(updateSchema, path, (Types.ListType) oldType, (Types.ListType) newType, newConn);
                break;
            case MAP:
                requireSameCategory(oldType, newType);
                applyMapChange(updateSchema, path, (Types.MapType) oldType, (Types.MapType) newType, newConn);
                break;
            default:
                throw new DorisConnectorException("Unsupported complex type for modify: " + oldType);
        }
    }

    /** The neutral child at {@code index} of {@code newConn}, or null when {@code newConn} is null / shorter. */
    private static ConnectorType childConn(ConnectorType newConn, int index) {
        if (newConn == null || index >= newConn.getChildren().size()) {
            return null;
        }
        return newConn.getChildren().get(index);
    }

    /**
     * Best-effort pre-build guard that restores the legacy {@code MODIFY COLUMN} message for a nested narrowing
     * to an iceberg-unrepresentable type (e.g. {@code ARRAY<INT> -> ARRAY<SMALLINT>}). Walks the CURRENT iceberg
     * {@code oldType} against the requested NEW neutral {@code newType} and, at the first nested primitive leaf
     * the new type cannot map to iceberg, throws {@code "Cannot change <old> to <new> in nested types"} — the
     * message legacy {@code ColumnType.checkSupportSchemaChangeForComplexType} produced in Doris type space
     * (where the narrow target still exists) — instead of the generic {@code "Unsupported type for Iceberg:
     * SMALLINT"} that {@link IcebergSchemaBuilder#buildColumnType} throws. If the structures do not align or
     * every nested leaf is iceberg-representable it returns without throwing, and the caller keeps the original
     * build error — so no other modify changes behavior.
     */
    public static void validateNestedModifyRepresentable(Type oldType, ConnectorType newType) {
        String newName = newType.getTypeName().toUpperCase(Locale.ROOT);
        switch (oldType.typeId()) {
            case LIST:
                if ("ARRAY".equals(newName) && newType.getChildren().size() == 1) {
                    validateNestedModifyRepresentable(((Types.ListType) oldType).elementType(),
                            newType.getChildren().get(0));
                }
                return;
            case MAP:
                if ("MAP".equals(newName) && newType.getChildren().size() == 2) {
                    Types.MapType oldMap = (Types.MapType) oldType;
                    validateNestedModifyRepresentable(oldMap.keyType(), newType.getChildren().get(0));
                    validateNestedModifyRepresentable(oldMap.valueType(), newType.getChildren().get(1));
                }
                return;
            case STRUCT:
                if ("STRUCT".equals(newName)) {
                    List<Types.NestedField> oldFields = oldType.asStructType().fields();
                    List<ConnectorType> newChildren = newType.getChildren();
                    int shared = Math.min(oldFields.size(), newChildren.size());
                    for (int i = 0; i < shared; i++) {
                        validateNestedModifyRepresentable(oldFields.get(i).type(), newChildren.get(i));
                    }
                }
                return;
            default:
                // oldType is a primitive leaf: if the new leaf is a primitive iceberg cannot represent, this is
                // a narrowing to an unrepresentable nested type -> restore the legacy message (lower-cased to
                // match ColumnType.toSql()).
                if (newType.getChildren().isEmpty() && !isIcebergRepresentable(newType)) {
                    throw new DorisConnectorException("Cannot change " + oldType + " to "
                            + newType.getTypeName().toLowerCase(Locale.ROOT) + " in nested types");
                }
        }
    }

    private static boolean isIcebergRepresentable(ConnectorType leaf) {
        try {
            IcebergTypeMapping.toIcebergPrimitive(leaf);
            return true;
        } catch (DorisConnectorException e) {
            return false;
        }
    }

    private static void applyStructChange(UpdateSchema updateSchema, String path,
            Types.StructType oldStruct, Types.StructType newStruct, ConnectorType newConn) {
        List<Types.NestedField> oldFields = oldStruct.fields();
        List<Types.NestedField> newFields = newStruct.fields();

        // Legacy ColumnType rule: a struct may only grow.
        if (oldFields.size() > newFields.size()) {
            throw new DorisConnectorException("Cannot reduce struct fields from " + oldStruct + " to " + newStruct);
        }

        Set<String> existingNames = new HashSet<>();
        for (int i = 0; i < oldFields.size(); i++) {
            Types.NestedField oldField = oldFields.get(i);
            Types.NestedField newField = newFields.get(i);
            String fieldPath = path + "." + oldField.name();
            existingNames.add(oldField.name());

            // Legacy ColumnType rule: existing fields are matched by position and may not be renamed.
            if (!oldField.name().equals(newField.name())) {
                throw new DorisConnectorException("Cannot rename struct field from '" + oldField.name()
                        + "' to '" + newField.name() + "'");
            }

            // #65329 omit-preserves-metadata: an omitted COMMENT on this field keeps its CURRENT doc; only an
            // explicit COMMENT (incl. "") overrides it. The iceberg newField only carries the (empty) doc, so
            // the "was it specified?" bit comes from the parallel neutral child.
            boolean commentSpecified = newConn == null || newConn.isChildCommentSpecified(i);
            String targetDoc = commentSpecified ? newField.doc() : oldField.doc();

            Type oldFieldType = oldField.type();
            Type newFieldType = newField.type();
            if (oldFieldType.isPrimitiveType()) {
                boolean typeChanged = !oldFieldType.equals(newFieldType);
                if (typeChanged && !isLegalNestedPrimitivePromotion(oldFieldType, newFieldType)) {
                    throw new DorisConnectorException("Cannot change " + oldFieldType + " to " + newFieldType
                            + " in nested types");
                }
                requireNotNarrowed(oldField, newField, fieldPath);
                boolean commentChanged = !Objects.equals(oldField.doc(), targetDoc);
                if (typeChanged || commentChanged) {
                    updateSchema.updateColumn(fieldPath, newFieldType.asPrimitiveType(), targetDoc);
                }
            } else {
                requireNotNarrowed(oldField, newField, fieldPath);
                apply(updateSchema, fieldPath, oldFieldType, newFieldType, childConn(newConn, i));
                if (!Objects.equals(oldField.doc(), targetDoc)) {
                    updateSchema.updateColumnDoc(fieldPath, targetDoc);
                }
            }

            // Widen NOT NULL -> nullable (the reverse is rejected above by requireNotNarrowed).
            if (oldField.isRequired() && newField.isOptional()) {
                updateSchema.makeColumnOptional(fieldPath);
            }
        }

        // Append the new fields (legacy parity: must be nullable and not clash with an existing name).
        for (int i = oldFields.size(); i < newFields.size(); i++) {
            Types.NestedField newField = newFields.get(i);
            if (existingNames.contains(newField.name())) {
                throw new DorisConnectorException("Added struct field '" + newField.name()
                        + "' conflicts with existing field");
            }
            if (newField.isRequired()) {
                throw new DorisConnectorException("New struct field '" + newField.name() + "' must be nullable");
            }
            updateSchema.addColumn(path, newField.name(), newField.type(), newField.doc());
        }
    }

    private static void applyListChange(UpdateSchema updateSchema, String path,
            Types.ListType oldList, Types.ListType newList, ConnectorType newConn) {
        String elementPath = path + "." + oldList.field(oldList.elementId()).name();
        Type oldElement = oldList.elementType();
        Type newElement = newList.elementType();
        if (oldElement.isPrimitiveType()) {
            boolean typeChanged = !oldElement.equals(newElement);
            if (typeChanged && !isLegalNestedPrimitivePromotion(oldElement, newElement)) {
                throw new DorisConnectorException("Cannot change " + oldElement + " to " + newElement
                        + " in nested types");
            }
            requireElementNotNarrowed(oldList, newList, elementPath);
            if (typeChanged) {
                updateSchema.updateColumn(elementPath, newElement.asPrimitiveType(), null);
            }
        } else {
            requireElementNotNarrowed(oldList, newList, elementPath);
            // ARRAY element is neutral child 0; its own doc is not carried (iceberg rejects element comments),
            // but a STRUCT nested inside the element still needs its per-field commentSpecified.
            apply(updateSchema, elementPath, oldElement, newElement, childConn(newConn, 0));
        }
        if (!oldList.isElementOptional() && newList.isElementOptional()) {
            updateSchema.makeColumnOptional(elementPath);
        }
    }

    private static void applyMapChange(UpdateSchema updateSchema, String path,
            Types.MapType oldMap, Types.MapType newMap, ConnectorType newConn) {
        // Legacy parity: a MAP key type may not change.
        if (!oldMap.keyType().equals(newMap.keyType())) {
            throw new DorisConnectorException("Cannot change MAP key type from " + oldMap.keyType()
                    + " to " + newMap.keyType());
        }
        String valuePath = path + "." + oldMap.field(oldMap.valueId()).name();
        Type oldValue = oldMap.valueType();
        Type newValue = newMap.valueType();
        if (oldValue.isPrimitiveType()) {
            boolean typeChanged = !oldValue.equals(newValue);
            if (typeChanged && !isLegalNestedPrimitivePromotion(oldValue, newValue)) {
                throw new DorisConnectorException("Cannot change " + oldValue + " to " + newValue
                        + " in nested types");
            }
            requireValueNotNarrowed(oldMap, newMap, valuePath);
            if (typeChanged) {
                updateSchema.updateColumn(valuePath, newValue.asPrimitiveType(), null);
            }
        } else {
            requireValueNotNarrowed(oldMap, newMap, valuePath);
            // MAP value is neutral child 1 (child 0 is the key); a STRUCT nested inside the value needs its
            // per-field commentSpecified.
            apply(updateSchema, valuePath, oldValue, newValue, childConn(newConn, 1));
        }
        if (!oldMap.isValueOptional() && newMap.isValueOptional()) {
            updateSchema.makeColumnOptional(valuePath);
        }
    }

    /** Rejects narrowing a nullable struct field to NOT NULL (iceberg cannot prove existing rows are non-null). */
    private static void requireNotNarrowed(Types.NestedField oldField, Types.NestedField newField, String fieldPath) {
        if (oldField.isOptional() && newField.isRequired()) {
            throw new DorisConnectorException("Cannot change nullable column " + fieldPath + " to not null");
        }
    }

    private static void requireElementNotNarrowed(Types.ListType oldList, Types.ListType newList, String elementPath) {
        if (oldList.isElementOptional() && !newList.isElementOptional()) {
            throw new DorisConnectorException("Cannot change nullable column " + elementPath + " to not null");
        }
    }

    private static void requireValueNotNarrowed(Types.MapType oldMap, Types.MapType newMap, String valuePath) {
        if (oldMap.isValueOptional() && !newMap.isValueOptional()) {
            throw new DorisConnectorException("Cannot change nullable column " + valuePath + " to not null");
        }
    }

    /**
     * Whether changing a nested primitive {@code oldType} to {@code newType} is a legal promotion, mirroring
     * legacy {@code ColumnType.checkSupportSchemaChangeForNestedPrimitive} restricted to the iceberg-representable
     * cases: an exact match (covers VARCHAR length growth, which both map to iceberg STRING), INT&rarr;BIGINT
     * (iceberg INTEGER&rarr;LONG), and FLOAT&rarr;DOUBLE. Everything else (e.g. a nested DECIMAL precision change,
     * any narrowing, a category change) is rejected — matching legacy's restrictive nested rule.
     */
    private static boolean isLegalNestedPrimitivePromotion(Type oldType, Type newType) {
        if (oldType.equals(newType)) {
            return true;
        }
        Type.TypeID oldId = oldType.typeId();
        Type.TypeID newId = newType.typeId();
        if (oldId == Type.TypeID.INTEGER && newId == Type.TypeID.LONG) {
            return true;
        }
        return oldId == Type.TypeID.FLOAT && newId == Type.TypeID.DOUBLE;
    }

    /** The iceberg type category (struct/list/map) of {@code newType} must equal {@code oldType}'s. */
    private static void requireSameCategory(Type oldType, Type newType) {
        boolean ok;
        switch (oldType.typeId()) {
            case STRUCT:
                ok = newType.isStructType();
                break;
            case LIST:
                ok = newType.isListType();
                break;
            case MAP:
                ok = newType.isMapType();
                break;
            default:
                ok = false;
        }
        if (!ok) {
            throw new DorisConnectorException("Cannot change complex column type category from "
                    + oldType + " to " + newType);
        }
    }
}
