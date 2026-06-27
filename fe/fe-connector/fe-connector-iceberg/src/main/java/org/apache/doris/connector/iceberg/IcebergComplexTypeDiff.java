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

import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Stages a complex-type {@code MODIFY COLUMN} onto an iceberg {@link UpdateSchema} by recursively diffing the
 * table's CURRENT iceberg type against the requested NEW iceberg type (built by
 * {@link IcebergSchemaBuilder#buildColumnType} from the neutral {@code ConnectorType}, which now carries the
 * per-element nullability + per-STRUCT-field comments needed to drive the diff).
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
     * @throws DorisConnectorException for any unsupported / illegal change (the caller maps it to a DdlException)
     */
    public static void apply(UpdateSchema updateSchema, String path, Type oldType, Type newType) {
        switch (oldType.typeId()) {
            case STRUCT:
                requireSameCategory(oldType, newType);
                applyStructChange(updateSchema, path, oldType.asStructType(), newType.asStructType());
                break;
            case LIST:
                requireSameCategory(oldType, newType);
                applyListChange(updateSchema, path, (Types.ListType) oldType, (Types.ListType) newType);
                break;
            case MAP:
                requireSameCategory(oldType, newType);
                applyMapChange(updateSchema, path, (Types.MapType) oldType, (Types.MapType) newType);
                break;
            default:
                throw new DorisConnectorException("Unsupported complex type for modify: " + oldType);
        }
    }

    private static void applyStructChange(UpdateSchema updateSchema, String path,
            Types.StructType oldStruct, Types.StructType newStruct) {
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

            Type oldFieldType = oldField.type();
            Type newFieldType = newField.type();
            if (oldFieldType.isPrimitiveType()) {
                boolean typeChanged = !oldFieldType.equals(newFieldType);
                if (typeChanged && !isLegalNestedPrimitivePromotion(oldFieldType, newFieldType)) {
                    throw new DorisConnectorException("Cannot change " + oldFieldType + " to " + newFieldType
                            + " in nested types");
                }
                requireNotNarrowed(oldField, newField, fieldPath);
                boolean commentChanged = !Objects.equals(oldField.doc(), newField.doc());
                if (typeChanged || commentChanged) {
                    updateSchema.updateColumn(fieldPath, newFieldType.asPrimitiveType(), newField.doc());
                }
            } else {
                requireNotNarrowed(oldField, newField, fieldPath);
                apply(updateSchema, fieldPath, oldFieldType, newFieldType);
                if (!Objects.equals(oldField.doc(), newField.doc())) {
                    updateSchema.updateColumnDoc(fieldPath, newField.doc());
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
            Types.ListType oldList, Types.ListType newList) {
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
            apply(updateSchema, elementPath, oldElement, newElement);
        }
        if (!oldList.isElementOptional() && newList.isElementOptional()) {
            updateSchema.makeColumnOptional(elementPath);
        }
    }

    private static void applyMapChange(UpdateSchema updateSchema, String path,
            Types.MapType oldMap, Types.MapType newMap) {
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
            apply(updateSchema, valuePath, oldValue, newValue);
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
