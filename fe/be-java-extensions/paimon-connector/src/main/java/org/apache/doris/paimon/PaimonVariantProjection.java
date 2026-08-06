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

package org.apache.doris.paimon;

import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.variant.GenericVariant;
import org.apache.paimon.data.variant.GenericVariantBuilder;
import org.apache.paimon.data.variant.Variant;
import org.apache.paimon.data.variant.VariantMetadataUtils;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Bridges Doris Variant access paths to Paimon's metadata-marked Variant extraction RowType.
 *
 * <p>Paimon returns one Variant value for every requested path. Doris expressions still consume a
 * single Variant slot, so this class rebuilds a partial object containing only those paths. Missing
 * paths are omitted, while a JSON null remains a present Variant null value.
 */
final class PaimonVariantProjection {
    private static final String FIELD_NAME_PREFIX = "__doris_variant_field_";
    private static final int VARIANT_VALUE_INDEX = 0;
    private static final int VARIANT_METADATA_INDEX = 1;
    private static final int VARIANT_FIELD_COUNT = 2;

    private final RowType readType;
    private final PathNode root;

    private PaimonVariantProjection(RowType readType, PathNode root) {
        this.readType = readType;
        this.root = root;
    }

    /**
     * Creates the metadata-marked RowType understood by Paimon's Variant reader.
     *
     * <p>Each Doris access path becomes one Variant field in {@link #readType}. Returning null
     * means that the complete Variant column must be read instead. This all-or-nothing fallback is
     * important because Doris still evaluates every original element_at expression after the scan.
     */
    static PaimonVariantProjection create(List<List<String>> paths, String timeZone) {
        if (paths == null || paths.isEmpty()) {
            return null;
        }

        List<DataField> fields = new ArrayList<>(paths.size());
        PathNode root = new PathNode();
        for (int fieldIndex = 0; fieldIndex < paths.size(); fieldIndex++) {
            List<String> path = paths.get(fieldIndex);
            if (!supportsObjectPath(path) || !root.add(path, fieldIndex)) {
                // Doris access paths currently do not retain whether a numeric segment came from
                // an array index or an object key. Falling back avoids changing either meaning.
                return null;
            }
            fields.add(new DataField(
                    fieldIndex,
                    FIELD_NAME_PREFIX + fieldIndex,
                    DataTypes.VARIANT(),
                    VariantMetadataUtils.buildVariantMetadata(toPaimonPath(path), false, timeZone)));
        }
        return new PaimonVariantProjection(new RowType(fields), root);
    }

    /** Returns the logical type passed to Paimon's ReadBuilder.withReadType. */
    RowType readType() {
        return readType;
    }

    /**
     * Rebuilds the extracted path values as one partial Variant object for Doris.
     *
     * <p>For example, Paimon returns separate fields for $.name and $.profile.city. This method
     * turns them into {"name": ..., "profile": {"city": ...}}, so the unchanged Doris
     * element_at expressions can continue to read a normal Variant slot.
     */
    Variant materialize(DataGetters record, int fieldIndex) {
        InternalRow extracted = record.getRow(fieldIndex, readType.getFieldCount());
        GenericVariantBuilder builder = new GenericVariantBuilder(false);
        appendObject(builder, root, extracted);
        return builder.result();
    }

    /**
     * Checks whether a path can be represented unambiguously by Paimon's current Variant metadata.
     * Array indexes and delimiter-bearing keys fall back to reading the complete Variant.
     */
    private static boolean supportsObjectPath(List<String> path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        for (String segment : path) {
            if (segment == null || segment.isEmpty() || segment.indexOf('.') >= 0
                    || segment.indexOf('[') >= 0 || segment.indexOf(';') >= 0
                    || isIntegerSegment(segment)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isIntegerSegment(String segment) {
        int offset = segment.startsWith("-") ? 1 : 0;
        if (offset == segment.length()) {
            return false;
        }
        for (int i = offset; i < segment.length(); i++) {
            if (!Character.isDigit(segment.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** Converts Doris path segments such as [profile, city] to Paimon's $.profile.city syntax. */
    private static String toPaimonPath(List<String> path) {
        return "$." + String.join(".", path);
    }

    /** Returns whether this path node or any descendant was present in the source Variant. */
    private static boolean hasValue(PathNode node, InternalRow extracted) {
        if (node.fieldIndex >= 0) {
            return hasExtractedVariant(extracted, node.fieldIndex);
        }
        for (PathNode child : node.children.values()) {
            if (hasValue(child, extracted)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasExtractedVariant(InternalRow extracted, int fieldIndex) {
        // Paimon 1.4.2's RowToColumnConverter writes a Variant's value and metadata children but
        // does not advance the enclosing HeapRowVector. Its null bitmap can therefore be shifted
        // when a batch mixes present and missing paths. The two binary children remain aligned,
        // so use them as the source of truth instead of extracted.isNullAt(fieldIndex).
        InternalRow variant = extracted.getRow(fieldIndex, VARIANT_FIELD_COUNT);
        boolean valueIsNull = variant.isNullAt(VARIANT_VALUE_INDEX);
        boolean metadataIsNull = variant.isNullAt(VARIANT_METADATA_INDEX);
        if (valueIsNull != metadataIsNull) {
            throw new IllegalStateException(
                    "Paimon projected Variant must contain both value and metadata");
        }
        return !valueIsNull;
    }

    /** Reads the aligned value and metadata children as one Paimon Variant. */
    private static Variant getExtractedVariant(InternalRow extracted, int fieldIndex) {
        InternalRow variant = extracted.getRow(fieldIndex, VARIANT_FIELD_COUNT);
        return new GenericVariant(
                variant.getBinary(VARIANT_VALUE_INDEX),
                variant.getBinary(VARIANT_METADATA_INDEX));
    }

    /**
     * Writes one object node recursively, omitting missing paths while preserving present JSON
     * null values. Child insertion order follows the requested access-path order.
     */
    private static void appendObject(
            GenericVariantBuilder builder, PathNode node, InternalRow extracted) {
        int start = builder.getWritePos();
        ArrayList<GenericVariantBuilder.FieldEntry> fields = new ArrayList<>();
        for (Map.Entry<String, PathNode> entry : node.children.entrySet()) {
            PathNode child = entry.getValue();
            if (!hasValue(child, extracted)) {
                continue;
            }
            String key = entry.getKey();
            int dictionaryId = builder.addKey(key);
            fields.add(new GenericVariantBuilder.FieldEntry(
                    key, dictionaryId, builder.getWritePos() - start));
            if (child.fieldIndex >= 0) {
                Variant value = getExtractedVariant(extracted, child.fieldIndex);
                builder.appendVariant(new GenericVariant(value.value(), value.metadata()));
            } else {
                appendObject(builder, child, extracted);
            }
        }
        builder.finishWritingObject(start, fields);
    }

    private static final class PathNode {
        private final Map<String, PathNode> children = new LinkedHashMap<>();
        private int fieldIndex = -1;

        /**
         * Adds one leaf path and records its position in Paimon's extracted Row.
         * Parent/child overlaps and duplicate paths are rejected because one node cannot safely be
         * materialized as both a leaf Variant and an object containing descendants.
         */
        private boolean add(List<String> path, int index) {
            PathNode node = this;
            for (String segment : path) {
                if (node.fieldIndex >= 0) {
                    return false;
                }
                node = node.children.computeIfAbsent(segment, ignored -> new PathNode());
            }
            if (!node.children.isEmpty() || node.fieldIndex >= 0) {
                return false;
            }
            node.fieldIndex = index;
            return true;
        }
    }
}
