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

/** Bridges Doris Variant access paths to Paimon's Variant extraction RowType. */
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

    static PaimonVariantProjection create(List<List<String>> paths, String timeZone) {
        if (paths == null || paths.isEmpty()) {
            return null;
        }
        List<DataField> fields = new ArrayList<>(paths.size());
        PathNode root = new PathNode();
        for (int fieldIndex = 0; fieldIndex < paths.size(); fieldIndex++) {
            List<String> path = paths.get(fieldIndex);
            if (!supportsObjectPath(path) || !root.add(path, fieldIndex)) {
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

    RowType readType() {
        return readType;
    }

    Variant materialize(DataGetters record, int fieldIndex) {
        InternalRow extracted = record.getRow(fieldIndex, readType.getFieldCount());
        GenericVariantBuilder builder = new GenericVariantBuilder(false);
        appendObject(builder, root, extracted);
        return builder.result();
    }

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

    private static String toPaimonPath(List<String> path) {
        return "$." + String.join(".", path);
    }

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
        InternalRow variant = extracted.getRow(fieldIndex, VARIANT_FIELD_COUNT);
        boolean valueIsNull = variant.isNullAt(VARIANT_VALUE_INDEX);
        boolean metadataIsNull = variant.isNullAt(VARIANT_METADATA_INDEX);
        if (valueIsNull != metadataIsNull) {
            throw new IllegalStateException(
                    "Paimon projected Variant must contain both value and metadata");
        }
        return !valueIsNull;
    }

    private static Variant getExtractedVariant(InternalRow extracted, int fieldIndex) {
        InternalRow variant = extracted.getRow(fieldIndex, VARIANT_FIELD_COUNT);
        return new GenericVariant(
                variant.getBinary(VARIANT_VALUE_INDEX),
                variant.getBinary(VARIANT_METADATA_INDEX));
    }

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
