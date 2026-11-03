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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Statement-time validation of Variant write-property changes; not used during metadata replay. */
public final class VariantPropertyChangePolicy {
    enum ChangeKind {
        LIGHT, IMMUTABLE, TEMPLATE
    }

    // Classify persisted VariantType fields only. The unpersisted raw properties map is not policy state.
    // Declaration order preserves master's diagnostic precedence for immutable differences.
    enum FieldPolicy {
        // Non-DOC budget changes only re-select dynamic paths for later writes; history keeps its layout.
        MAX_SUBCOLUMNS_COUNT("variantMaxSubcolumnsCount", ChangeKind.LIGHT, Column::getVariantMaxSubcolumnsCount),
        TYPED_PATHS_TO_SPARSE("enableTypedPathsToSparse", Column::getVariantEnableTypedPathsToSparse,
                "Can not change variant enable typed paths to sparse"),
        SPARSE_STATISTICS_SIZE("variantMaxSparseColumnStatisticsSize", Column::getVariantMaxSparseColumnStatisticsSize,
                "Can not change variant max sparse column statistics size"),
        SPARSE_SHARD_COUNT("variantSparseHashShardCount", Column::getVariantSparseHashShardCount,
                "Can not change variant sparse bucket num"),
        DOC_MODE("enableVariantDocMode", Column::getVariantEnableDocMode,
                "Can not change variant enable doc snapshot mode"),
        DOC_SHARD_COUNT("variantDocShardCount", Column::getVariantDocShardCount,
                "Can not change variant doc snapshot shard count"),
        NESTED_GROUP("enableNestedGroup", Column::getVariantEnableNestedGroup,
                "Can not change variant enable nested group"),
        DOC_MATERIALIZATION_MIN_ROWS("variantDocMaterializationMinRows", ChangeKind.LIGHT,
                Column::getvariantDocMaterializationMinRows),
        FIELDS("fields", ChangeKind.TEMPLATE, null),
        FIELD_MAP("fieldMap", ChangeKind.TEMPLATE, null);

        final String serializedName;
        final ChangeKind kind;
        private final Function<Column, ?> value;
        private final String rejectionMessage;

        FieldPolicy(String serializedName, Function<Column, ?> value, String rejectionMessage) {
            this.serializedName = serializedName;
            this.kind = ChangeKind.IMMUTABLE;
            this.value = value;
            this.rejectionMessage = rejectionMessage;
        }

        FieldPolicy(String serializedName, ChangeKind kind, Function<Column, ?> value) {
            this.serializedName = serializedName;
            this.kind = kind;
            this.value = value;
            this.rejectionMessage = null;
        }

        boolean changed(Column source, Column target) {
            return !Objects.equals(value.apply(source), value.apply(target));
        }
    }

    private VariantPropertyChangePolicy() {
    }

    public static void checkColumnLevel(Column source, Column target) throws DdlException {
        for (FieldPolicy field : FieldPolicy.values()) {
            if (field.kind == ChangeKind.IMMUTABLE && field.changed(source, target)) {
                throw new DdlException(field.rejectionMessage);
            }
        }
        // DOC mode is equal here. DOC budget changes stay rejected until old DOC layouts are qualified.
        if (source.getVariantEnableDocMode() && FieldPolicy.MAX_SUBCOLUMNS_COUNT.changed(source, target)) {
            throw new DdlException("Can not change variant max subcolumns count on a DOC-mode column");
        }
        // A pure count change keeps every template entry, so master's children rule does not apply to it.
        if (hasBudgetChange(source, target) && sameEntries(templates(source), templates(target))) {
            return;
        }
        // Otherwise both persisted template fields follow master's children rule, even when unchanged.
        if (CollectionUtils.isNotEmpty(source.getChildren()) || CollectionUtils.isNotEmpty(target.getChildren())) {
            throw new DdlException("Can not change variant schema templates");
        }
    }

    static List<VariantField> templates(Column column) {
        if (!(column.getType() instanceof VariantType)) {
            return Collections.emptyList();
        }
        List<VariantField> fields = ((VariantType) column.getType()).getPredefinedFields();
        return fields == null ? Collections.emptyList() : new ArrayList<>(fields);
    }

    private static boolean sameEntries(List<VariantField> source, List<VariantField> target) {
        if (source.size() != target.size()) {
            return false;
        }
        for (int i = 0; i < source.size(); i++) {
            if (!sameEntry(source.get(i), target.get(i))) {
                return false;
            }
        }
        return true;
    }

    // VariantField.equals ignores comment and match mode, so compare every stored attribute explicitly.
    private static boolean sameEntry(VariantField source, VariantField target) {
        return source.getPattern().equals(target.getPattern())
                && patternType(source) == patternType(target)
                && !nullabilityDiffers(source.getType(), target.getType())
                && Objects.equals(source.getType(), target.getType())
                && Objects.equals(comment(source), comment(target));
    }

    private static PatternType patternType(VariantField field) {
        // Replayed entries without a match mode mean MATCH_NAME_GLOB, as in Column.gsonPostProcess.
        return field.getPatternType() == null ? PatternType.MATCH_NAME_GLOB : field.getPatternType();
    }

    private static String comment(VariantField field) {
        return field.getComment() == null || field.getComment().isEmpty() ? null : field.getComment();
    }

    private static boolean nullabilityDiffers(Type source, Type target) {
        if (source instanceof ArrayType && target instanceof ArrayType) {
            ArrayType a = (ArrayType) source;
            ArrayType b = (ArrayType) target;
            return a.getContainsNull() != b.getContainsNull() || nullabilityDiffers(a.getItemType(), b.getItemType());
        }
        if (source instanceof MapType && target instanceof MapType) {
            MapType a = (MapType) source;
            MapType b = (MapType) target;
            return !Objects.equals(a.getIsKeyContainsNull(), b.getIsKeyContainsNull())
                    || !Objects.equals(a.getIsValueContainsNull(), b.getIsValueContainsNull())
                    || nullabilityDiffers(a.getKeyType(), b.getKeyType())
                    || nullabilityDiffers(a.getValueType(), b.getValueType());
        }
        if (source instanceof StructType && target instanceof StructType) {
            List<StructField> a = ((StructType) source).getFields();
            List<StructField> b = ((StructType) target).getFields();
            if (a.size() != b.size()) {
                return false;
            }
            for (int i = 0; i < a.size(); i++) {
                if (a.get(i).getContainsNull() != b.get(i).getContainsNull()
                        || nullabilityDiffers(a.get(i).getType(), b.get(i).getType())) {
                    return true;
                }
            }
        }
        return false;
    }

    /** True when the dynamic subcolumn budget differs. Only this delta is new in the light route. */
    public static boolean hasBudgetChange(Column source, Column target) {
        return FieldPolicy.MAX_SUBCOLUMNS_COUNT.changed(source, target);
    }

    /**
     * A budget change is accepted only as a pure Variant property change, so it never carries
     * nullability, default, comment, key or aggregation deltas through the light route.
     */
    public static void checkPropertyChangeIsolated(Column source, Column target) throws DdlException {
        if (!hasBudgetChange(source, target)) {
            return;
        }
        if (source.isAllowNull() != target.isAllowNull()
                || !Objects.equals(source.getDefaultValue(), target.getDefaultValue())
                || !Objects.equals(source.getComment(), target.getComment())
                || source.isKey() != target.isKey()
                || !Objects.equals(source.getAggregationType(), target.getAggregationType())) {
            throw new DdlException(
                    "Can not change variant max subcolumns count together with other column attributes");
        }
    }
}
