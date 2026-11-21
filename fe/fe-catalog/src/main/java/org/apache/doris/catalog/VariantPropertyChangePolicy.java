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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Rules for metadata-only changes to an existing VARIANT column. */
public final class VariantPropertyChangePolicy {
    private VariantPropertyChangePolicy() {
    }

    public static void checkColumnLevel(Column source, Column target) throws DdlException {
        if (source.getVariantEnableTypedPathsToSparse() != target.getVariantEnableTypedPathsToSparse()) {
            throw new DdlException("Can not change variant enable typed paths to sparse");
        }
        if (source.getVariantMaxSparseColumnStatisticsSize() != target.getVariantMaxSparseColumnStatisticsSize()) {
            throw new DdlException("Can not change variant max sparse column statistics size");
        }
        if (source.getVariantSparseHashShardCount() != target.getVariantSparseHashShardCount()) {
            throw new DdlException("Can not change variant sparse bucket num");
        }
        if (source.getVariantEnableDocMode() != target.getVariantEnableDocMode()) {
            throw new DdlException("Can not change variant enable doc snapshot mode");
        }
        if (source.getVariantDocShardCount() != target.getVariantDocShardCount()) {
            throw new DdlException("Can not change variant doc snapshot shard count");
        }
        if (source.getVariantEnableNestedGroup() != target.getVariantEnableNestedGroup()) {
            throw new DdlException("Can not change variant enable nested group");
        }
        boolean templatesChanged = templatesChanged(source, target);
        if ((source.getVariantMaxSubcolumnsCount() != target.getVariantMaxSubcolumnsCount() || templatesChanged)
                && (source.getVariantEnableDocMode()
                || source.getVariantEnableNestedGroup() || source.getVariantEnableTypedPathsToSparse())) {
            throw new DdlException("Can not change variant max subcolumns count or schema templates"
                    + " with doc mode, nested group or typed paths to sparse enabled");
        }
        if (templatesChanged) {
            List<VariantField> oldFields = fields(source);
            List<VariantField> newFields = fields(target);
            if (newFields.size() < oldFields.size()) {
                throw new DdlException("Can only append variant schema templates");
            }
            for (int i = 0; i < oldFields.size(); i++) {
                if (!isAllowedEntryChange(oldFields.get(i), newFields.get(i))) {
                    throw new DdlException("Can only append variant schema templates");
                }
            }
            Set<String> patterns = new HashSet<>();
            for (VariantField field : newFields) {
                if (!patterns.add(field.getPattern())) {
                    throw new DdlException("Duplicate variant schema template pattern: " + field.getPattern());
                }
            }
        }
    }

    public static boolean hasPropertyChange(Column source, Column target) {
        return source.getVariantMaxSubcolumnsCount() != target.getVariantMaxSubcolumnsCount()
                || templatesChanged(source, target);
    }

    public static boolean templatesChanged(Column source, Column target) {
        List<VariantField> oldFields = fields(source);
        List<VariantField> newFields = fields(target);
        if (oldFields.size() != newFields.size()) {
            return true;
        }
        for (int i = 0; i < oldFields.size(); i++) {
            if (!sameEntry(oldFields.get(i), newFields.get(i))) {
                return true;
            }
        }
        return false;
    }

    // Single hook for template entry modification. Only identical entries are allowed today; a future
    // narrow-to-wide type widening relaxes this function alone (BE readers cast and compaction rewrites).
    static boolean isAllowedEntryChange(VariantField oldEntry, VariantField newEntry) {
        return sameEntry(oldEntry, newEntry);
    }

    private static boolean sameEntry(VariantField oldEntry, VariantField newEntry) {
        // Thrift includes nested nullability, which Type.equals does not compare for arrays and maps.
        return Objects.equals(oldEntry.getPattern(), newEntry.getPattern())
                && patternType(oldEntry) == patternType(newEntry)
                && oldEntry.getType().equals(newEntry.getType())
                && oldEntry.getType().toThrift().equals(newEntry.getType().toThrift())
                && Objects.equals(oldEntry.getComment(), newEntry.getComment());
    }

    private static PatternType patternType(VariantField field) {
        return field.getPatternType() == null ? PatternType.MATCH_NAME_GLOB : field.getPatternType();
    }

    private static List<VariantField> fields(Column column) {
        return column.getType() instanceof VariantType
                ? ((VariantType) column.getType()).getPredefinedFields() : Collections.emptyList();
    }

    private static String defaultExpression(Column column) {
        return column.getDefaultValueExprDef() == null ? null : column.getDefaultValueExprDef().getSql();
    }

    public static void checkPropertyChangeIsolated(Column source, Column target, boolean hasPosition)
            throws DdlException {
        if (hasPosition || source.isAllowNull() != target.isAllowNull()
                || !Objects.equals(source.getDefaultValue(), target.getDefaultValue())
                || !Objects.equals(defaultExpression(source), defaultExpression(target))
                || !Objects.equals(source.getRealDefaultValue(), target.getRealDefaultValue())
                || !Objects.equals(source.getComment(), target.getComment())
                || source.isKey() != target.isKey()
                // The implicit flag is how the statement spelled the aggregation, not a column property:
                // a unique-key value column is CREATEd explicit and MODIFYed implicit REPLACE.
                || !Objects.equals(source.getAggregationType(), target.getAggregationType())) {
            throw new DdlException("Variant property changes must be isolated from column position,"
                    + " nullability, default, comment, key or aggregation changes");
        }
    }
}
