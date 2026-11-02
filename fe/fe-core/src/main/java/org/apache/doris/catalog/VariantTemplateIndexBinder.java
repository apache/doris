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

import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VariantField;
import org.apache.doris.nereids.types.VariantType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Resolves field-pattern indexes against ordered Variant templates without changing metadata. */
public final class VariantTemplateIndexBinder {
    private VariantTemplateIndexBinder() {
    }

    public static void validate(String columnName, VariantType variantType, List<IndexDefinition> indexes,
            TInvertedIndexFileStorageFormat format) {
        Map<String, List<IndexDefinition>> fieldIndexes = new HashMap<>();
        Map<String, DataType> fieldTypes = new HashMap<>();
        for (IndexDefinition index : indexes) {
            String pattern = InvertedIndexUtil.getInvertedIndexFieldPattern(index.getProperties());
            DataType type = variantType;
            if (!pattern.isEmpty()) {
                VariantField template = findTemplate(variantType, pattern);
                if (template == null) {
                    throw new AnalysisException("can not find field pattern: " + pattern
                            + " in column: " + columnName);
                }
                type = template.getDataType();
                if (!IndexDefinition.isSupportIdxType(type)) {
                    throw new AnalysisException("field pattern: " + pattern
                            + " is not supported for inverted index of column: " + columnName);
                }
                try {
                    InvertedIndexUtil.checkInvertedIndexParser(columnName,
                            type.toCatalogDataType().getPrimitiveType(), index.getProperties(), format);
                } catch (Exception ex) {
                    throw new AnalysisException("invalid INVERTED index: field pattern: "
                            + pattern + ", " + ex.getMessage(), ex);
                }
            }
            fieldIndexes.computeIfAbsent(pattern, k -> new ArrayList<>()).add(index);
            fieldTypes.put(pattern, type);
        }
        for (Map.Entry<String, List<IndexDefinition>> entry : fieldIndexes.entrySet()) {
            if (!InvertedIndexUtil.canHaveMultipleInvertedIndexes(fieldTypes.get(entry.getKey()), entry.getValue())) {
                throw new AnalysisException("column: " + columnName
                        + " cannot have multiple inverted indexes of the same type with field pattern: "
                        + entry.getKey());
            }
        }
    }

    /** The caller supplies inverted indexes of this column; analyzer selection remains with the caller. */
    public static List<Index> getMatchingIndexes(Column column, List<String> subPath, List<Index> indexes) {
        Column template = findMatchingTemplate(column, String.join(".", subPath));
        String pattern = "";
        if (template != null) {
            // The matched template supplies both its name and fieldPatternType. Index
            // metadata stores only the name; keep the BE's existing interpretation, including
            // duplicate pattern text under different match modes.
            pattern = template.getName();
        }
        List<Index> matching = new ArrayList<>();
        for (Index index : indexes) {
            if (pattern.equals(index.getInvertedIndexFieldPattern())) {
                matching.add(index);
            }
        }
        return matching;
    }

    // CREATE requires unique names; the selected field retains its name and declared match mode.
    private static VariantField findTemplate(VariantType type, String pattern) {
        for (VariantField field : type.getPredefinedFields()) {
            if (field.getPattern().equals(pattern)) {
                return field;
            }
        }
        return null;
    }

    private static Column findMatchingTemplate(Column column, String path) {
        // First match wins, like the BE writer (variant_util.cpp), including overlapping patterns.
        if (column.getChildren() != null) {
            for (Column child : column.getChildren()) {
                if (child.getFieldPatternType() == PatternType.MATCH_NAME_GLOB) {
                    try {
                        if (FileSystems.getDefault().getPathMatcher("glob:" + child.getName())
                                .matches(Paths.get(path))) {
                            return child;
                        }
                    } catch (Exception ex) {
                        // Retain the existing read behavior for invalid legacy glob patterns.
                        continue;
                    }
                } else if (child.getFieldPatternType() == PatternType.MATCH_NAME && child.getName().equals(path)) {
                    return child;
                }
            }
        }
        return null;
    }
}
