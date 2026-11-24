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
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.commands.info.IndexDefinition;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.thrift.TInvertedIndexFileStorageFormat;

import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared template binding for CREATE, ALTER and query index selection. */
public final class VariantTemplateIndexBinder {
    private VariantTemplateIndexBinder() {
    }

    public static void validate(String columnName, Type type, List<IndexDefinition> indexDefs,
            TInvertedIndexFileStorageFormat format) {
        Map<String, List<IndexDefinition>> byPattern = new HashMap<>();
        Map<String, DataType> types = new HashMap<>();
        for (IndexDefinition indexDef : indexDefs) {
            String pattern = InvertedIndexUtil.getInvertedIndexFieldPattern(indexDef.getProperties());
            DataType indexType = DataType.fromCatalogType(type);
            if (!pattern.isEmpty()) {
                VariantField field = null;
                if (type instanceof VariantType) {
                    for (VariantField candidate : ((VariantType) type).getPredefinedFields()) {
                        if (candidate.getPattern().equals(pattern)) {
                            field = candidate;
                            break;
                        }
                    }
                }
                if (field == null) {
                    throw new AnalysisException("can not find field pattern: " + pattern + " in column: " + columnName);
                }
                indexType = DataType.fromCatalogType(field.getType());
                if (!IndexDefinition.isSupportIdxType(indexType)) {
                    throw new AnalysisException("field pattern: " + pattern
                            + " is not supported for inverted index of column: " + columnName);
                }
                try {
                    InvertedIndexUtil.checkInvertedIndexParser(columnName, field.getType().getPrimitiveType(),
                            indexDef.getProperties(), format);
                } catch (Exception ex) {
                    throw new AnalysisException("invalid INVERTED index: field pattern: "
                            + pattern + ", " + ex.getMessage(), ex);
                }
            }
            byPattern.computeIfAbsent(pattern, k -> new ArrayList<>()).add(indexDef);
            types.put(pattern, indexType);
        }
        for (Map.Entry<String, List<IndexDefinition>> entry : byPattern.entrySet()) {
            if (!InvertedIndexUtil.canHaveMultipleInvertedIndexes(types.get(entry.getKey()), entry.getValue())) {
                throw new AnalysisException("column: " + columnName
                        + " cannot have multiple inverted indexes of the same type with field pattern: "
                        + entry.getKey());
            }
        }
    }

    public static void validateIndexes(Column column, List<Index> indexes, TInvertedIndexFileStorageFormat format) {
        List<IndexDefinition> definitions = new ArrayList<>();
        for (Index index : indexes) {
            if (index.getIndexType() == IndexType.INVERTED && index.getColumns().size() == 1
                    && column.nameEquals(index.getColumns().get(0), false)) {
                definitions.add(new IndexDefinition(index.getIndexName(), false, index.getColumns(), "INVERTED",
                        index.getProperties(), index.getComment()));
            }
        }
        validate(column.getName(), column.getType(), definitions, format);
    }

    /** First match wins, as in the BE writer; an empty result selects the root index. */
    public static String firstMatchingPattern(Column column, List<String> subPath) {
        String path = String.join(".", subPath);
        if (column.getChildren() != null) {
            for (Column child : column.getChildren()) {
                String pattern = child.getName();
                if (child.getFieldPatternType() == null
                        || child.getFieldPatternType() == PatternType.MATCH_NAME_GLOB) {
                    try {
                        if (FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Paths.get(path))) {
                            return pattern;
                        }
                    } catch (Exception e) {
                        // Preserve query selection behavior for legacy invalid glob patterns.
                        continue;
                    }
                } else if (child.getFieldPatternType() == PatternType.MATCH_NAME && pattern.equals(path)) {
                    return pattern;
                }
            }
        }
        return "";
    }
}
