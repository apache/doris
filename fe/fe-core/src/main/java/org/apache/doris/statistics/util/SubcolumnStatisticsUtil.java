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

package org.apache.doris.statistics.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PatternType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantField;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.SqlUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Discovers and resolves scalar leaf subcolumns declared on {@link VariantType} (exact predefined
 * fields) and {@link StructType} columns for statistics collection, persistence, and CBO lookup.
 */
public final class SubcolumnStatisticsUtil {

    public static final int MAX_STRUCT_NESTING_DEPTH = 4;
    public static final int MAX_SUBCOLUMNS_PER_ROOT = 64;

    public record SubcolumnDescriptor(
            String colId,
            Column rootColumn,
            List<String> subPath,
            Type leafType,
            Column syntheticColumn,
            String sqlExpr) {
    }

    private SubcolumnStatisticsUtil() {
    }

    /**
     * Formats the canonical statistics {@code col_id} for a subcolumn path under {@code rootColumnName}.
     * Uses {@link PropertyAnalyzer#encodeVariantPathComponent(String)} so literal {@code '.'} and {@code '%'}
     * inside path segments never collide with path separators and never introduce SQL quotes or backslashes.
     */
    public static String formatSubcolumnId(String rootColumnName, List<String> subPath) {
        Preconditions.checkArgument(rootColumnName != null && !rootColumnName.isEmpty(),
                "rootColumnName must not be empty");
        Preconditions.checkArgument(subPath != null && !subPath.isEmpty(),
                "subPath must not be empty");
        return rootColumnName + "." + subPath.stream()
                .map(PropertyAnalyzer::encodeVariantPathComponent)
                .collect(Collectors.joining("."));
    }

    public static List<SubcolumnDescriptor> discover(TableIf table, long indexId) {
        if (table == null) {
            return List.of();
        }
        boolean escapeVariantDots = table instanceof OlapTable olap && olap.getVariantEscapeDotsInKeys();
        if (table instanceof OlapTable olapTable) {
            long resolvedIdxId = indexId == -1 ? olapTable.getBaseIndexId() : indexId;
            MaterializedIndexMeta meta = olapTable.getIndexMetaByIndexId(resolvedIdxId);
            if (meta == null) {
                return List.of();
            }
            return discoverFromColumns(meta.getSchema(), escapeVariantDots);
        }
        return discoverFromColumns(table.getBaseSchema(), false);
    }

    public static List<SubcolumnDescriptor> discoverAllIndexes(TableIf table) {
        if (table == null) {
            return List.of();
        }
        if (table instanceof OlapTable olapTable) {
            List<Column> orderedColumns = new ArrayList<>(olapTable.getBaseSchema(false));
            long baseIndexId = olapTable.getBaseIndexId();
            olapTable.getIndexIdToMeta().entrySet().stream()
                    .filter(entry -> entry.getKey() != baseIndexId && entry.getValue() != null)
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> orderedColumns.addAll(entry.getValue().getSchema()));
            return discoverFromColumns(orderedColumns, olapTable.getVariantEscapeDotsInKeys());
        }
        return discoverFromColumns(table.getBaseSchema(false), false);
    }

    public static List<String> discoverSubcolumnIds(TableIf table) {
        return discoverAllIndexes(table).stream()
                .map(SubcolumnDescriptor::colId)
                .collect(Collectors.toList());
    }

    public static List<SubcolumnDescriptor> discoverForColumn(Column rootColumn, boolean escapeVariantDots) {
        if (rootColumn == null || !rootColumn.isVisible()) {
            return List.of();
        }
        List<SubcolumnDescriptor> out = new ArrayList<>();
        collectForRootColumn(rootColumn, escapeVariantDots, Set.of(), out);
        return out;
    }

    public static Optional<SubcolumnDescriptor> resolve(TableIf table, long indexId, String colId) {
        if (table == null || colId == null || colId.indexOf('.') <= 0) {
            return Optional.empty();
        }
        if (table instanceof OlapTable olapTable) {
            long resolvedIdxId = indexId == -1 ? olapTable.getBaseIndexId() : indexId;
            MaterializedIndexMeta meta = olapTable.getIndexMetaByIndexId(resolvedIdxId);
            return meta == null ? Optional.empty() : resolve(meta, olapTable, colId);
        }
        if (table.getColumn(colId) != null) {
            return Optional.empty();
        }
        return matchDescriptor(discoverFromColumns(table.getBaseSchema(), false), colId);
    }

    public static Optional<SubcolumnDescriptor> resolve(MaterializedIndexMeta meta, OlapTable table, String colId) {
        if (meta == null || colId == null || colId.indexOf('.') <= 0) {
            return Optional.empty();
        }
        if (meta.getColumnByName(colId) != null) {
            return Optional.empty();
        }
        boolean escapeVariantDots = table != null && table.getVariantEscapeDotsInKeys();
        return matchDescriptor(discoverFromColumns(meta.getSchema(), escapeVariantDots), colId);
    }

    public static Optional<SubcolumnDescriptor> resolveAnyIndex(TableIf table, String colId) {
        if (table == null || colId == null || colId.indexOf('.') <= 0) {
            return Optional.empty();
        }
        if (table instanceof OlapTable olapTable) {
            if (olapTable.getVisibleColumn(colId) != null) {
                return Optional.empty();
            }
            for (MaterializedIndexMeta meta : olapTable.getIndexIdToMeta().values()) {
                Optional<SubcolumnDescriptor> found = resolve(meta, olapTable, colId);
                if (found.isPresent()) {
                    return found;
                }
            }
            return Optional.empty();
        }
        return resolve(table, -1L, colId);
    }

    public static Column resolveColumn(TableIf table, long indexId, String colId) {
        return resolve(table, indexId, colId).map(SubcolumnDescriptor::syntheticColumn).orElse(null);
    }

    public static Column resolveColumn(MaterializedIndexMeta meta, OlapTable table, String colId) {
        return resolve(meta, table, colId).map(SubcolumnDescriptor::syntheticColumn).orElse(null);
    }

    public static String resolveSqlExpr(TableIf table, long indexId, String colName, Column resolvedCol) {
        Optional<SubcolumnDescriptor> subcolumn = resolve(table, indexId, colName);
        if (subcolumn.isPresent()) {
            return subcolumn.get().sqlExpr();
        }
        String fallbackName = resolvedCol != null ? resolvedCol.getName() : String.valueOf(colName);
        return SqlUtils.getIdentSql(fallbackName);
    }

    public static String resolveSqlExpr(MaterializedIndexMeta meta, OlapTable table, String colName,
            Column resolvedCol) {
        Optional<SubcolumnDescriptor> subcolumn = resolve(meta, table, colName);
        if (subcolumn.isPresent()) {
            return subcolumn.get().sqlExpr();
        }
        String fallbackName = resolvedCol != null ? resolvedCol.getName() : String.valueOf(colName);
        return SqlUtils.getIdentSql(fallbackName);
    }

    private static List<SubcolumnDescriptor> discoverFromColumns(
            Collection<Column> columns, boolean escapeVariantDots) {
        if (columns == null || columns.isEmpty()) {
            return List.of();
        }
        Set<String> physicalColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column col : columns) {
            if (col != null && col.getName() != null) {
                physicalColumnNames.add(col.getName());
            }
        }
        Map<String, SubcolumnDescriptor> deduplicated = new LinkedHashMap<>();
        for (Column col : columns) {
            if (col == null || !col.isVisible()) {
                continue;
            }
            List<SubcolumnDescriptor> perRoot = new ArrayList<>();
            collectForRootColumn(col, escapeVariantDots, physicalColumnNames, perRoot);
            for (SubcolumnDescriptor desc : perRoot) {
                deduplicated.putIfAbsent(desc.colId(), desc);
            }
        }
        return List.copyOf(deduplicated.values());
    }

    private static void collectForRootColumn(Column rootColumn, boolean escapeVariantDots,
            Set<String> physicalColumnNames, List<SubcolumnDescriptor> out) {
        Type type = rootColumn.getType();
        if (type instanceof VariantType variantType) {
            collectVariantSubcolumns(rootColumn, variantType, escapeVariantDots, physicalColumnNames, out);
        } else if (type instanceof StructType structType) {
            collectStructSubcolumns(rootColumn, structType, new ArrayList<>(), 1, physicalColumnNames, out);
        }
    }

    private static void collectVariantSubcolumns(Column rootColumn, VariantType variantType,
            boolean escapeVariantDots, Set<String> physicalColumnNames, List<SubcolumnDescriptor> out) {
        List<VariantField> fields = variantType.getPredefinedFields();
        if (fields == null || fields.isEmpty()) {
            return;
        }
        for (VariantField field : fields) {
            if (out.size() >= MAX_SUBCOLUMNS_PER_ROOT) {
                return;
            }
            if (field == null || field.getType() == null || StatisticsUtil.isUnsupportedType(field.getType())) {
                continue;
            }
            String pattern = field.getPattern();
            if (pattern == null || pattern.isEmpty() || !isExactVariantField(field)) {
                continue;
            }
            List<String> subPath = parseVariantPattern(pattern, escapeVariantDots);
            if (subPath.isEmpty()) {
                continue;
            }
            String colId = formatSubcolumnId(rootColumn.getName(), subPath);
            if (physicalColumnNames.contains(colId)) {
                continue;
            }
            Type leafType = field.getType();
            String sqlExpr = "CAST(" + buildElementAtChain(rootColumn.getName(), subPath)
                    + " AS " + leafType.toSql() + ")";
            Column synthetic = new Column(colId, leafType, false, null, true, null, "");
            out.add(new SubcolumnDescriptor(colId, rootColumn, List.copyOf(subPath), leafType, synthetic, sqlExpr));
        }
    }

    private static void collectStructSubcolumns(Column rootColumn, StructType structType, List<String> prefix,
            int depth, Set<String> physicalColumnNames, List<SubcolumnDescriptor> out) {
        if (depth > MAX_STRUCT_NESTING_DEPTH || structType.getFields() == null) {
            return;
        }
        for (StructField field : structType.getFields()) {
            if (out.size() >= MAX_SUBCOLUMNS_PER_ROOT) {
                return;
            }
            if (field == null || field.getName() == null || field.getName().isEmpty() || field.getType() == null) {
                continue;
            }
            prefix.add(field.getName());
            try {
                Type fieldType = field.getType();
                if (fieldType instanceof StructType nestedStruct) {
                    collectStructSubcolumns(rootColumn, nestedStruct, prefix, depth + 1, physicalColumnNames, out);
                } else if (!StatisticsUtil.isUnsupportedType(fieldType)) {
                    List<String> subPath = List.copyOf(prefix);
                    String colId = formatSubcolumnId(rootColumn.getName(), subPath);
                    if (!physicalColumnNames.contains(colId)) {
                        String sqlExpr = buildElementAtChain(rootColumn.getName(), subPath);
                        Column synthetic = new Column(colId, fieldType, false, null, true, null, "");
                        out.add(new SubcolumnDescriptor(colId, rootColumn, subPath, fieldType, synthetic, sqlExpr));
                    }
                }
            } finally {
                prefix.remove(prefix.size() - 1);
            }
        }
    }

    private static Optional<SubcolumnDescriptor> matchDescriptor(List<SubcolumnDescriptor> descriptors, String colId) {
        for (SubcolumnDescriptor desc : descriptors) {
            if (desc.colId().equals(colId)) {
                return Optional.of(desc);
            }
        }
        for (SubcolumnDescriptor desc : descriptors) {
            if (desc.rootColumn().getType() instanceof StructType && desc.colId().equalsIgnoreCase(colId)) {
                return Optional.of(desc);
            }
            if (desc.rootColumn().getType() instanceof VariantType) {
                String rootPrefix = desc.rootColumn().getName() + ".";
                if (colId.length() == desc.colId().length()
                        && colId.regionMatches(true, 0, rootPrefix, 0, rootPrefix.length())
                        && colId.substring(rootPrefix.length()).equals(desc.colId().substring(rootPrefix.length()))) {
                    return Optional.of(desc);
                }
            }
        }
        return Optional.empty();
    }

    private static boolean isExactVariantField(VariantField field) {
        if (field.getPatternType() == PatternType.MATCH_NAME) {
            return true;
        }
        return !containsGlobWildcard(field.getPattern());
    }

    private static boolean containsGlobWildcard(String pattern) {
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            if (ch == '*' || ch == '?' || ch == '[' || ch == ']') {
                return true;
            }
        }
        return false;
    }

    private static List<String> parseVariantPattern(String pattern, boolean escapeVariantDots) {
        String[] rawSegments = pattern.split("\\.", -1);
        List<String> subPath = new ArrayList<>(rawSegments.length);
        for (String raw : rawSegments) {
            if (raw.isEmpty()) {
                return Collections.emptyList();
            }
            if (escapeVariantDots) {
                try {
                    String decoded = PropertyAnalyzer.decodeVariantPathComponent(raw);
                    if (decoded.isEmpty()) {
                        return Collections.emptyList();
                    }
                    subPath.add(decoded);
                } catch (AnalysisException e) {
                    return Collections.emptyList();
                }
            } else {
                subPath.add(raw);
            }
        }
        return subPath;
    }

    public static String buildElementAtChain(String rootColumnName, List<String> subPath) {
        String expr = SqlUtils.getIdentSql(rootColumnName);
        for (String segment : subPath) {
            expr = "element_at(" + expr + ", '" + StatisticsUtil.escapeSQL(segment) + "')";
        }
        return expr;
    }

    public static String normalizeSubcolumnIdCase(TableIf table, String colId) {
        return resolveAnyIndex(table, colId).map(SubcolumnDescriptor::colId)
                .orElseGet(() -> colId.toLowerCase(Locale.ROOT));
    }
}
