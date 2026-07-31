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

package org.apache.doris.connector.jdbc;

import org.apache.doris.connector.api.DorisConnectorException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps remote identifier names (databases, tables, columns) to local Doris names.
 *
 * <p>Supports two modes:
 * <ul>
 *   <li><b>Lowercase mode</b>: When {@code isLowerCaseMetaNames} is true,
 *       all remote names are lowercased.</li>
 *   <li><b>Explicit mapping</b>: A JSON mapping string defines specific
 *       remote-to-local name translations.</li>
 * </ul>
 *
 * <p>This is a pure-Java reimplementation of the fe-core {@code JdbcIdentifierMapping}
 * without Jackson dependency. JSON parsing uses a simple regex-based approach.</p>
 */
public final class JdbcIdentifierMapper {

    private static final Logger LOG = Logger.getLogger(JdbcIdentifierMapper.class.getName());

    // Matches a JSON string key-value pair: "key" : "value"
    // Key: any characters except unescaped quote (supports Unicode, spaces, hyphens, etc.)
    // Value: any characters except unescaped quote, but allows escaped quotes (\")
    private static final Pattern JSON_STRING_FIELD = Pattern.compile(
            "\"((?:[^\"\\\\]|\\\\.)*)\"\\s*:\\s*\"((?:[^\"\\\\]|\\\\.)*)\"");

    private final boolean isLowerCaseTableNames;
    private final boolean isLowerCaseMetaNames;
    private final String metaNamesMapping;

    private final List<DbMapping> databaseMappings;
    private final List<TableMapping> tableMappings;
    private final List<ColumnMapping> columnMappings;

    /**
     * Creates an identifier mapper.
     *
     * @param isLowerCaseTableNames whether the server lower_case_table_names != 0
     * @param isLowerCaseMetaNames whether the catalog lower_case_meta_names is true
     * @param metaNamesMapping the JSON mapping string (may be null or empty)
     */
    public JdbcIdentifierMapper(boolean isLowerCaseTableNames,
            boolean isLowerCaseMetaNames, String metaNamesMapping) {
        this.isLowerCaseTableNames = isLowerCaseTableNames;
        this.isLowerCaseMetaNames = isLowerCaseMetaNames;
        this.metaNamesMapping = metaNamesMapping;
        this.databaseMappings = new ArrayList<>();
        this.tableMappings = new ArrayList<>();
        this.columnMappings = new ArrayList<>();
        if (!isMappingInvalid()) {
            parseMappings();
            validateMappings();
        }
    }

    public String fromRemoteDatabaseName(String remoteDatabaseName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteDatabaseName;
        }
        Map<String, String> databaseNameMapping = new HashMap<>();
        for (DbMapping m : databaseMappings) {
            databaseNameMapping.put(m.remoteDatabase,
                    applyLowerCaseIfNeeded(m.mapping));
        }
        return getMappedName(remoteDatabaseName, databaseNameMapping);
    }

    public String fromRemoteTableName(String remoteDatabaseName,
            String remoteTableName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteTableName;
        }
        Map<String, String> tableNameMapping = new HashMap<>();
        for (TableMapping m : tableMappings) {
            if (remoteDatabaseName.equals(m.remoteDatabase)) {
                tableNameMapping.put(m.remoteTable,
                        applyLowerCaseIfNeeded(m.mapping));
            }
        }
        return getMappedName(remoteTableName, tableNameMapping);
    }

    public String fromRemoteColumnName(String remoteDatabaseName,
            String remoteTableName, String remoteColumnName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteColumnName;
        }
        Map<String, String> columnNameMapping = new HashMap<>();
        for (ColumnMapping m : columnMappings) {
            if (remoteDatabaseName.equals(m.remoteDatabase)
                    && remoteTableName.equals(m.remoteTable)) {
                columnNameMapping.put(m.remoteColumn,
                        applyLowerCaseIfNeeded(m.mapping));
            }
        }
        return getMappedName(remoteColumnName, columnNameMapping);
    }

    private boolean isMappingInvalid() {
        return metaNamesMapping == null || metaNamesMapping.isEmpty();
    }

    private String getMappedName(String name, Map<String, String> nameMapping) {
        String mappedName = nameMapping.getOrDefault(name, name);
        return isLowerCaseMetaNames ? mappedName.toLowerCase() : mappedName;
    }

    private String applyLowerCaseIfNeeded(String value) {
        return isLowerCaseMetaNames ? value.toLowerCase() : value;
    }

    // ---- Simple JSON parsing (no Jackson/Gson dependency) ----

    private void parseMappings() {
        List<Map<String, String>> databases = parseJsonArray(metaNamesMapping, "databases");
        for (Map<String, String> obj : databases) {
            databaseMappings.add(new DbMapping(
                    obj.getOrDefault("remoteDatabase", ""),
                    obj.getOrDefault("mapping", "")));
        }

        List<Map<String, String>> tables = parseJsonArray(metaNamesMapping, "tables");
        for (Map<String, String> obj : tables) {
            tableMappings.add(new TableMapping(
                    obj.getOrDefault("remoteDatabase", ""),
                    obj.getOrDefault("remoteTable", ""),
                    obj.getOrDefault("mapping", "")));
        }

        List<Map<String, String>> columns = parseJsonArray(metaNamesMapping, "columns");
        for (Map<String, String> obj : columns) {
            columnMappings.add(new ColumnMapping(
                    obj.getOrDefault("remoteDatabase", ""),
                    obj.getOrDefault("remoteTable", ""),
                    obj.getOrDefault("remoteColumn", ""),
                    obj.getOrDefault("mapping", "")));
        }
    }

    /**
     * Parses a JSON array from a top-level JSON object by field name.
     * Expected format: {"fieldName": [{...}, {...}]}
     * Returns a list of maps, each representing one JSON object in the array.
     */
    static List<Map<String, String>> parseJsonArray(String json, String fieldName) {
        List<Map<String, String>> result = new ArrayList<>();
        if (json == null || json.isEmpty()) {
            return result;
        }
        // Find "fieldName" : [
        String key = "\"" + fieldName + "\"";
        int keyIdx = json.indexOf(key);
        if (keyIdx < 0) {
            return result;
        }
        int bracketStart = json.indexOf('[', keyIdx + key.length());
        if (bracketStart < 0) {
            return result;
        }
        int bracketEnd = findMatchingBracket(json, bracketStart);
        if (bracketEnd < 0) {
            throw new DorisConnectorException(
                    "JSON format is incorrect, please check the meta_names_mapping property");
        }
        String arrayContent = json.substring(bracketStart + 1, bracketEnd);

        // Parse each {...} object in the array
        int pos = 0;
        while (pos < arrayContent.length()) {
            int objStart = arrayContent.indexOf('{', pos);
            if (objStart < 0) {
                break;
            }
            int objEnd = arrayContent.indexOf('}', objStart);
            if (objEnd < 0) {
                break;
            }
            String objStr = arrayContent.substring(objStart, objEnd + 1);
            result.add(parseJsonObject(objStr));
            pos = objEnd + 1;
        }
        return result;
    }

    private static int findMatchingBracket(String json, int openBracketIdx) {
        int depth = 0;
        for (int i = openBracketIdx; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '[') {
                depth++;
            } else if (c == ']') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private static Map<String, String> parseJsonObject(String objStr) {
        Map<String, String> map = new HashMap<>();
        Matcher m = JSON_STRING_FIELD.matcher(objStr);
        while (m.find()) {
            map.put(unescapeJson(m.group(1)), unescapeJson(m.group(2)));
        }
        return map;
    }

    private static String unescapeJson(String s) {
        if (s.indexOf('\\') < 0) {
            return s;
        }
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char next = s.charAt(i + 1);
                switch (next) {
                    case '"':
                    case '\\':
                    case '/':
                        sb.append(next);
                        break;
                    case 'n':
                        sb.append('\n');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    default:
                        sb.append(c);
                        sb.append(next);
                        break;
                }
                i++;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    // ---- Validation ----

    private void validateMappings() {
        Map<String, Set<String>> duplicateErrors = new LinkedHashMap<>();

        Set<String> dbKeySet = new HashSet<>();
        Map<String, Set<String>> tableKeySet = new HashMap<>();
        Map<String, Map<String, Set<String>>> columnKeySet = new HashMap<>();
        Map<String, Set<String>> dbMappingCheck = new HashMap<>();
        Map<String, Map<String, Set<String>>> tableMappingCheck = new HashMap<>();
        Map<String, Map<String, Map<String, Set<String>>>> columnMappingCheck = new HashMap<>();

        validateDatabases(duplicateErrors, dbKeySet, dbMappingCheck);
        validateTables(duplicateErrors, tableKeySet, tableMappingCheck);
        validateColumns(duplicateErrors, columnKeySet, columnMappingCheck);

        if (!duplicateErrors.isEmpty()) {
            StringBuilder sb = new StringBuilder("Duplicate mapping found:\n");
            duplicateErrors.forEach((key, value) -> {
                sb.append(key).append(":\n");
                value.forEach(error -> sb.append("  - ").append(error).append("\n"));
            });
            throw new DorisConnectorException(sb.toString());
        }
    }

    private void validateDatabases(Map<String, Set<String>> errors,
            Set<String> dbKeySet, Map<String, Set<String>> dbMappingCheck) {
        Map<String, String> mappingSet = new HashMap<>();
        for (DbMapping m : databaseMappings) {
            if (!dbKeySet.add(m.remoteDatabase)) {
                errors.computeIfAbsent("databases", k -> new LinkedHashSet<>())
                        .add(String.format("Duplicate remoteDatabase found: %s", m.remoteDatabase));
            }
            String existed = mappingSet.get(m.mapping);
            if (existed != null) {
                errors.computeIfAbsent("databases", k -> new LinkedHashSet<>())
                        .add(String.format("Remote name: %s, duplicate mapping: %s (original: %s)",
                                m.remoteDatabase, m.mapping, existed));
            } else {
                mappingSet.put(m.mapping, m.remoteDatabase);
            }
            if (isLowerCaseMetaNames) {
                checkCaseConflict(m.mapping, dbMappingCheck,
                        errors, "databases", m.remoteDatabase);
            }
        }
    }

    private void validateTables(Map<String, Set<String>> errors,
            Map<String, Set<String>> tableKeySet,
            Map<String, Map<String, Set<String>>> tableMappingCheck) {
        // Scope mapping uniqueness per database — at runtime, fromRemoteTableName()
        // only resolves within a single remote database.
        Map<String, Map<String, String>> perDbMappingSet = new HashMap<>();
        for (TableMapping m : tableMappings) {
            Set<String> tables = tableKeySet.computeIfAbsent(m.remoteDatabase, k -> new HashSet<>());
            if (!tables.add(m.remoteTable)) {
                errors.computeIfAbsent("tables", k -> new LinkedHashSet<>())
                        .add(String.format("Duplicate remoteTable found in database %s: %s",
                                m.remoteDatabase, m.remoteTable));
            }
            Map<String, String> mappingSet = perDbMappingSet.computeIfAbsent(
                    m.remoteDatabase, k -> new HashMap<>());
            String existed = mappingSet.get(m.mapping);
            if (existed != null) {
                errors.computeIfAbsent("tables", k -> new LinkedHashSet<>())
                        .add(String.format("Remote name: %s, duplicate mapping: %s (original: %s)",
                                m.remoteTable, m.mapping, existed));
            } else {
                mappingSet.put(m.mapping, m.remoteTable);
            }
            if (isLowerCaseMetaNames || isLowerCaseTableNames) {
                Map<String, Set<String>> dbMap = tableMappingCheck.computeIfAbsent(
                        m.remoteDatabase, k -> new HashMap<>());
                checkCaseConflict(m.mapping, dbMap, errors, "tables", m.remoteTable);
            }
        }
    }

    private void validateColumns(Map<String, Set<String>> errors,
            Map<String, Map<String, Set<String>>> columnKeySet,
            Map<String, Map<String, Map<String, Set<String>>>> columnMappingCheck) {
        // Scope mapping uniqueness per (database, table) — at runtime,
        // fromRemoteColumnName() only resolves within a single table.
        Map<String, Map<String, Map<String, String>>> perTableMappingSet = new HashMap<>();
        for (ColumnMapping m : columnMappings) {
            Map<String, Set<String>> tblMap = columnKeySet.computeIfAbsent(
                    m.remoteDatabase, k -> new HashMap<>());
            Set<String> cols = tblMap.computeIfAbsent(m.remoteTable, k -> new HashSet<>());
            if (!cols.add(m.remoteColumn)) {
                errors.computeIfAbsent("columns", k -> new LinkedHashSet<>())
                        .add(String.format("Duplicate remoteColumn found in database %s, table %s: %s",
                                m.remoteDatabase, m.remoteTable, m.remoteColumn));
            }
            Map<String, Map<String, String>> dbMappings = perTableMappingSet.computeIfAbsent(
                    m.remoteDatabase, k -> new HashMap<>());
            Map<String, String> mappingSet = dbMappings.computeIfAbsent(
                    m.remoteTable, k -> new HashMap<>());
            String existed = mappingSet.get(m.mapping);
            if (existed != null) {
                errors.computeIfAbsent("columns", k -> new LinkedHashSet<>())
                        .add(String.format("Remote name: %s, duplicate mapping: %s (original: %s)",
                                m.remoteColumn, m.mapping, existed));
            } else {
                mappingSet.put(m.mapping, m.remoteColumn);
            }
            Map<String, Map<String, Set<String>>> dbMap = columnMappingCheck.computeIfAbsent(
                    m.remoteDatabase, k -> new HashMap<>());
            Map<String, Set<String>> tblCheckMap = dbMap.computeIfAbsent(
                    m.remoteTable, k -> new HashMap<>());
            checkCaseConflict(m.mapping, tblCheckMap, errors, "columns", m.remoteColumn);
        }
    }

    private static void checkCaseConflict(String mapping,
            Map<String, Set<String>> caseCheckMap,
            Map<String, Set<String>> errors,
            String nodeType, String remoteKey) {
        String lower = mapping.toLowerCase();
        Set<String> variants = caseCheckMap.computeIfAbsent(lower, k -> new LinkedHashSet<>());
        if (!variants.isEmpty() && variants.stream().noneMatch(v -> v.equals(mapping))) {
            errors.computeIfAbsent(nodeType, k -> new LinkedHashSet<>())
                    .add(String.format(
                            "Remote name: %s, case-only different mapping found: %s (existing variants: %s)",
                            remoteKey, mapping, variants));
        }
        variants.add(mapping);
    }

    // ---- Mapping data holders ----

    private static final class DbMapping {
        final String remoteDatabase;
        final String mapping;

        DbMapping(String remoteDatabase, String mapping) {
            this.remoteDatabase = remoteDatabase;
            this.mapping = mapping;
        }
    }

    private static final class TableMapping {
        final String remoteDatabase;
        final String remoteTable;
        final String mapping;

        TableMapping(String remoteDatabase, String remoteTable, String mapping) {
            this.remoteDatabase = remoteDatabase;
            this.remoteTable = remoteTable;
            this.mapping = mapping;
        }
    }

    private static final class ColumnMapping {
        final String remoteDatabase;
        final String remoteTable;
        final String remoteColumn;
        final String mapping;

        ColumnMapping(String remoteDatabase, String remoteTable,
                String remoteColumn, String mapping) {
            this.remoteDatabase = remoteDatabase;
            this.remoteTable = remoteTable;
            this.remoteColumn = remoteColumn;
            this.mapping = mapping;
        }
    }
}
