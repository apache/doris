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

package org.apache.doris.datasource.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;

public class JdbcIdentifierMapping implements IdentifierMapping {
    private static final Logger LOG = LogManager.getLogger(JdbcIdentifierMapping.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final boolean isLowerCaseTableNames;
    private final boolean isLowerCaseMetaNames;
    private final String metaNamesMapping;

    public JdbcIdentifierMapping(boolean isLowerCaseTableNames, boolean isLowerCaseMetaNames, String metaNamesMapping) {
        this.isLowerCaseTableNames = isLowerCaseTableNames;
        this.isLowerCaseMetaNames = isLowerCaseMetaNames;
        this.metaNamesMapping = metaNamesMapping;
        validateMappings();
    }

    private boolean isMappingInvalid() {
        return metaNamesMapping == null || metaNamesMapping.isEmpty();
    }

    @Override
    public String fromRemoteDatabaseName(String remoteDatabaseName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteDatabaseName;
        }
        JsonNode databasesNode = readAndParseJson(metaNamesMapping, "databases");

        Map<String, String> databaseNameMapping = Maps.newHashMap();
        if (databasesNode.isArray()) {
            for (JsonNode node : databasesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String mapping = applyLowerCaseIfNeeded(node.path("mapping").asText());
                databaseNameMapping.put(remoteDatabase, mapping);
            }
        }
        return getMappedName(remoteDatabaseName, databaseNameMapping);
    }

    @Override
    public String fromRemoteTableName(String remoteDatabaseName, String remoteTableName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteTableName;
        }
        JsonNode tablesNode = readAndParseJson(metaNamesMapping, "tables");

        Map<String, String> tableNameMapping = Maps.newHashMap();
        if (tablesNode.isArray()) {
            for (JsonNode node : tablesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                if (remoteDatabaseName.equals(remoteDatabase)) {
                    String remoteTable = node.path("remoteTable").asText();
                    String mapping = applyLowerCaseIfNeeded(node.path("mapping").asText());
                    tableNameMapping.put(remoteTable, mapping);
                }
            }
        }
        return getMappedName(remoteTableName, tableNameMapping);
    }

    @Override
    public String fromRemoteColumnName(String remoteDatabaseName, String remoteTableName, String remoteColumnName) {
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteColumnName;
        }
        JsonNode columnsNode = readAndParseJson(metaNamesMapping, "columns");

        Map<String, String> columnNameMapping = Maps.newHashMap();
        if (columnsNode.isArray()) {
            for (JsonNode node : columnsNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String remoteTable = node.path("remoteTable").asText();
                if (remoteDatabaseName.equals(remoteDatabase) && remoteTableName.equals(remoteTable)) {
                    String remoteColumn = node.path("remoteColumn").asText();
                    String mapping = applyLowerCaseIfNeeded(node.path("mapping").asText());
                    columnNameMapping.put(remoteColumn, mapping);
                }
            }
        }
        return getMappedName(remoteColumnName, columnNameMapping);
    }

    private String getMappedName(String name, Map<String, String> nameMapping) {
        String mappedName = nameMapping.getOrDefault(name, name);
        return isLowerCaseMetaNames ? mappedName.toLowerCase() : mappedName;
    }

    private JsonNode readAndParseJson(String jsonPath, String nodeName) {
        try {
            JsonNode rootNode = mapper.readTree(jsonPath);
            return rootNode.path(nodeName);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON format is incorrect, please check the metaNamesMapping property", e);
        }
    }

    private void validateMappings() {
        Map<String, Set<String>> duplicateErrors = Maps.newLinkedHashMap();
        try {
            JsonNode rootNode = mapper.readTree(metaNamesMapping);

            Map<String, Set<String>> dbMappingCheck = Maps.newHashMap();
            Map<String, Map<String, Set<String>>> tableMappingCheck = Maps.newHashMap();
            Map<String, Map<String, Map<String, Set<String>>>> columnMappingCheck = Maps.newHashMap();

            Set<String> dbKeySet = Sets.newHashSet();
            Map<String, Set<String>> tableKeySet = Maps.newHashMap();
            Map<String, Map<String, Set<String>>> columnKeySet = Maps.newHashMap();

            validateNode(rootNode.path("databases"), "databases", duplicateErrors, dbMappingCheck, tableMappingCheck,
                    columnMappingCheck, dbKeySet, tableKeySet, columnKeySet);
            validateNode(rootNode.path("tables"), "tables", duplicateErrors, dbMappingCheck, tableMappingCheck,
                    columnMappingCheck, dbKeySet, tableKeySet, columnKeySet);
            validateNode(rootNode.path("columns"), "columns", duplicateErrors, dbMappingCheck, tableMappingCheck,
                    columnMappingCheck, dbKeySet, tableKeySet, columnKeySet);

            if (!duplicateErrors.isEmpty()) {
                StringBuilder errorBuilder = new StringBuilder("Duplicate mapping found:\n");
                duplicateErrors.forEach((key, value) -> {
                    errorBuilder.append(key).append(":\n");
                    value.forEach(error -> errorBuilder.append("  - ").append(error).append("\n"));
                });
                throw new RuntimeException(errorBuilder.toString());
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException("The JSON format is incorrect, please check the metaNamesMapping property", e);
        }
    }

    private void validateNode(JsonNode nodes,
            String nodeType,
            Map<String, Set<String>> duplicateErrors,
            Map<String, Set<String>> dbMappingCheck,
            Map<String, Map<String, Set<String>>> tableMappingCheck,
            Map<String, Map<String, Map<String, Set<String>>>> columnMappingCheck,
            Set<String> dbKeySet,
            Map<String, Set<String>> tableKeySet,
            Map<String, Map<String, Set<String>>> columnKeySet) {
        Map<String, String> mappingSet = Maps.newHashMap();
        if (nodes.isArray()) {
            for (JsonNode node : nodes) {
                String remoteKey;
                String remoteDb = null;
                String remoteTbl = null;
                switch (nodeType) {
                    case "databases":
                        remoteKey = node.path("remoteDatabase").asText();
                        checkDuplicateRemoteDatabaseKey(remoteKey, dbKeySet, duplicateErrors);
                        break;
                    case "tables":
                        remoteDb = node.path("remoteDatabase").asText();
                        remoteKey = node.path("remoteTable").asText();
                        checkDuplicateRemoteTableKey(remoteDb, remoteKey, tableKeySet, duplicateErrors);
                        break;
                    case "columns":
                        remoteDb = node.path("remoteDatabase").asText();
                        remoteTbl = node.path("remoteTable").asText();
                        remoteKey = node.path("remoteColumn").asText();
                        checkDuplicateRemoteColumnKey(remoteDb, remoteTbl, remoteKey, columnKeySet, duplicateErrors);
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown type: " + nodeType);
                }

                String mapping = node.path("mapping").asText();

                String existed = mappingSet.get(mapping);
                if (existed != null) {
                    duplicateErrors
                            .computeIfAbsent(nodeType, k -> Sets.newLinkedHashSet())
                            .add(String.format("Remote name: %s, duplicate mapping: %s (original: %s)",
                                    remoteKey, mapping, existed));
                } else {
                    mappingSet.put(mapping, remoteKey);
                }

                switch (nodeType) {
                    case "databases":
                        if (isLowerCaseMetaNames) {
                            checkCaseConflictForDatabase(mapping, dbMappingCheck, duplicateErrors, nodeType, remoteKey);
                        }
                        break;
                    case "tables":
                        if (isLowerCaseMetaNames || isLowerCaseTableNames) {
                            checkCaseConflictForTable(remoteDb, mapping, tableMappingCheck, duplicateErrors,
                                    nodeType, remoteKey);
                        }
                        break;
                    case "columns":
                        checkCaseConflictForColumn(remoteDb, remoteTbl, mapping, columnMappingCheck, duplicateErrors,
                                nodeType, remoteKey);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void checkDuplicateRemoteDatabaseKey(String remoteDatabase,
            Set<String> dbKeySet,
            Map<String, Set<String>> duplicateErrors) {
        if (dbKeySet == null) {
            return;
        }
        if (!dbKeySet.add(remoteDatabase)) {
            duplicateErrors
                    .computeIfAbsent("databases", k -> Sets.newLinkedHashSet())
                    .add(String.format("Duplicate remoteDatabase found: %s", remoteDatabase));
        }
    }

    private void checkDuplicateRemoteTableKey(String remoteDb,
            String remoteTable,
            Map<String, Set<String>> tableKeySet,
            Map<String, Set<String>> duplicateErrors) {
        if (tableKeySet == null) {
            return;
        }
        Set<String> tables = tableKeySet.computeIfAbsent(remoteDb, k -> Sets.newHashSet());
        if (!tables.add(remoteTable)) {
            duplicateErrors
                    .computeIfAbsent("tables", k -> Sets.newLinkedHashSet())
                    .add(String.format("Duplicate remoteTable found in database %s: %s", remoteDb, remoteTable));
        }
    }

    private void checkDuplicateRemoteColumnKey(String remoteDb,
            String remoteTbl,
            String remoteColumn,
            Map<String, Map<String, Set<String>>> columnKeySet,
            Map<String, Set<String>> duplicateErrors) {
        if (columnKeySet == null) {
            return;
        }
        Map<String, Set<String>> tblMap = columnKeySet.computeIfAbsent(remoteDb, k -> Maps.newHashMap());
        Set<String> columns = tblMap.computeIfAbsent(remoteTbl, k -> Sets.newHashSet());
        if (!columns.add(remoteColumn)) {
            duplicateErrors
                    .computeIfAbsent("columns", k -> Sets.newLinkedHashSet())
                    .add(String.format("Duplicate remoteColumn found in database %s, table %s: %s",
                            remoteDb, remoteTbl, remoteColumn));
        }
    }

    private void checkCaseConflictForDatabase(String mapping,
            Map<String, Set<String>> dbMappingCheck,
            Map<String, Set<String>> duplicateErrors,
            String nodeType,
            String remoteKey) {
        if (dbMappingCheck == null) {
            return;
        }
        String lower = mapping.toLowerCase();
        Set<String> variants = dbMappingCheck.computeIfAbsent(lower, k -> Sets.newLinkedHashSet());
        if (!variants.isEmpty() && variants.stream().noneMatch(v -> v.equals(mapping))) {
            duplicateErrors
                    .computeIfAbsent(nodeType, k -> Sets.newLinkedHashSet())
                    .add(String.format("Remote name: %s, case-only different mapping found: %s (existing variants: %s)",
                            remoteKey, mapping, variants));
        }
        variants.add(mapping);
    }

    private void checkCaseConflictForTable(String remoteDb,
            String mapping,
            Map<String, Map<String, Set<String>>> tableMappingCheck,
            Map<String, Set<String>> duplicateErrors,
            String nodeType,
            String remoteKey) {
        if (tableMappingCheck == null || remoteDb == null) {
            return;
        }
        Map<String, Set<String>> dbMap = tableMappingCheck.computeIfAbsent(remoteDb, k -> Maps.newHashMap());
        String lower = mapping.toLowerCase();
        Set<String> variants = dbMap.computeIfAbsent(lower, k -> Sets.newLinkedHashSet());
        if (!variants.isEmpty() && variants.stream().noneMatch(v -> v.equals(mapping))) {
            duplicateErrors
                    .computeIfAbsent(nodeType, k -> Sets.newLinkedHashSet())
                    .add(String.format("Remote name: %s (database: %s), "
                                    + "case-only different mapping found: %s (existing variants: %s)",
                            remoteKey, remoteDb, mapping, variants));
        }
        variants.add(mapping);
    }

    private void checkCaseConflictForColumn(String remoteDb,
            String remoteTbl,
            String mapping,
            Map<String, Map<String, Map<String, Set<String>>>> columnMappingCheck,
            Map<String, Set<String>> duplicateErrors,
            String nodeType,
            String remoteKey) {
        if (columnMappingCheck == null || remoteDb == null || remoteTbl == null) {
            return;
        }
        Map<String, Map<String, Set<String>>> dbMap = columnMappingCheck.computeIfAbsent(remoteDb,
                k -> Maps.newHashMap());
        Map<String, Set<String>> tblMap = dbMap.computeIfAbsent(remoteTbl, k -> Maps.newHashMap());
        String lower = mapping.toLowerCase();
        Set<String> variants = tblMap.computeIfAbsent(lower, k -> Sets.newLinkedHashSet());

        if (!variants.isEmpty() && variants.stream().noneMatch(v -> v.equals(mapping))) {
            duplicateErrors
                    .computeIfAbsent(nodeType, k -> Sets.newLinkedHashSet())
                    .add(String.format(
                            "Remote name: %s (database: %s, table: %s), "
                                    + "case-only different mapping found: %s (existing variants: %s)",
                            remoteKey, remoteDb, remoteTbl, mapping, variants));
        }
        variants.add(mapping);
    }

    private String applyLowerCaseIfNeeded(String value) {
        return isLowerCaseMetaNames ? value.toLowerCase() : value;
    }
}
