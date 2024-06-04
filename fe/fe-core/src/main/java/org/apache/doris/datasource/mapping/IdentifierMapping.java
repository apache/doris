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

import org.apache.doris.catalog.Column;
import org.apache.doris.qe.GlobalVariable;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class IdentifierMapping {
    private static final Logger LOG = LogManager.getLogger(IdentifierMapping.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, String> localDBToRemoteDB = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> localTableToRemoteTable
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>>
            localColumnToRemoteColumn = new ConcurrentHashMap<>();

    private final AtomicBoolean dbNamesLoaded = new AtomicBoolean(false);
    private final ConcurrentHashMap<String, AtomicBoolean> tableNamesLoadedMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, AtomicBoolean>> columnNamesLoadedMap
            = new ConcurrentHashMap<>();

    private final boolean isLowerCaseMetaNames;
    private final String metaNamesMapping;

    public IdentifierMapping(boolean isLowerCaseMetaNames, String metaNamesMapping) {
        this.isLowerCaseMetaNames = isLowerCaseMetaNames;
        this.metaNamesMapping = metaNamesMapping;
    }

    public List<String> setDatabaseNameMapping(List<String> remoteDatabaseNames) {
        JsonNode databasesNode = readAndParseJson(metaNamesMapping, "databases");

        Map<String, String> databaseNameMapping = Maps.newTreeMap();
        if (databasesNode.isArray()) {
            for (JsonNode node : databasesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String mapping = node.path("mapping").asText();
                databaseNameMapping.put(remoteDatabase, mapping);
            }
        }

        Map<String, List<String>> result = nameListToMapping(remoteDatabaseNames, localDBToRemoteDB,
                databaseNameMapping, isLowerCaseMetaNames);
        List<String> localDatabaseNames = result.get("localNames");
        List<String> conflictNames = result.get("conflictNames");
        if (!conflictNames.isEmpty()) {
            throw new RuntimeException(
                    "Conflict database/schema names found when lower_case_meta_names is true: " + conflictNames
                            + ". Please set lower_case_meta_names to false or"
                            + " use meta_name_mapping to specify the names.");
        }
        return localDatabaseNames;
    }

    public List<String> setTableNameMapping(String remoteDbName, List<String> remoteTableNames) {
        JsonNode tablesNode = readAndParseJson(metaNamesMapping, "tables");

        Map<String, String> tableNameMapping = Maps.newTreeMap();
        if (tablesNode.isArray()) {
            for (JsonNode node : tablesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                if (remoteDbName.equals(remoteDatabase)) {
                    String remoteTable = node.path("remoteTable").asText();
                    String mapping = node.path("mapping").asText();
                    tableNameMapping.put(remoteTable, mapping);
                }
            }
        }

        localTableToRemoteTable.putIfAbsent(remoteDbName, new ConcurrentHashMap<>());

        List<String> localTableNames;
        List<String> conflictNames;

        if (GlobalVariable.lowerCaseTableNames == 1) {
            Map<String, List<String>> result = nameListToMapping(remoteTableNames,
                    localTableToRemoteTable.get(remoteDbName),
                    tableNameMapping, true);
            localTableNames = result.get("localNames");
            conflictNames = result.get("conflictNames");
            if (!conflictNames.isEmpty()) {
                throw new RuntimeException(
                        "Conflict table names found in remote database/schema: " + remoteDbName
                                + " when lower_case_table_names is 1: " + conflictNames
                                + ". Please use meta_name_mapping to specify the names.");
            }
        } else {
            Map<String, List<String>> result = nameListToMapping(remoteTableNames,
                    localTableToRemoteTable.get(remoteDbName),
                    tableNameMapping, isLowerCaseMetaNames);
            localTableNames = result.get("localNames");
            conflictNames = result.get("conflictNames");

            if (!conflictNames.isEmpty()) {
                throw new RuntimeException(
                        "Conflict table names found in remote database/schema: " + remoteDbName
                                + "when lower_case_meta_names is true: " + conflictNames
                                + ". Please set lower_case_meta_names to false or"
                                + " use meta_name_mapping to specify the table names.");
            }
        }
        return localTableNames;
    }

    public List<Column> setColumnNameMapping(String remoteDbName, String remoteTableName, List<Column> remoteColumns) {
        JsonNode tablesNode = readAndParseJson(metaNamesMapping, "columns");

        Map<String, String> columnNameMapping = Maps.newTreeMap();
        if (tablesNode.isArray()) {
            for (JsonNode node : tablesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String remoteTable = node.path("remoteTable").asText();
                if (remoteDbName.equals(remoteDatabase) && remoteTable.equals(remoteTableName)) {
                    String remoteColumn = node.path("remoteColumn").asText();
                    String mapping = node.path("mapping").asText();
                    columnNameMapping.put(remoteColumn, mapping);
                }
            }
        }
        localColumnToRemoteColumn.putIfAbsent(remoteDbName, new ConcurrentHashMap<>());
        localColumnToRemoteColumn.get(remoteDbName).putIfAbsent(remoteTableName, new ConcurrentHashMap<>());

        List<String> localColumnNames;
        List<String> conflictNames;

        // Get the name from localColumns and save it to List<String>
        List<String> remoteColumnNames = Lists.newArrayList();
        for (Column remoteColumn : remoteColumns) {
            remoteColumnNames.add(remoteColumn.getName());
        }

        Map<String, List<String>> result = nameListToMapping(remoteColumnNames,
                localColumnToRemoteColumn.get(remoteDbName).get(remoteTableName),
                columnNameMapping, isLowerCaseMetaNames);
        localColumnNames = result.get("localNames");
        conflictNames = result.get("conflictNames");
        if (!conflictNames.isEmpty()) {
            throw new RuntimeException(
                    "Conflict column names found in remote database/schema: " + remoteDbName
                            + " in remote table: " + remoteTableName
                            + " when lower_case_meta_names is true: " + conflictNames
                            + ". Please set lower_case_meta_names to false or"
                            + " use meta_name_mapping to specify the column names.");
        }
        // Replace the name in remoteColumns with localColumnNames
        for (int i = 0; i < remoteColumns.size(); i++) {
            remoteColumns.get(i).setName(localColumnNames.get(i));
        }
        return remoteColumns;
    }

    public String getRemoteDatabaseName(String localDbName) {
        return getRequiredMapping(localDBToRemoteDB, localDbName, "database", this::loadDatabaseNamesIfNeeded,
                localDbName);
    }

    public String getRemoteTableName(String localDbName, String localTableName) {
        String remoteDbName = getRemoteDatabaseName(localDbName);
        Map<String, String> tableMap = localTableToRemoteTable.computeIfAbsent(remoteDbName,
                k -> new ConcurrentHashMap<>());
        return getRequiredMapping(tableMap, localTableName, "table", () -> loadTableNamesIfNeeded(localDbName),
                localTableName);
    }

    public Map<String, String> getRemoteColumnNames(String localDbName, String localTableName) {
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        ConcurrentHashMap<String, ConcurrentHashMap<String, String>> tableColumnMap
                = localColumnToRemoteColumn.computeIfAbsent(remoteDbName, k -> new ConcurrentHashMap<>());
        Map<String, String> columnMap = tableColumnMap.computeIfAbsent(remoteTableName, k -> new ConcurrentHashMap<>());
        if (columnMap.isEmpty()) {
            LOG.info("Column name mapping missing, loading column names for localDbName: {}, localTableName: {}",
                    localDbName, localTableName);
            loadColumnNamesIfNeeded(localDbName, localTableName);
            columnMap = tableColumnMap.get(remoteTableName);
        }
        if (columnMap.isEmpty()) {
            LOG.warn("No remote column found for localTableName: {}. Please refresh this catalog.", localTableName);
            throw new RuntimeException(
                    "No remote column found for localTableName: " + localTableName + ". Please refresh this catalog.");
        }
        return columnMap;
    }


    private void loadDatabaseNamesIfNeeded() {
        if (dbNamesLoaded.compareAndSet(false, true)) {
            try {
                loadDatabaseNames();
            } catch (Exception e) {
                dbNamesLoaded.set(false); // Reset on failure
                LOG.warn("Error loading database names", e);
            }
        }
    }

    private void loadTableNamesIfNeeded(String localDbName) {
        AtomicBoolean isLoaded = tableNamesLoadedMap.computeIfAbsent(localDbName, k -> new AtomicBoolean(false));
        if (isLoaded.compareAndSet(false, true)) {
            try {
                loadTableNames(localDbName);
            } catch (Exception e) {
                tableNamesLoadedMap.get(localDbName).set(false); // Reset on failure
                LOG.warn("Error loading table names for localDbName: {}", localDbName, e);
            }
        }
    }

    private void loadColumnNamesIfNeeded(String localDbName, String localTableName) {
        columnNamesLoadedMap.putIfAbsent(localDbName, new ConcurrentHashMap<>());
        AtomicBoolean isLoaded = columnNamesLoadedMap.get(localDbName)
                .computeIfAbsent(localTableName, k -> new AtomicBoolean(false));
        if (isLoaded.compareAndSet(false, true)) {
            try {
                loadColumnNames(localDbName, localTableName);
            } catch (Exception e) {
                columnNamesLoadedMap.get(localDbName).get(localTableName).set(false); // Reset on failure
                LOG.warn("Error loading column names for localDbName: {}, localTableName: {}", localDbName,
                        localTableName, e);
            }
        }
    }

    private <K, V> V getRequiredMapping(Map<K, V> map, K key, String typeName, Runnable loadIfNeeded,
            String entityName) {
        if (map.isEmpty() || !map.containsKey(key) || map.get(key) == null) {
            LOG.info("{} mapping missing, loading for {}: {}", typeName, typeName, entityName);
            loadIfNeeded.run();
        }
        V value = map.get(key);
        if (value == null) {
            LOG.warn("No remote {} found for {}: {}. Please refresh this catalog.", typeName, typeName, entityName);
            throw new RuntimeException("No remote " + typeName + " found for " + typeName + ": " + entityName
                    + ". Please refresh this catalog.");
        }
        return value;
    }

    // Load the database name from the data source.
    // In the corresponding getDatabaseNameList(), setDatabaseNameMapping() must be used to update the mapping.
    protected abstract void loadDatabaseNames();

    // Load the table names for the specified database from the data source.
    // In the corresponding getTableNameList(), setTableNameMapping() must be used to update the mapping.
    protected abstract void loadTableNames(String localDbName);

    // Load the column names for a specified table in a database from the data source.
    // In the corresponding getColumnNameList(), setColumnNameMapping() must be used to update the mapping.
    protected abstract void loadColumnNames(String localDbName, String localTableName);

    private JsonNode readAndParseJson(String jsonPath, String nodeName) {
        JsonNode rootNode;
        try {
            rootNode = mapper.readTree(jsonPath);
            return rootNode.path(nodeName);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("parse meta_names_mapping property error", e);
        }
    }

    private Map<String, List<String>> nameListToMapping(List<String> remoteNames,
            ConcurrentHashMap<String, String> localNameToRemoteName,
            Map<String, String> nameMapping, boolean isLowerCaseMetaNames) {
        List<String> filteredDatabaseNames = Lists.newArrayList();
        Set<String> lowerCaseNames = Sets.newHashSet();
        Map<String, List<String>> nameMap = Maps.newHashMap();
        List<String> conflictNames = Lists.newArrayList();

        for (String name : remoteNames) {
            String mappedName = nameMapping.getOrDefault(name, name);
            String localName = isLowerCaseMetaNames ? mappedName.toLowerCase() : mappedName;

            // Use computeIfAbsent to ensure atomicity
            localNameToRemoteName.computeIfAbsent(localName, k -> name);

            if (isLowerCaseMetaNames && !lowerCaseNames.add(localName)) {
                if (nameMap.containsKey(localName)) {
                    nameMap.get(localName).add(mappedName);
                }
            } else {
                nameMap.putIfAbsent(localName, Lists.newArrayList(Collections.singletonList(mappedName)));
            }

            filteredDatabaseNames.add(localName);
        }

        for (List<String> conflictNameList : nameMap.values()) {
            if (conflictNameList.size() > 1) {
                conflictNames.addAll(conflictNameList);
            }
        }

        Map<String, List<String>> result = Maps.newConcurrentMap();
        result.put("localNames", filteredDatabaseNames);
        result.put("conflictNames", conflictNames);
        return result;
    }
}
