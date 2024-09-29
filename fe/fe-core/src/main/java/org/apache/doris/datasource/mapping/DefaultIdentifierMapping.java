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

public class DefaultIdentifierMapping implements IdentifierMapping {
    private static final Logger LOG = LogManager.getLogger(DefaultIdentifierMapping.class);

    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, String> localDBToRemoteDB = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, String>> localTableToRemoteTable
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, String>>>
            localColumnToRemoteColumn = new ConcurrentHashMap<>();

    private final boolean isLowerCaseMetaNames;
    private final String metaNamesMapping;

    public DefaultIdentifierMapping(boolean isLowerCaseMetaNames, String metaNamesMapping) {
        this.isLowerCaseMetaNames = isLowerCaseMetaNames;
        this.metaNamesMapping = metaNamesMapping;
    }

    private boolean isMappingInvalid() {
        return metaNamesMapping == null || metaNamesMapping.isEmpty();
    }

    @Override
    public List<String> fromRemoteDatabaseName(List<String> remoteDatabaseNames) {
        // If mapping is not required, return the original input
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteDatabaseNames;
        }
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

    @Override
    public List<String> fromRemoteTableName(String remoteDbName, List<String> remoteTableNames) {
        // If mapping is not required, return the original input
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteTableNames;
        }
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

        Map<String, List<String>> result = nameListToMapping(remoteTableNames,
                localTableToRemoteTable.get(remoteDbName),
                tableNameMapping, isLowerCaseMetaNames);
        List<String> localTableNames = result.get("localNames");
        List<String> conflictNames = result.get("conflictNames");

        if (!conflictNames.isEmpty()) {
            throw new RuntimeException(
                    "Conflict table names found in remote database/schema: " + remoteDbName
                            + " when lower_case_meta_names is true: " + conflictNames
                            + ". Please set lower_case_meta_names to false or"
                            + " use meta_name_mapping to specify the table names.");
        }
        return localTableNames;
    }

    @Override
    public List<Column> fromRemoteColumnName(String remoteDatabaseName, String remoteTableName,
            List<Column> remoteColumns) {
        // If mapping is not required, return the original input
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return remoteColumns;
        }
        JsonNode tablesNode = readAndParseJson(metaNamesMapping, "columns");

        Map<String, String> columnNameMapping = Maps.newTreeMap();
        if (tablesNode.isArray()) {
            for (JsonNode node : tablesNode) {
                String remoteDatabase = node.path("remoteDatabase").asText();
                String remoteTable = node.path("remoteTable").asText();
                if (remoteDatabaseName.equals(remoteDatabase) && remoteTable.equals(remoteTableName)) {
                    String remoteColumn = node.path("remoteColumn").asText();
                    String mapping = node.path("mapping").asText();
                    columnNameMapping.put(remoteColumn, mapping);
                }
            }
        }
        localColumnToRemoteColumn.putIfAbsent(remoteDatabaseName, new ConcurrentHashMap<>());
        localColumnToRemoteColumn.get(remoteDatabaseName).putIfAbsent(remoteTableName, new ConcurrentHashMap<>());

        List<String> remoteColumnNames = Lists.newArrayList();
        for (Column remoteColumn : remoteColumns) {
            remoteColumnNames.add(remoteColumn.getName());
        }

        Map<String, List<String>> result = nameListToMapping(remoteColumnNames,
                localColumnToRemoteColumn.get(remoteDatabaseName).get(remoteTableName),
                columnNameMapping, isLowerCaseMetaNames);
        List<String> localColumnNames = result.get("localNames");
        List<String> conflictNames = result.get("conflictNames");
        if (!conflictNames.isEmpty()) {
            throw new RuntimeException(
                    "Conflict column names found in remote database/schema: " + remoteDatabaseName
                            + " in remote table: " + remoteTableName
                            + " when lower_case_meta_names is true: " + conflictNames
                            + ". Please set lower_case_meta_names to false or"
                            + " use meta_name_mapping to specify the column names.");
        }
        for (int i = 0; i < remoteColumns.size(); i++) {
            remoteColumns.get(i).setName(localColumnNames.get(i));
        }
        return remoteColumns;
    }

    @Override
    public String toRemoteDatabaseName(String localDatabaseName) {
        // If mapping is not required, return the original input
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return localDatabaseName;
        }
        return getRequiredMapping(localDBToRemoteDB, localDatabaseName, "database", localDatabaseName);
    }

    @Override
    public String toRemoteTableName(String remoteDatabaseName, String localTableName) {
        // If mapping is not required, return the original input
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return localTableName;
        }
        Map<String, String> tableMap = localTableToRemoteTable.computeIfAbsent(remoteDatabaseName,
                k -> new ConcurrentHashMap<>());
        return getRequiredMapping(tableMap, localTableName, "table", localTableName);
    }

    @Override
    public Map<String, String> toRemoteColumnNames(String remoteDatabaseName, String remoteTableName) {
        // If mapping is not required, return an empty map (since there's no mapping)
        if (!isLowerCaseMetaNames && isMappingInvalid()) {
            return Collections.emptyMap();
        }
        ConcurrentHashMap<String, ConcurrentHashMap<String, String>> tableColumnMap
                = localColumnToRemoteColumn.computeIfAbsent(remoteDatabaseName, k -> new ConcurrentHashMap<>());
        Map<String, String> columnMap = tableColumnMap.computeIfAbsent(remoteTableName, k -> new ConcurrentHashMap<>());
        if (columnMap.isEmpty()) {
            LOG.warn("No remote column found for: {}. Please refresh this catalog.", remoteTableName);
            throw new RuntimeException(
                    "No remote column found for: " + remoteTableName + ". Please refresh this catalog.");
        }
        return columnMap;
    }

    private <K, V> V getRequiredMapping(Map<K, V> map, K key, String typeName, String entityName) {
        V value = map.get(key);
        if (value == null) {
            LOG.warn("No remote {} found for {}: {}. Please refresh this catalog.", typeName, typeName, entityName);
            throw new RuntimeException("No remote " + typeName + " found for " + typeName + ": " + entityName
                    + ". Please refresh this catalog.");
        }
        return value;
    }

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
