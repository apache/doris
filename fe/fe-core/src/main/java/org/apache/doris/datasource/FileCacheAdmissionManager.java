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

package org.apache.doris.datasource;

import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigWatcher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileCacheAdmissionManager {
    private static final Logger LOG = LogManager.getLogger(FileCacheAdmissionManager.class);

    public enum RuleType {
        EXCLUDE(0),
        INCLUDE(1);

        private final int value;

        RuleType(int value) {
            this.value = value;
        }

        public static RuleType fromValue(int value) {
            if (value == 0) {
                return EXCLUDE;
            } else if (value == 1) {
                return INCLUDE;
            }
            throw new IllegalArgumentException("Invalid RuleType Value: " + value);
        }
    }

    public enum RuleLevel {
        PARTITION,   // 0
        TABLE,       // 1
        DATABASE,    // 2
        CATALOG,     // 3
        GLOBAL,      // 4
        INVALID      // 5
    }

    public static class RulePattern {
        private final long id;
        private final String userIdentity;
        private final String catalog;
        private final String database;
        private final String table;
        private final String partitionPattern;
        private final RuleType ruleType;

        public RulePattern(long id, String userIdentity, String catalog, String database,
                           String table, String partitionPattern, RuleType ruleType) {
            this.id = id;
            this.userIdentity = userIdentity;
            this.catalog = catalog != null ? catalog : "";
            this.database = database != null ? database : "";
            this.table = table != null ? table : "";
            this.partitionPattern = partitionPattern != null ? partitionPattern : "";
            this.ruleType = ruleType;
        }

        public long getId() {
            return id;
        }

        public String getUserIdentity() {
            return userIdentity;
        }

        public String getCatalog() {
            return catalog;
        }

        public String getDatabase() {
            return database;
        }

        public String getTable() {
            return table;
        }

        public String getPartitionPattern() {
            return partitionPattern;
        }

        public RuleType getRuleType() {
            return ruleType;
        }
    }

    public static class AdmissionRule {
        private final long id;
        private final String userIdentity;
        private final String catalog;
        private final String database;
        private final String table;
        private final String partitionPattern;
        private final RuleType ruleType;
        private final boolean enabled;
        private final long createdTime;
        private final long updatedTime;

        @JsonCreator
        public AdmissionRule(
                @JsonProperty("id") long id,
                @JsonProperty("user_identity") String userIdentity,
                @JsonProperty("catalog_name") String catalog,
                @JsonProperty("database_name") String database,
                @JsonProperty("table_name") String table,
                @JsonProperty("partition_pattern") String partitionPattern,
                @JsonProperty("rule_type") int ruleType,
                @JsonProperty("enabled") boolean enabled,
                @JsonProperty("created_time") long createdTime,
                @JsonProperty("updated_time") long updatedTime) {
            this.id = id;
            this.userIdentity = userIdentity != null ? userIdentity : "";
            this.catalog = catalog != null ? catalog : "";
            this.database = database != null ? database : "";
            this.table = table != null ? table : "";
            this.partitionPattern = partitionPattern != null ? partitionPattern : "";
            this.ruleType = RuleType.fromValue(ruleType);
            this.enabled = enabled;
            this.createdTime = createdTime;
            this.updatedTime = updatedTime;
        }

        public RulePattern toRulePattern() {
            return new RulePattern(id, userIdentity, catalog, database, table, partitionPattern, ruleType);
        }

        public long getId() {
            return id;
        }

        public String getUserIdentity() {
            return userIdentity;
        }

        public String getTable() {
            return table;
        }

        public String getDatabase() {
            return database;
        }

        public String getCatalog() {
            return catalog;
        }

        public String getPartitionPattern() {
            return partitionPattern;
        }

        public RuleType getRuleType() {
            return ruleType;
        }

        public boolean getEnabled() {
            return enabled;
        }

        public long getCreatedTime() {
            return createdTime;
        }

        public long getUpdatedTime() {
            return updatedTime;
        }
    }

    public static class RuleLoader {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        public static List<AdmissionRule> loadRulesFromFile(String filePath) throws Exception {
            File file = new File(filePath);
            if (!file.exists()) {
                throw new IllegalArgumentException("File cache admission JSON file does not exist: " + filePath);
            }

            return MAPPER.readValue(file, new TypeReference<List<AdmissionRule>>() {});
        }
    }

    public static class RuleCollection {
        private Boolean excludeGlobal = false;
        private final Set<String> excludeCatalogRules = new HashSet<>();
        private final Map<String, Set<String>> excludeDatabaseRules = new HashMap<>();
        private final Map<String, Set<String>> excludeTableRules = new HashMap<>();

        private Boolean includeGlobal = false;
        private final Set<String> includeCatalogRules = new HashSet<>();
        private final Map<String, Set<String>> includeDatabaseRules = new HashMap<>();
        private final Map<String, Set<String>> includeTableRules = new HashMap<>();

        public static final String REASON_COMMON_CATALOG_BLACKLIST = "common catalog-level blacklist rule";
        public static final String REASON_COMMON_CATALOG_WHITELIST = "common catalog-level whitelist rule";
        public static final String REASON_COMMON_DATABASE_BLACKLIST = "common database-level blacklist rule";
        public static final String REASON_COMMON_DATABASE_WHITELIST = "common database-level whitelist rule";
        public static final String REASON_COMMON_TABLE_BLACKLIST = "common table-level blacklist rule";
        public static final String REASON_COMMON_TABLE_WHITELIST = "common table-level whitelist rule";
        public static final String REASON_USER_GLOBAL_BLACKLIST = "user global-level blacklist rule";
        public static final String REASON_USER_GLOBAL_WHITELIST = "user global-level whitelist rule";
        public static final String REASON_USER_CATALOG_BLACKLIST = "user catalog-level blacklist rule";
        public static final String REASON_USER_CATALOG_WHITELIST = "user catalog-level whitelist rule";
        public static final String REASON_USER_DATABASE_BLACKLIST = "user database-level blacklist rule";
        public static final String REASON_USER_DATABASE_WHITELIST = "user database-level whitelist rule";
        public static final String REASON_USER_TABLE_BLACKLIST = "user table-level blacklist rule";
        public static final String REASON_USER_TABLE_WHITELIST = "user table-level whitelist rule";
        public static final String REASON_DEFAULT = "default rule";

        public boolean isAdmittedAtTableLevel(String userIdentity, String catalog, String database, String table,
                                              AtomicReference<String> reason) {

            String catalogDatabase = catalog + "." + database;

            if (containsKeyValue(excludeTableRules, table, catalogDatabase)) {
                reason.set(REASON_COMMON_TABLE_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeTableRules, table, catalogDatabase)) {
                reason.set(REASON_COMMON_TABLE_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(excludeDatabaseRules, database, catalog)) {
                reason.set(REASON_COMMON_DATABASE_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeDatabaseRules, database, catalog)) {
                reason.set(REASON_COMMON_DATABASE_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (excludeCatalogRules.contains(catalog)) {
                reason.set(REASON_COMMON_CATALOG_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (includeCatalogRules.contains(catalog)) {
                reason.set(REASON_COMMON_CATALOG_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }

            reason.set(REASON_DEFAULT);
            logAdmission(false, userIdentity, catalog, database, table, reason.get());
            return false;
        }

        public boolean isAdmittedAtTableLevel(RuleCollection userCollection, String userIdentity,
                                              String catalog, String database, String table,
                                              AtomicReference<String> reason) {

            String catalogDatabase = catalog + "." + database;

            if (containsKeyValue(excludeTableRules, table, catalogDatabase)) {
                reason.set(REASON_COMMON_TABLE_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(userCollection.excludeTableRules, table, catalogDatabase)) {
                reason.set(REASON_USER_TABLE_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeTableRules, table, catalogDatabase)) {
                reason.set(REASON_COMMON_TABLE_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(userCollection.includeTableRules, table, catalogDatabase)) {
                reason.set(REASON_USER_TABLE_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(excludeDatabaseRules, database, catalog)) {
                reason.set(REASON_COMMON_DATABASE_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(userCollection.excludeDatabaseRules, database, catalog)) {
                reason.set(REASON_USER_DATABASE_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeDatabaseRules, database, catalog)) {
                reason.set(REASON_COMMON_DATABASE_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(userCollection.includeDatabaseRules, database, catalog)) {
                reason.set(REASON_USER_DATABASE_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (excludeCatalogRules.contains(catalog)) {
                reason.set(REASON_COMMON_CATALOG_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (userCollection.excludeCatalogRules.contains(catalog)) {
                reason.set(REASON_USER_CATALOG_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (includeCatalogRules.contains(catalog)) {
                reason.set(REASON_COMMON_CATALOG_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (userCollection.includeCatalogRules.contains(catalog)) {
                reason.set(REASON_USER_CATALOG_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (userCollection.excludeGlobal) {
                reason.set(REASON_USER_GLOBAL_BLACKLIST);
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (userCollection.includeGlobal) {
                reason.set(REASON_USER_GLOBAL_WHITELIST);
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }

            reason.set(REASON_DEFAULT);
            logAdmission(false, userIdentity, catalog, database, table, reason.get());
            return false;
        }

        private boolean containsKeyValue(Map<String, Set<String>> map, String key, String value) {
            Set<String> set = map.get(key);
            return set != null && set.contains(value);
        }

        private void logAdmission(boolean admitted, String userIdentity, String catalog, String database,
                                  String table, String reason) {
            if (LOG.isDebugEnabled()) {
                String status = admitted ? "admitted" : "denied";

                String logMessage = String.format(
                        "File cache request %s by %s, user_identity: %s, "
                            + "catalog: %s, database: %s, table: %s",
                        status, reason, userIdentity, catalog, database, table);

                LOG.debug(logMessage);
            }
        }

        public RuleLevel getRuleLevel(RulePattern rulePattern) {
            int pattern = 0;
            if (!rulePattern.getPartitionPattern().isEmpty()) {
                pattern |= 1;
            }
            if (!rulePattern.getTable().isEmpty()) {
                pattern |= 1 << 1;
            }
            if (!rulePattern.getDatabase().isEmpty()) {
                pattern |= 1 << 2;
            }
            if (!rulePattern.getCatalog().isEmpty()) {
                pattern |= 1 << 3;
            }

            RuleLevel[] levelTable = {
                /* 0000 */ RuleLevel.GLOBAL,     // 0
                /* 0001 */ RuleLevel.INVALID,    // 1
                /* 0010 */ RuleLevel.INVALID,    // 2
                /* 0011 */ RuleLevel.INVALID,    // 3
                /* 0100 */ RuleLevel.INVALID,    // 4
                /* 0101 */ RuleLevel.INVALID,    // 5
                /* 0110 */ RuleLevel.INVALID,    // 6
                /* 0111 */ RuleLevel.INVALID,    // 7
                /* 1000 */ RuleLevel.CATALOG,    // 8
                /* 1001 */ RuleLevel.INVALID,    // 9
                /* 1010 */ RuleLevel.INVALID,    // 10
                /* 1011 */ RuleLevel.INVALID,    // 11
                /* 1100 */ RuleLevel.DATABASE,   // 12
                /* 1101 */ RuleLevel.INVALID,    // 13
                /* 1110 */ RuleLevel.TABLE,      // 14
                /* 1111 */ RuleLevel.PARTITION   // 15
            };

            return levelTable[pattern];
        }

        public void add(RulePattern rulePattern) {
            RuleLevel ruleLevel = getRuleLevel(rulePattern);
            if (ruleLevel == RuleLevel.INVALID) {
                return;
            }

            Set<String> catalogRules = (rulePattern.getRuleType() == RuleType.EXCLUDE)
                    ? excludeCatalogRules : includeCatalogRules;
            Map<String, Set<String>> databaseRules = (rulePattern.getRuleType() == RuleType.EXCLUDE)
                    ? excludeDatabaseRules : includeDatabaseRules;
            Map<String, Set<String>> tableRules = (rulePattern.getRuleType() == RuleType.EXCLUDE)
                    ? excludeTableRules : includeTableRules;

            switch (ruleLevel) {
                case GLOBAL:
                    if (rulePattern.getRuleType() == RuleType.EXCLUDE) {
                        excludeGlobal = true;
                    } else {
                        includeGlobal = true;
                    }
                    break;
                case CATALOG:
                    catalogRules.add(rulePattern.getCatalog());
                    break;
                case DATABASE:
                    databaseRules.computeIfAbsent(rulePattern.getDatabase(), k -> ConcurrentHashMap.newKeySet())
                            .add(rulePattern.getCatalog());
                    break;
                case TABLE:
                    String catalogDatabase = rulePattern.getCatalog() + "." + rulePattern.getDatabase();
                    tableRules.computeIfAbsent(rulePattern.getTable(), k -> ConcurrentHashMap.newKeySet())
                            .add(catalogDatabase);
                    break;
                case PARTITION:
                    // TODO: Implementing partition-level rules
                    break;
                default:
                    break;
            }
        }
    }

    public static class ConcurrentRuleManager {
        // Characters in ASCII order: A-Z, then other symbols, then a-z
        private static final int PARTITION_COUNT = 58;
        private final List<Map<String, RuleCollection>> maps;
        private final RuleCollection commonCollection;

        static List<String> otherReasons = new ArrayList<>(Arrays.asList(
                "empty user_identity",
                "invalid user_identity"
        ));

        public ConcurrentRuleManager() {
            maps = new ArrayList<>(PARTITION_COUNT);
            commonCollection = new RuleCollection();

            for (int i = 0; i < PARTITION_COUNT; i++) {
                maps.add(new ConcurrentHashMap<>());
            }
        }

        private int getIndex(char firstChar) {
            return firstChar - 'A';
        }

        public void initialize(List<AdmissionRule> rules) {
            for (AdmissionRule rule : rules) {
                if (!rule.getEnabled()) {
                    continue;
                }

                RulePattern rulePattern = rule.toRulePattern();

                if (rulePattern.getUserIdentity().isEmpty()) {
                    commonCollection.add(rulePattern);
                    continue;
                }

                char firstChar = rulePattern.getUserIdentity().charAt(0);
                if (!Character.isAlphabetic(firstChar)) {
                    continue;
                }

                int index = getIndex(firstChar);
                maps.get(index).computeIfAbsent(rulePattern.getUserIdentity(),
                        k -> new RuleCollection()).add(rulePattern);
            }
        }

        public boolean isAdmittedAtTableLevel(String userIdentity, String catalog, String database, String table,
                                              AtomicReference<String> reason) {
            if (userIdentity.isEmpty()) {
                reason.set(otherReasons.get(0));
                logDefaultAdmission(userIdentity, catalog, database, table, reason.get());
                return false;
            }

            char firstChar = userIdentity.charAt(0);
            if (!Character.isAlphabetic(firstChar)) {
                reason.set(otherReasons.get(1));
                logDefaultAdmission(userIdentity, catalog, database, table, reason.get());
                return false;
            }

            int index = getIndex(firstChar);
            RuleCollection collection = maps.get(index).get(userIdentity);
            if (collection == null) {
                return commonCollection.isAdmittedAtTableLevel(userIdentity, catalog, database, table, reason);
            } else {
                return commonCollection.isAdmittedAtTableLevel(
                        collection, userIdentity, catalog, database, table, reason);
            }
        }

        private void logDefaultAdmission(String userIdentity, String catalog, String database, String table,
                                         String reason) {
            if (LOG.isDebugEnabled()) {
                boolean admitted = false;
                String decision = admitted ? "admitted" : "denied";

                String logMessage = String.format(
                        "File cache request %s by default rule, "
                        + "user_identity: %s, catalog: %s, database: %s, table: %s, reason: %s",
                        decision, userIdentity, catalog, database, table, reason);

                LOG.debug(logMessage);
            }
        }
    }

    private ConcurrentRuleManager ruleManager;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private static final FileCacheAdmissionManager INSTANCE = new FileCacheAdmissionManager();

    private ConfigWatcher watcher;

    public FileCacheAdmissionManager() {
        this.ruleManager = new ConcurrentRuleManager();
    }

    public static FileCacheAdmissionManager getInstance() {
        return INSTANCE;
    }

    public void initialize(List<AdmissionRule> rules) {
        ruleManager.initialize(rules);
    }

    public boolean isAdmittedAtTableLevel(String userIdentity, String catalog, String database, String table,
                                          AtomicReference<String> reason) {
        readLock.lock();
        boolean admissionResult = ruleManager.isAdmittedAtTableLevel(userIdentity, catalog, database, table, reason);
        readLock.unlock();

        return admissionResult;
    }

    public void loadOnStartup() {
        LOG.info("Loading file cache admission rules...");
        loadRules();

        LOG.info("Starting file cache admission rules refreshing task");
        watcher = new ConfigWatcher(Config.file_cache_admission_control_json_dir);
        watcher.setOnCreateConsumer(filePath -> {
            String fileName = filePath.toString();
            if (fileName.endsWith(".json")) {
                loadRules();
            }
        });
        watcher.setOnDeleteConsumer(filePath -> {
            String fileName = filePath.toString();
            if (fileName.endsWith(".json")) {
                loadRules();
            }
        });
        watcher.setOnModifyConsumer(filePath -> {
            String fileName = filePath.toString();
            if (fileName.endsWith(".json")) {
                loadRules();
            }
        });
        watcher.start();

        LOG.info("Started file cache admission rules refreshing task");
    }

    public void loadRules(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            LOG.warn("File cache admission JSON file path is not configured, admission control will be disabled.");
            return;
        }

        try {
            List<AdmissionRule> loadedRules = RuleLoader.loadRulesFromFile(filePath);
            LOG.info("{} rules loaded successfully from file: {}", loadedRules.size(), filePath);

            ConcurrentRuleManager newRuleManager = new ConcurrentRuleManager();
            newRuleManager.initialize(loadedRules);

            writeLock.lock();
            ruleManager = newRuleManager;
            writeLock.unlock();
        } catch (Exception e) {
            LOG.error("Failed to load file cache admission rules from file: {}", filePath, e);
        }
    }

    public void loadRules() {
        if (Config.file_cache_admission_control_json_dir == null
                    || Config.file_cache_admission_control_json_dir.isEmpty()) {
            LOG.warn("File cache admission JSON directory is not configured, admission control will be disabled.");
            return;
        }

        try {
            File ruleDir = new File(Config.file_cache_admission_control_json_dir);

            if (!ruleDir.exists()) {
                LOG.warn("File cache admission JSON directory does not exist: {}",
                        Config.file_cache_admission_control_json_dir);
                return;
            }

            if (!ruleDir.isDirectory()) {
                LOG.error("File cache admission JSON directory is not a directory: {}",
                        Config.file_cache_admission_control_json_dir);
                return;
            }

            File[] jsonFiles = ruleDir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".json");
                }
            });

            LOG.info("Found {} JSON files in admission rule directory: {}",
                    jsonFiles.length, Config.file_cache_admission_control_json_dir);

            List<AdmissionRule> allRules = new ArrayList<>();

            for (File jsonFile : jsonFiles) {
                List<AdmissionRule> loadedRules = RuleLoader.loadRulesFromFile(jsonFile.getPath());
                LOG.info("{} rules loaded successfully from JSON file: {}", loadedRules.size(),
                        jsonFile.getPath());

                allRules.addAll(loadedRules);
            }

            ConcurrentRuleManager newRuleManager = new ConcurrentRuleManager();
            newRuleManager.initialize(allRules);

            writeLock.lock();
            ruleManager = newRuleManager;
            writeLock.unlock();
        } catch (Exception e) {
            LOG.error("Failed to load file cache admission rules from directory: {}",
                    Config.file_cache_admission_control_json_dir, e);
        }
    }
}
