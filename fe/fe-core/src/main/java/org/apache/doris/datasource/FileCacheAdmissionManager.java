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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
            this.userIdentity = userIdentity;
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

    public static class ConcurrentRuleCollection {
        private Boolean excludeGlobal = false;
        private final Set<String> excludeCatalogRules = new HashSet<>();
        private final Map<String, Set<String>> excludeDatabaseRules = new HashMap<>();
        private final Map<String, Set<String>> excludeTableRules = new HashMap<>();

        private Boolean includeGlobal = false;
        private final Set<String> includeCatalogRules = new HashSet<>();
        private final Map<String, Set<String>> includeDatabaseRules = new HashMap<>();
        private final Map<String, Set<String>> includeTableRules = new HashMap<>();

        static List<String> reasons = new ArrayList<>(Arrays.asList(
                "common catalog-level blacklist rule",      // 0
                "common catalog-level whitelist rule",      // 1
                "common database-level blacklist rule",     // 2
                "common database-level whitelist rule",     // 3
                "common table-level blacklist rule",        // 4
                "common table-level whitelist rule",        // 5
                "user global-level blacklist rule",         // 6
                "user global-level whitelist rule",         // 7
                "user catalog-level blacklist rule",        // 8
                "user catalog-level whitelist rule",        // 9
                "user database-level blacklist rule",       // 10
                "user database-level whitelist rule",       // 11
                "user table-level blacklist rule",          // 12
                "user table-level whitelist rule",          // 13
                "default rule"                              // 14
        ));

        public boolean isAllowed(String userIdentity, String catalog, String database, String table,
                                 AtomicReference<String> reason) {

            String catalogTable = catalog + "." + database;

            if (containsKeyValue(excludeTableRules, table, catalogTable)) {
                reason.set(reasons.get(4));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeTableRules, table, catalogTable)) {
                reason.set(reasons.get(5));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(excludeDatabaseRules, database, catalog)) {
                reason.set(reasons.get(2));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeDatabaseRules, database, catalog)) {
                reason.set(reasons.get(3));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (excludeCatalogRules.contains(catalog)) {
                reason.set(reasons.get(0));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (includeCatalogRules.contains(catalog)) {
                reason.set(reasons.get(1));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }

            // TODO: Implementing partition-level rules

            reason.set(reasons.get(14));
            logAdmission(Config.file_cache_admission_control_default_allow,
                    userIdentity, catalog, database, table, reason.get());
            return Config.file_cache_admission_control_default_allow;
        }

        public boolean isAllowed(ConcurrentRuleCollection userCollection, String userIdentity, String catalog,
                                 String database, String table, AtomicReference<String> reason) {

            String catalogTable = catalog + "." + database;

            if (containsKeyValue(excludeTableRules, table, catalogTable)) {
                reason.set(reasons.get(4));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(userCollection.excludeTableRules, table, catalogTable)) {
                reason.set(reasons.get(12));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeTableRules, table, catalogTable)) {
                reason.set(reasons.get(5));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(userCollection.includeTableRules, table, catalogTable)) {
                reason.set(reasons.get(13));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(excludeDatabaseRules, database, catalog)) {
                reason.set(reasons.get(2));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(userCollection.excludeDatabaseRules, database, catalog)) {
                reason.set(reasons.get(10));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (containsKeyValue(includeDatabaseRules, database, catalog)) {
                reason.set(reasons.get(3));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (containsKeyValue(userCollection.includeDatabaseRules, database, catalog)) {
                reason.set(reasons.get(11));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (excludeCatalogRules.contains(catalog)) {
                reason.set(reasons.get(0));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (userCollection.excludeCatalogRules.contains(catalog)) {
                reason.set(reasons.get(8));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (includeCatalogRules.contains(catalog)) {
                reason.set(reasons.get(1));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (userCollection.includeCatalogRules.contains(catalog)) {
                reason.set(reasons.get(9));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }
            if (userCollection.excludeGlobal) {
                reason.set(reasons.get(6));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (userCollection.includeGlobal) {
                reason.set(reasons.get(7));
                logAdmission(true, userIdentity, catalog, database, table, reason.get());
                return true;
            }

            // TODO: Implementing partition-level rules

            reason.set(reasons.get(14));
            logAdmission(Config.file_cache_admission_control_default_allow,
                    userIdentity, catalog, database, table, reason.get());
            return Config.file_cache_admission_control_default_allow;
        }

        private boolean containsKeyValue(Map<String, Set<String>> map, String key, String value) {
            Set<String> set = map.get(key);
            return set != null && set.contains(value);
        }

        private void logAdmission(boolean allowed, String userIdentity,
                                  String catalog, String database,
                                  String table, String reason) {
            String status = allowed ? "allowed" : "denied";

            String logMessage = String.format(
                    "File cache request %s by %s, user_identity: %s, "
                        + "catalog: %s, database: %s, table: %s",
                    status, reason, userIdentity, catalog, database, table);

            LOG.debug(logMessage);
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
        private static final int PARTITION_COUNT = 58; // A-Z + a-z + 其他字符
        private final List<Map<String, ConcurrentRuleCollection>> maps;
        private final ConcurrentRuleCollection commonCollection;

        static List<String> otherReasons = new ArrayList<>(Arrays.asList(
                "empty user_identity",
                "invalid user_identity"
        ));

        public ConcurrentRuleManager() {
            maps = new ArrayList<>(PARTITION_COUNT);
            commonCollection = new ConcurrentRuleCollection();

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
                        k -> new ConcurrentRuleCollection()).add(rulePattern);
            }
        }

        public boolean isAllowed(String userIdentity, String catalog, String database, String table,
                                 AtomicReference<String> reason) {
            if (userIdentity.isEmpty()) {
                reason.set(otherReasons.get(0));
                logDefaultAdmission(userIdentity, catalog, database, table, reason.get());
                return Config.file_cache_admission_control_default_allow;
            }

            char firstChar = userIdentity.charAt(0);
            if (!Character.isAlphabetic(firstChar)) {
                reason.set(otherReasons.get(1));
                logDefaultAdmission(userIdentity, catalog, database, table, reason.get());
                return Config.file_cache_admission_control_default_allow;
            }

            int index = getIndex(firstChar);
            ConcurrentRuleCollection collection = maps.get(index).get(userIdentity);
            if (collection == null) {
                return commonCollection.isAllowed(userIdentity, catalog, database, table, reason);
            } else {
                return commonCollection.isAllowed(collection, userIdentity, catalog, database, table, reason);
            }
        }

        private void logDefaultAdmission(String userIdentity, String catalog,
                                         String database, String table, String reason) {
            boolean allowed = Config.file_cache_admission_control_default_allow;
            String decision = allowed ? "allowed" : "denied";

            String logMessage = String.format(
                    "File cache request %s by file_cache_admission_control_default_allow, "
                    + "user_identity: %s, catalog: %s, database: %s, table: %s, reason: %s",
                    decision, userIdentity, catalog, database, table, reason);

            LOG.debug(logMessage);
        }
    }

    private ConcurrentRuleManager ruleManager;

    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    private static final FileCacheAdmissionManager INSTANCE = new FileCacheAdmissionManager();

    private ScheduledExecutorService executorService;

    long lastLoadedTime;

    public FileCacheAdmissionManager() {
        this.ruleManager = new ConcurrentRuleManager();
        this.lastLoadedTime = 0;
    }

    public static FileCacheAdmissionManager getInstance() {
        return INSTANCE;
    }

    public void initialize(List<AdmissionRule> rules) {
        ruleManager.initialize(rules);
    }

    public boolean isAllowed(String userIdentity, String catalog, String database, String table,
                             AtomicReference<String> reason) {
        readLock.lock();
        boolean isAllowed = ruleManager.isAllowed(userIdentity, catalog, database, table, reason);
        readLock.unlock();

        return isAllowed;
    }

    public void loadOnStartup() {
        LOG.info("Loading file cache admission rules...");

        loadRules();
        startRefreshTask();
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
        if (Config.file_cache_admission_control_json_file_path == null
                || Config.file_cache_admission_control_json_file_path.isEmpty()) {
            LOG.warn("File cache admission JSON file path is not configured, admission control will be disabled.");
            return;
        }

        try {
            File ruleFile = new File(Config.file_cache_admission_control_json_file_path);

            if (!ruleFile.exists()) {
                LOG.warn("File cache admission JSON file does not exist: {}",
                        Config.file_cache_admission_control_json_file_path);
                return;
            }

            long lastModified = ruleFile.lastModified();
            if (lastModified <= lastLoadedTime) {
                LOG.info("File cache admission rules file has not been modified since last load, skip loading.");
                return;
            }

            List<AdmissionRule> loadedRules = RuleLoader.loadRulesFromFile(
                    Config.file_cache_admission_control_json_file_path);
            LOG.info("{} rules loaded successfully from file: {}", loadedRules.size(),
                    Config.file_cache_admission_control_json_file_path);

            ConcurrentRuleManager newRuleManager = new ConcurrentRuleManager();
            newRuleManager.initialize(loadedRules);

            writeLock.lock();
            lastLoadedTime = lastModified;
            ruleManager = newRuleManager;
            writeLock.unlock();
        } catch (Exception e) {
            LOG.error("Failed to load file cache admission rules from file: {}",
                    Config.file_cache_admission_control_json_file_path, e);
        }
    }

    private void startRefreshTask() {
        int interval = Config.file_cache_admission_control_fresh_interval_s;
        if (interval <= 0) {
            LOG.info("File cache admission control refresh interval is {} (<=0), refresh task will not be started.",
                    interval);
            return;
        }

        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "file-cache-admission-rule-refresh");
            t.setDaemon(true);
            return t;
        });

        executorService.scheduleAtFixedRate(() -> {
            LOG.info("Refreshing file cache admission rules...");
            loadRules();
        }, interval, interval, TimeUnit.SECONDS);

        LOG.info("Started refreshing task, interval: {} seconds", interval);
    }

    public void shutdown() {
        if (executorService != null) {
            LOG.info("Starting shutdown refreshing executorService");
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                        LOG.warn("Refreshing executorService did not terminate");
                    }
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            LOG.info("Refreshing executorService shutdown completed");
        }
    }
}
