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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class FileCacheAdmissionManager {
    private static final Logger LOG = LogManager.getLogger(FileCacheAdmissionManager.class);

    public enum RuleType {
        EXCLUDE,     // 0
        INCLUDE      // 1
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

        public RulePattern(RulePattern other) {
            this.id = other.id;
            this.userIdentity = other.userIdentity;
            this.catalog = other.catalog;
            this.database = other.database;
            this.table = other.table;
            this.partitionPattern = other.partitionPattern;
            this.ruleType = other.ruleType;
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
        private final RulePattern rulePattern;
        private final boolean enabled;
        private final long updatedTime;

        public AdmissionRule(long id, String userIdentity, String catalog,
                             String database, String table, String partitionPattern,
                             RuleType ruleType, boolean enabled, long updatedTime) {
            this.rulePattern = new RulePattern(id, userIdentity, catalog, database, table, partitionPattern,
                    ruleType);
            this.enabled = enabled;
            this.updatedTime = updatedTime;
        }

        public RulePattern getRulePattern() {
            return rulePattern;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public long getUpdatedTime() {
            return updatedTime;
        }
    }

    public static class ConcurrentRuleCollection {
        private AtomicBoolean excludeGlobal = new AtomicBoolean(false);
        private final Set<String> excludeCatalogRules = ConcurrentHashMap.newKeySet();
        private final Map<String, Set<String>> excludeDatabaseRules = new ConcurrentHashMap<>();
        private final Map<String, Set<String>> excludeTableRules = new ConcurrentHashMap<>();

        private AtomicBoolean includeGlobal = new AtomicBoolean(false);
        private final Set<String> includeCatalogRules = ConcurrentHashMap.newKeySet();
        private final Map<String, Set<String>> includeDatabaseRules = new ConcurrentHashMap<>();
        private final Map<String, Set<String>> includeTableRules = new ConcurrentHashMap<>();

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
            if (userCollection.excludeGlobal.get()) {
                reason.set(reasons.get(6));
                logAdmission(false, userIdentity, catalog, database, table, reason.get());
                return false;
            }
            if (userCollection.includeGlobal.get()) {
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
                        excludeGlobal.set(true);
                    } else {
                        includeGlobal.set(true);
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

        public void remove(RulePattern rulePattern) {
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
                        excludeGlobal.set(false);
                    } else {
                        includeGlobal.set(false);
                    }
                    break;
                case CATALOG:
                    catalogRules.remove(rulePattern.getCatalog());
                    break;
                case DATABASE:
                    Set<String> catalogSet = databaseRules.get(rulePattern.getDatabase());
                    if (catalogSet != null) {
                        catalogSet.remove(rulePattern.getCatalog());
                        if (catalogSet.isEmpty()) {
                            databaseRules.remove(rulePattern.getDatabase());
                        }
                    }
                    break;
                case TABLE:
                    Set<String> catalogDbSet = tableRules.get(rulePattern.getTable());
                    if (catalogDbSet != null) {
                        String catalogDbKey = rulePattern.getCatalog() + "." + rulePattern.getDatabase();
                        catalogDbSet.remove(catalogDbKey);
                        if (catalogDbSet.isEmpty()) {
                            tableRules.remove(rulePattern.getTable());
                        }
                    }
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
        private final Map<Long, RulePattern> idToRulePattern = new ConcurrentHashMap<>();

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
                if (!rule.isEnabled()) {
                    continue;
                }

                if (rule.getRulePattern().getUserIdentity().isEmpty()) {
                    commonCollection.add(rule.getRulePattern());
                    idToRulePattern.put(rule.getRulePattern().getId(), new RulePattern(rule.getRulePattern()));
                    continue;
                }

                char firstChar = rule.getRulePattern().getUserIdentity().charAt(0);
                if (!Character.isAlphabetic(firstChar)) {
                    continue;
                }

                int index = getIndex(firstChar);
                maps.get(index).computeIfAbsent(rule.getRulePattern().getUserIdentity(),
                        k -> new ConcurrentRuleCollection()).add(rule.getRulePattern());
                idToRulePattern.put(rule.getRulePattern().getId(), new RulePattern(rule.getRulePattern()));
            }
        }

        public void remove(RulePattern rulePattern) {
            String userIdentity = rulePattern.getUserIdentity();
            if (userIdentity.isEmpty()) {
                commonCollection.remove(rulePattern);
                idToRulePattern.remove(rulePattern.getId());
                return;
            }

            char firstChar = userIdentity.charAt(0);
            if (!Character.isAlphabetic(firstChar)) {
                return;
            }

            int index = getIndex(firstChar);
            maps.get(index).get(userIdentity).remove(rulePattern);
            idToRulePattern.remove(rulePattern.getId());
        }

        public void add(RulePattern rulePattern) {
            String userIdentity = rulePattern.getUserIdentity();
            if (userIdentity.isEmpty()) {
                commonCollection.add(rulePattern);
                idToRulePattern.put(rulePattern.getId(), new RulePattern(rulePattern));
                return;
            }

            char firstChar = userIdentity.charAt(0);
            if (!Character.isAlphabetic(firstChar)) {
                return;
            }

            int index = getIndex(firstChar);
            maps.get(index).computeIfAbsent(rulePattern.getUserIdentity(),
                    k -> new ConcurrentRuleCollection()).add(rulePattern);
            idToRulePattern.put(rulePattern.getId(), new RulePattern(rulePattern));
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

    private final ConcurrentRuleManager ruleManager;

    private volatile long maxUpdatedTime;
    private static final FileCacheAdmissionManager INSTANCE = new FileCacheAdmissionManager();

    private ScheduledExecutorService executorService;

    public FileCacheAdmissionManager() {
        this.ruleManager = new ConcurrentRuleManager();
        this.maxUpdatedTime = 0;
    }

    public static FileCacheAdmissionManager getInstance() {
        return INSTANCE;
    }

    public void initialize(List<AdmissionRule> rules, long maxUpdatedTime) {
        ruleManager.initialize(rules);
        this.maxUpdatedTime = Math.max(this.maxUpdatedTime, maxUpdatedTime);
    }

    public void update(List<AdmissionRule> rules, long maxUpdatedTime) {
        for (AdmissionRule rule : rules) {
            RulePattern oldRulePattern = ruleManager.idToRulePattern.get(rule.getRulePattern().getId());
            if (oldRulePattern != null) {
                ruleManager.remove(oldRulePattern);
            }
            if (rule.isEnabled()) {
                ruleManager.add(rule.getRulePattern());
            }
        }
        this.maxUpdatedTime = Math.max(this.maxUpdatedTime, maxUpdatedTime);
    }

    public boolean isAllowed(String userIdentity, String catalog, String database, String table,
                             AtomicReference<String> reason) {
        return ruleManager.isAllowed(userIdentity, catalog, database, table, reason);
    }

    public void loadOnStartup() {
        try {
            LOG.info("Loading file cache admission policy...");

            loadRules(false);

            if (Config.file_cache_admission_control_fresh_interval_s > 0) {
                startRefreshTask();
            }

            LOG.info("File cache admission policy loaded successfully");
        } catch (Exception e) {
            LOG.error("Failed to load file cache admission policy", e);
        }
    }

    private void loadRules(boolean isUpdate) throws SQLException {
        String sql = "SELECT id, user_identity, catalog_name, database_name, "
                + "table_name, partition_pattern, rule_type, enabled, "
                + "UNIX_TIMESTAMP(updated_time) FROM admission_policy";

        if (isUpdate && maxUpdatedTime > 0) {
            sql += " WHERE updated_time > FROM_UNIXTIME(" + maxUpdatedTime + ")";
        }

        LOG.info("Loading rules with SQL: {}", sql);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)) {

            List<AdmissionRule> rules = new ArrayList<>();
            long maxUpdatedTime = 0;

            while (rs.next()) {
                long id = rs.getLong(1);
                String userIdentity = rs.getString(2);
                String catalog = rs.getString(3);
                String database = rs.getString(4);
                String table = rs.getString(5);
                String partitionPattern = rs.getString(6);
                int ruleTypeValue = rs.getInt(7);
                RuleType ruleType = RuleType.values()[Math.max(0, Math.min(ruleTypeValue, 1))];
                boolean enabled = rs.getBoolean(8);
                long updatedTime = rs.getLong(9);
                maxUpdatedTime = Math.max(maxUpdatedTime, updatedTime);

                AdmissionRule rule = new AdmissionRule(
                        id, userIdentity, catalog, database, table,
                        partitionPattern, ruleType, enabled, updatedTime
                );
                rules.add(rule);
            }

            if (isUpdate) {
                LOG.info(rules.size() + " rules updated successfully");
                update(rules, maxUpdatedTime);
            } else {
                LOG.info(rules.size() + " rules loaded successfully");
                initialize(rules, maxUpdatedTime);
            }
        }
    }

    private Connection getConnection() throws SQLException {
        String url = String.format("jdbc:mysql://%s:%d/%s",
                Config.file_cache_admission_control_mysql_host,
                Config.file_cache_admission_control_mysql_port,
                Config.file_cache_admission_control_mysql_database);

        return DriverManager.getConnection(url,
                Config.file_cache_admission_control_mysql_user,
                Config.file_cache_admission_control_mysql_password);
    }

    private void startRefreshTask() {
        int interval = Config.file_cache_admission_control_fresh_interval_s;
        if (interval <= 0) {
            return;
        }

        executorService = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "file-cache-admission-policy-refresh");
            t.setDaemon(true);
            return t;
        });

        executorService.scheduleAtFixedRate(() -> {
            try {
                LOG.info("Refreshing file cache admission policy...");
                loadRules(true);
            } catch (Exception e) {
                LOG.warn("Failed to refresh file cache admission policy", e);
            }
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
