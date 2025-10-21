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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.backup.BackupJobInfo;
import org.apache.doris.backup.BackupMeta;
import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.LabelNameInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableRefInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * RestoreCommand
 */
public class RestoreCommand extends Command implements ForwardWithSync {
    public static final String PROP_RESERVE_REPLICA = "reserve_replica";
    public static final String PROP_RESERVE_COLOCATE = "reserve_colocate";
    public static final String PROP_RESERVE_DYNAMIC_PARTITION_ENABLE = "reserve_dynamic_partition_enable";
    public static final String PROP_CLEAN_TABLES = "clean_tables";
    public static final String PROP_CLEAN_PARTITIONS = "clean_partitions";
    public static final String PROP_ATOMIC_RESTORE = "atomic_restore";
    public static final String PROP_FORCE_REPLACE = "force_replace";
    public static final String PROP_STORAGE_VAULT_NAME = "storage_vault_name";

    private static final Logger LOG = LogManager.getLogger(RestoreCommand.class);
    private static final String PROP_TIMEOUT = "timeout";
    private static final long MIN_TIMEOUT_MS = 600 * 1000L;
    private static final String PROP_ALLOW_LOAD = "allow_load";
    private static final String PROP_BACKUP_TIMESTAMP = "backup_timestamp";
    private static final String PROP_META_VERSION = "meta_version";
    private static final String PROP_IS_BEING_SYNCED = PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED;

    private boolean allowLoad = false;
    private ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
    private String backupTimestamp = null;
    private int metaVersion = -1;
    private boolean reserveReplica = false;
    private boolean reserveColocate = false;
    private boolean reserveDynamicPartitionEnable = false;
    private boolean isLocal = false;
    private boolean isBeingSynced = false;
    private boolean isCleanTables = false;
    private boolean isCleanPartitions = false;
    private boolean isAtomicRestore = false;
    private boolean isForceReplace = false;

    private final LabelNameInfo labelNameInfo;
    private final String repoName;
    private final List<TableRefInfo> tableRefInfos;
    private final Map<String, String> properties;
    private final boolean isExclude;
    private long timeoutMs;
    private BackupMeta meta = null;
    private BackupJobInfo jobInfo = null;
    private String storageVaultName = null;

    /**
     * BackupCommand
     */
    public RestoreCommand(LabelNameInfo labelNameInfo,
                          String repoName,
                          List<TableRefInfo> tableRefInfos,
                          Map<String, String> properties,
                          boolean isExclude) {
        super(PlanType.RESTORE_COMMAND);
        Objects.requireNonNull(labelNameInfo, "labelNameInfo is null");
        Objects.requireNonNull(repoName, "repoName is null");
        Objects.requireNonNull(tableRefInfos, "tableRefInfos is null");
        Objects.requireNonNull(properties, "properties is null");
        this.labelNameInfo = labelNameInfo;
        this.repoName = repoName;
        this.tableRefInfos = tableRefInfos;
        this.properties = properties;
        this.isExclude = isExclude;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getBackupHandler().process(this);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException, DdlException {
        if (repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            isLocal = true;
            if (jobInfo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "restore from the local repo via SQL call is not supported");
            }
        }

        labelNameInfo.validate(ctx);

        // user need database level privilege(not table level),
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                labelNameInfo.getDb(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }

        analyzeTableRefInfo();
        analyzeProperties();
    }

    private void analyzeTableRefInfo() throws AnalysisException {
        if (tableRefInfos.isEmpty()) {
            return;
        }
        checkTableRefWithoutDatabase();
        updateTableRefInfos();

        // check if alias is duplicated
        Set<String> aliasSet = Sets.newHashSet();
        for (TableRefInfo tableRefInfo : tableRefInfos) {
            aliasSet.add(tableRefInfo.getTableNameInfo().getTbl());
        }

        for (TableRefInfo tableRefInfo : tableRefInfos) {
            if (tableRefInfo.hasAlias() && !aliasSet.add(tableRefInfo.getTableAlias())) {
                throw new AnalysisException("Duplicated alias name: " + tableRefInfo.getTableAlias());
            }
        }
    }

    private void checkTableRefWithoutDatabase() throws AnalysisException {
        for (TableRefInfo tableRef : tableRefInfos) {
            if (!Strings.isNullOrEmpty(tableRef.getTableNameInfo().getDb())) {
                throw new AnalysisException("Cannot specify database name on backup objects: "
                    + tableRef.getTableNameInfo().getTbl() + ". Specify database name before label");
            }
            // set db name because we can not persist empty string when writing bdbje log
            tableRef.getTableNameInfo().setDb(labelNameInfo.getDb());
        }
    }

    private void updateTableRefInfos() throws AnalysisException {
        Map<String, TableRefInfo> tblPartsMap;
        if (GlobalVariable.lowerCaseTableNames == 0) {
            // comparisons case sensitive
            tblPartsMap = Maps.newTreeMap();
        } else {
            tblPartsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        }

        for (TableRefInfo tableRefInfo : tableRefInfos) {
            String tableName = tableRefInfo.getTableNameInfo().getTbl();
            if (!tblPartsMap.containsKey(tableName)) {
                tblPartsMap.put(tableName, tableRefInfo);
            } else {
                throw new AnalysisException("Duplicated table: " + tableName);
            }
        }

        // update table ref
        tableRefInfos.clear();
        tableRefInfos.addAll(tblPartsMap.values());
        if (LOG.isDebugEnabled()) {
            LOG.debug("table refs after normalization: {}", Joiner.on(",").join(tableRefInfos));
        }
    }

    /**
     * analyzeProperties
     */
    public void analyzeProperties() throws AnalysisException {
        // timeout
        if (properties.containsKey("timeout")) {
            try {
                timeoutMs = Long.valueOf(properties.get(PROP_TIMEOUT));
            } catch (NumberFormatException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid timeout format: " + properties.get(PROP_TIMEOUT));
            }

            if (timeoutMs * 1000 < MIN_TIMEOUT_MS) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR, "timeout must be at least 10 min");
            }

            timeoutMs = timeoutMs * 1000;
            properties.remove(PROP_TIMEOUT);
        } else {
            timeoutMs = Config.backup_job_default_timeout_ms;
        }

        Map<String, String> copiedProperties = Maps.newHashMap(properties);

        // allow load
        allowLoad = eatBooleanProperty(copiedProperties, PROP_ALLOW_LOAD, allowLoad);

        // replication num
        this.replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(copiedProperties, "");
        if (this.replicaAlloc.isNotSet()) {
            this.replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        }
        // reserve replica
        reserveReplica = eatBooleanProperty(copiedProperties, PROP_RESERVE_REPLICA, reserveReplica);
        // force set reserveReplica to false, do not keep the origin allocation
        if (reserveReplica && !Config.force_olap_table_replication_allocation.isEmpty()) {
            reserveReplica = false;
        }

        // reserve colocate
        reserveColocate = eatBooleanProperty(copiedProperties, PROP_RESERVE_COLOCATE, reserveColocate);

        // reserve dynamic partition enable
        reserveDynamicPartitionEnable = eatBooleanProperty(
            copiedProperties, PROP_RESERVE_DYNAMIC_PARTITION_ENABLE, reserveDynamicPartitionEnable);

        // backup timestamp
        if (copiedProperties.containsKey(PROP_BACKUP_TIMESTAMP)) {
            backupTimestamp = copiedProperties.get(PROP_BACKUP_TIMESTAMP);
            copiedProperties.remove(PROP_BACKUP_TIMESTAMP);
        } else {
            if (!isLocal) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Missing " + PROP_BACKUP_TIMESTAMP + " property");
            }
        }

        // meta version
        if (copiedProperties.containsKey(PROP_META_VERSION)) {
            try {
                metaVersion = Integer.valueOf(copiedProperties.get(PROP_META_VERSION));
            } catch (NumberFormatException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid meta version format: " + copiedProperties.get(PROP_META_VERSION));
            }
            copiedProperties.remove(PROP_META_VERSION);
        }

        // storage vault name
        if (Config.isCloudMode() && ((CloudEnv) Env.getCurrentEnv()).getEnableStorageVault()) {
            Pair<String, String> info = PropertyAnalyzer.analyzeStorageVault(copiedProperties,
                    Env.getCurrentInternalCatalog().getDbOrAnalysisException(labelNameInfo.getDb()));
            Preconditions.checkArgument(StringUtils.isNumeric(info.second),
                    "Invalid storage vault id :%s", info.second);
            // Check if user has storage vault usage privilege
            ConnectContext context = ConnectContext.get();
            if (context != null && !Env.getCurrentEnv().getAccessManager()
                    .checkStorageVaultPriv(context.getCurrentUserIdentity(), info.first, PrivPredicate.USAGE)) {
                throw new AnalysisException(String.format("USAGE denied to user '%s'@'%s' for storage vault '%s'",
                        context.getQualifiedUser(), context.getRemoteIP(), info.first));
            }
            storageVaultName = info.first;
        }

        // is being synced
        isBeingSynced = eatBooleanProperty(copiedProperties, PROP_IS_BEING_SYNCED, isBeingSynced);

        // is clean tables
        isCleanTables = eatBooleanProperty(copiedProperties, PROP_CLEAN_TABLES, isCleanTables);

        // is clean partitions
        isCleanPartitions = eatBooleanProperty(copiedProperties, PROP_CLEAN_PARTITIONS, isCleanPartitions);

        // is atomic restore
        isAtomicRestore = eatBooleanProperty(copiedProperties, PROP_ATOMIC_RESTORE, isAtomicRestore);

        // is force replace
        isForceReplace = eatBooleanProperty(copiedProperties, PROP_FORCE_REPLACE, isForceReplace);

        if (!copiedProperties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Unknown restore job properties: " + copiedProperties.keySet());
        }
    }

    private boolean eatBooleanProperty(Map<String, String> copiedProperties, String name, boolean defaultValue)
            throws AnalysisException {
        boolean retval = defaultValue;
        if (copiedProperties.containsKey(name)) {
            String value = copiedProperties.get(name);
            if (value.equalsIgnoreCase("true")) {
                retval = true;
            } else if (value.equalsIgnoreCase("false")) {
                retval = false;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid boolean property " + name + " value: " + value);
            }
            copiedProperties.remove(name);
        }
        return retval;
    }

    public List<TableRefInfo> getTableRefInfos() {
        return tableRefInfos;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public boolean isExclude() {
        return isExclude;
    }

    public String getRepoName() {
        return repoName;
    }

    public String getLabel() {
        return labelNameInfo.getLabel();
    }

    public String getDbName() {
        return labelNameInfo.getDb();
    }

    public boolean allowLoad() {
        return allowLoad;
    }

    public String getBackupTimestamp() {
        return backupTimestamp;
    }

    public ReplicaAllocation getReplicaAlloc() {
        return replicaAlloc;
    }

    public String getStorageVaultName() {
        return storageVaultName;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    public boolean reserveReplica() {
        return reserveReplica;
    }

    public boolean reserveColocate() {
        return reserveColocate;
    }

    public boolean reserveDynamicPartitionEnable() {
        return reserveDynamicPartitionEnable;
    }

    public boolean isBeingSynced() {
        return isBeingSynced;
    }

    public boolean isCleanTables() {
        return isCleanTables;
    }

    public boolean isCleanPartitions() {
        return isCleanPartitions;
    }

    public boolean isAtomicRestore() {
        return isAtomicRestore;
    }

    public boolean isForceReplace() {
        return isForceReplace;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public BackupMeta getMeta() {
        return meta;
    }

    public void setMeta(BackupMeta meta) {
        this.meta = meta;
    }

    public BackupJobInfo getJobInfo() {
        return jobInfo;
    }

    public void setJobInfo(BackupJobInfo jobInfo) {
        this.jobInfo = jobInfo;
    }

    public void setIsBeingSynced() {
        properties.put(PROP_IS_BEING_SYNCED, "true");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRestoreCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.RESTORE;
    }
}
