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

package org.apache.doris.analysis;

import org.apache.doris.backup.Repository;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class RestoreStmt extends AbstractBackupStmt implements NotFallbackInParser {
    private static final String PROP_ALLOW_LOAD = "allow_load";
    private static final String PROP_BACKUP_TIMESTAMP = "backup_timestamp";
    private static final String PROP_META_VERSION = "meta_version";
    private static final String PROP_IS_BEING_SYNCED = PropertyAnalyzer.PROPERTIES_IS_BEING_SYNCED;

    public static final String PROP_RESERVE_REPLICA = "reserve_replica";
    public static final String PROP_RESERVE_DYNAMIC_PARTITION_ENABLE = "reserve_dynamic_partition_enable";
    public static final String PROP_CLEAN_TABLES = "clean_tables";
    public static final String PROP_CLEAN_PARTITIONS = "clean_partitions";
    public static final String PROP_ATOMIC_RESTORE = "atomic_restore";

    private boolean allowLoad = false;
    private ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
    private String backupTimestamp = null;
    private int metaVersion = -1;
    private boolean reserveReplica = false;
    private boolean reserveDynamicPartitionEnable = false;
    private boolean isLocal = false;
    private boolean isBeingSynced = false;
    private boolean isCleanTables = false;
    private boolean isCleanPartitions = false;
    private boolean isAtomicRestore = false;
    private byte[] meta = null;
    private byte[] jobInfo = null;

    public RestoreStmt(LabelName labelName, String repoName, AbstractBackupTableRefClause restoreTableRefClause,
            Map<String, String> properties) {
        super(labelName, repoName, restoreTableRefClause, properties);
    }

    public RestoreStmt(LabelName labelName, String repoName, AbstractBackupTableRefClause restoreTableRefClause,
            Map<String, String> properties, byte[] meta, byte[] jobInfo) {
        super(labelName, repoName, restoreTableRefClause, properties);
        this.meta = meta;
        this.jobInfo = jobInfo;
    }

    public boolean allowLoad() {
        return allowLoad;
    }

    public ReplicaAllocation getReplicaAlloc() {
        return replicaAlloc;
    }

    public String getBackupTimestamp() {
        return backupTimestamp;
    }

    public int getMetaVersion() {
        return metaVersion;
    }

    public boolean reserveReplica() {
        return reserveReplica;
    }

    public boolean reserveDynamicPartitionEnable() {
        return reserveDynamicPartitionEnable;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public byte[] getMeta() {
        return meta;
    }

    public byte[] getJobInfo() {
        return jobInfo;
    }

    public void setIsBeingSynced() {
        setProperty(PROP_IS_BEING_SYNCED, "true");
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

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (repoName.equals(Repository.KEEP_ON_LOCAL_REPO_NAME)) {
            isLocal = true;
            if (jobInfo == null) {
                ErrorReport.reportDdlException(ErrorCode.ERR_COMMON_ERROR,
                        "restore from the local repo via SQL call is not supported");
            }
        }
        super.analyze(analyzer);
    }

    @Override
    protected void customAnalyzeTableRefClause() throws AnalysisException {
        // check if alias is duplicated
        Set<String> aliasSet = Sets.newHashSet();
        for (TableRef tblRef : abstractBackupTableRefClause.getTableRefList()) {
            aliasSet.add(tblRef.getName().getTbl());
        }

        for (TableRef tblRef : abstractBackupTableRefClause.getTableRefList()) {
            if (tblRef.hasExplicitAlias() && !aliasSet.add(tblRef.getExplicitAlias())) {
                throw new AnalysisException("Duplicated alias name: " + tblRef.getExplicitAlias());
            }
        }
    }

    @Override
    public void analyzeProperties() throws AnalysisException {
        super.analyzeProperties();

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

        // is being synced
        isBeingSynced = eatBooleanProperty(copiedProperties, PROP_IS_BEING_SYNCED, isBeingSynced);

        // is clean tables
        isCleanTables = eatBooleanProperty(copiedProperties, PROP_CLEAN_TABLES, isCleanTables);

        // is clean partitions
        isCleanPartitions = eatBooleanProperty(copiedProperties, PROP_CLEAN_PARTITIONS, isCleanPartitions);

        // is atomic restore
        isAtomicRestore = eatBooleanProperty(copiedProperties, PROP_ATOMIC_RESTORE, isAtomicRestore);

        if (!copiedProperties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Unknown restore job properties: " + copiedProperties.keySet());
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("RESTORE SNAPSHOT ").append(labelName.toSql());
        sb.append("\n").append("FROM ").append(repoName).append("\n");
        if (abstractBackupTableRefClause != null) {
            sb.append(abstractBackupTableRefClause.toSql()).append("\n");
        }
        sb.append("PROPERTIES\n(");
        sb.append(new PrintableMap<String, String>(properties, " = ", true, true));
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.RESTORE;
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
}
