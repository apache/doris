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

import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

public class RestoreStmt extends AbstractBackupStmt {
    private static final String PROP_ALLOW_LOAD = "allow_load";
    private static final String PROP_REPLICATION_NUM = "replication_num";
    private static final String PROP_BACKUP_TIMESTAMP = "backup_timestamp";
    private static final String PROP_META_VERSION = "meta_version";
    private static final String PROP_RESERVE_REPLICA = "reserve_replica";
    private static final String PROP_RESERVE_DYNAMIC_PARTITION_ENABLE = "reserve_dynamic_partition_enable";

    private boolean allowLoad = false;
    private ReplicaAllocation replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
    private String backupTimestamp = null;
    private int metaVersion = -1;
    private boolean reserveReplica = false;
    private boolean reserveDynamicPartitionEnable = false;

    public RestoreStmt(LabelName labelName, String repoName, AbstractBackupTableRefClause restoreTableRefClause,
                       Map<String, String> properties) {
        super(labelName, repoName, restoreTableRefClause, properties);
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

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
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
        if (copiedProperties.containsKey(PROP_ALLOW_LOAD)) {
            if (copiedProperties.get(PROP_ALLOW_LOAD).equalsIgnoreCase("true")) {
                allowLoad = true;
            } else if (copiedProperties.get(PROP_ALLOW_LOAD).equalsIgnoreCase("false")) {
                allowLoad = false;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid allow load value: " + copiedProperties.get(PROP_ALLOW_LOAD));
            }
            copiedProperties.remove(PROP_ALLOW_LOAD);
        }

        // replication num
        this.replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(copiedProperties, "");
        if (this.replicaAlloc.isNotSet()) {
            this.replicaAlloc = ReplicaAllocation.DEFAULT_ALLOCATION;
        }
        // reserve replica
        if (copiedProperties.containsKey(PROP_RESERVE_REPLICA)) {
            if (copiedProperties.get(PROP_RESERVE_REPLICA).equalsIgnoreCase("true")) {
                reserveReplica = true;
            } else if (copiedProperties.get(PROP_RESERVE_REPLICA).equalsIgnoreCase("false")) {
                reserveReplica = false;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid reserve_replica value: " + copiedProperties.get(PROP_RESERVE_REPLICA));
            }
            copiedProperties.remove(PROP_RESERVE_REPLICA);
        }
        // reserve dynamic partition enable
        if (copiedProperties.containsKey(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE)) {
            if (copiedProperties.get(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE).equalsIgnoreCase("true")) {
                reserveDynamicPartitionEnable = true;
            } else if (copiedProperties.get(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE).equalsIgnoreCase("false")) {
                reserveDynamicPartitionEnable = false;
            } else {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid reserve dynamic partition enable value: "
                        + copiedProperties.get(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE));
            }
            copiedProperties.remove(PROP_RESERVE_DYNAMIC_PARTITION_ENABLE);
        }
        // backup timestamp
        if (copiedProperties.containsKey(PROP_BACKUP_TIMESTAMP)) {
            backupTimestamp = copiedProperties.get(PROP_BACKUP_TIMESTAMP);
            copiedProperties.remove(PROP_BACKUP_TIMESTAMP);
        } else {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Missing " + PROP_BACKUP_TIMESTAMP + " property");
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
}
