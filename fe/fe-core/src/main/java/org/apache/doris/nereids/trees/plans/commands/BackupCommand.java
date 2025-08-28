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
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * BackupCommand
 */
public class BackupCommand extends Command implements ForwardWithSync {
    public static final String PROP_CONTENT = "content";
    private static final Logger LOG = LogManager.getLogger(BackupCommand.class);
    private static final String PROP_TIMEOUT = "timeout";
    private static final long MIN_TIMEOUT_MS = 600 * 1000L;
    private static final String PROP_TYPE = "type";

    /**
     * BackupType
     */
    public enum BackupType {
        INCREMENTAL, FULL
    }

    /**
     * BackupContent
     */
    public enum BackupContent {
        METADATA_ONLY, ALL
    }

    private BackupType type = BackupType.FULL;
    private BackupContent content = BackupContent.ALL;

    private final LabelNameInfo labelNameInfo;
    private final String repoName;
    private final List<TableRefInfo> tableRefInfos;
    private final Map<String, String> properties;
    private final boolean isExclude;

    private long timeoutMs;

    /**
     * BackupCommand
     */
    public BackupCommand(LabelNameInfo labelNameInfo,
                         String repoName,
                         List<TableRefInfo> tableRefInfos,
                         Map<String, String> properties,
                         boolean isExclude) {
        super(PlanType.BACKUP_COMMAND);
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
    public void validate(ConnectContext ctx) throws AnalysisException {
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
        // tbl refs can not set alias in backup
        for (TableRefInfo tableRefInfo : tableRefInfos) {
            if (tableRefInfo.hasAlias()) {
                throw new AnalysisException("Can not set alias for table in Backup Command: " + tableRefInfo);
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

    private void analyzeProperties() throws AnalysisException {
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
        // type
        String typeProp = copiedProperties.get(PROP_TYPE);
        if (typeProp != null) {
            try {
                type = BackupType.valueOf(typeProp.toUpperCase());
            } catch (Exception e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid backup job type: " + typeProp);
            }
            copiedProperties.remove(PROP_TYPE);
        }
        // content
        String contentProp = copiedProperties.get(PROP_CONTENT);
        if (contentProp != null) {
            try {
                content = BackupContent.valueOf(contentProp.toUpperCase());
            } catch (IllegalArgumentException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid backup job content:" + contentProp);
            }
            copiedProperties.remove(PROP_CONTENT);
        }

        if (!copiedProperties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Unknown backup job properties: " + copiedProperties.keySet());
        }
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

    public BackupType getBackupType() {
        return type;
    }

    public BackupContent getContent() {
        return content;
    }

    public String getLabel() {
        return labelNameInfo.getLabel();
    }

    public String getDbName() {
        return labelNameInfo.getDb();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitBackupCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.BACKUP;
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("BackupCommand not supported in cloud mode");
        throw new DdlException("denied");
    }
}
