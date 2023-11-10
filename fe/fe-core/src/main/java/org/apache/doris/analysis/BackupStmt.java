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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;

import com.google.common.collect.Maps;

import java.util.Map;

public class BackupStmt extends AbstractBackupStmt {
    private static final String PROP_TYPE = "type";
    public static final String PROP_CONTENT = "content";
    public static final String PROP_BACKUP_TABLES_ERROR_IGNORE_RATIO = "backup_tables_error_ignore_ratio";

    public enum BackupType {
        INCREMENTAL, FULL
    }

    public enum BackupContent {
        METADATA_ONLY, ALL
    }

    private BackupType type = BackupType.FULL;
    private BackupContent content = BackupContent.ALL;

    private double backupTablesErrorIgnoreRatio = 0.0;

    public BackupStmt(LabelName labelName, String repoName, AbstractBackupTableRefClause abstractBackupTableRefClause,
                      Map<String, String> properties) {
        super(labelName, repoName, abstractBackupTableRefClause, properties);
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public BackupType getType() {
        return type;
    }

    public BackupContent getContent() {
        return content;
    }

    public double getBackupTablesErrorIgnoreRatio() {
        return backupTablesErrorIgnoreRatio;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
    }

    @Override
    protected void customAnalyzeTableRefClause() throws AnalysisException {
        // tbl refs can not set alias in backup
        for (TableRef tblRef : abstractBackupTableRefClause.getTableRefList()) {
            if (tblRef.hasExplicitAlias()) {
                throw new AnalysisException("Can not set alias for table in Backup Stmt: " + tblRef);
            }
        }
    }

    @Override
    protected void analyzeProperties() throws AnalysisException {
        super.analyzeProperties();

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
        // backup_tables_error_ignore_ratio
        if (copiedProperties.containsKey(PROP_BACKUP_TABLES_ERROR_IGNORE_RATIO)) {
            try {
                backupTablesErrorIgnoreRatio =
                                Double.parseDouble(copiedProperties.get(PROP_BACKUP_TABLES_ERROR_IGNORE_RATIO));
                if (backupTablesErrorIgnoreRatio < 0.0 || backupTablesErrorIgnoreRatio >= 1.0) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                            "Invalid backup_tables_error_ignore_ratio: " + backupTablesErrorIgnoreRatio);
                }
            } catch (IllegalArgumentException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                        "Invalid backup_tables_error_ignore_ratio: "
                        + copiedProperties.get(PROP_BACKUP_TABLES_ERROR_IGNORE_RATIO));
            }
            copiedProperties.remove(PROP_BACKUP_TABLES_ERROR_IGNORE_RATIO);
        }

        if (!copiedProperties.isEmpty()) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                    "Unknown backup job properties: " + copiedProperties.keySet());
        }
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("BACKUP SNAPSHOT ").append(labelName.toSql());
        sb.append("\n").append("TO ").append(repoName).append("\n");
        if (abstractBackupTableRefClause != null) {
            sb.append(abstractBackupTableRefClause.toSql()).append("\n");
        }
        sb.append("PROPERTIES\n(");
        sb.append(new PrintableMap<>(properties, " = ", true, true));
        sb.append("\n)");
        return sb.toString();
    }
}
