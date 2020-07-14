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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class BackupStmt extends AbstractBackupStmt {
    private final static String PROP_TYPE = "type";

    public enum BackupType {
        INCREMENTAL, FULL
    }

    private BackupType type = BackupType.FULL;

    public BackupStmt(LabelName labelName, String repoName, List<TableRef> tblRefs, Map<String, String> properties) {
        super(labelName, repoName, tblRefs, properties);
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public BackupType getType() {
        return type;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // tbl refs can not set alias in backup
        for (TableRef tblRef : tblRefs) {
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
        if (copiedProperties.containsKey(PROP_TYPE)) {
            try {
                type = BackupType.valueOf(copiedProperties.get(PROP_TYPE).toUpperCase());
            } catch (Exception e) { 
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                                                    "Invalid backup job type: "
                                                            + copiedProperties.get(PROP_TYPE));
            }
            copiedProperties.remove(PROP_TYPE);
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
        sb.append("\n").append("TO ").append(repoName).append("\nON\n(");

        sb.append(Joiner.on(",\n").join(tblRefs));

        sb.append("\n)\nPROPERTIES\n(");
        sb.append(new PrintableMap<String, String>(properties, " = ", true, true));
        sb.append("\n)");
        return sb.toString();
    }
}