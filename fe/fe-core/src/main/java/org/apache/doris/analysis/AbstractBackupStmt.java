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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class AbstractBackupStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(AbstractBackupStmt.class);

    private static final String PROP_TIMEOUT = "timeout";
    private static final long MIN_TIMEOUT_MS = 600 * 1000L; // 10 min

    protected LabelName labelName;
    protected String repoName;
    protected AbstractBackupTableRefClause abstractBackupTableRefClause;
    protected Map<String, String> properties;

    protected long timeoutMs;

    public AbstractBackupStmt(LabelName labelName, String repoName,
            AbstractBackupTableRefClause abstractBackupTableRefClause,
            Map<String, String> properties) {
        this.labelName = labelName;
        this.repoName = repoName;
        this.abstractBackupTableRefClause = abstractBackupTableRefClause;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        labelName.analyze(analyzer);

        // user need database level privilege(not table level), because when doing restore operation,
        // the restore table may be newly created, so we can not judge its privileges.
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME,
                        labelName.getDbName(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }

        analyzeTableRefClause();
        analyzeProperties();
    }

    private void analyzeTableRefClause() throws UserException {
        if (abstractBackupTableRefClause == null) {
            return;
        }
        checkTableRefWithoutDatabase();
        abstractBackupTableRefClause.analyze(analyzer);
        customAnalyzeTableRefClause();
    }

    private void checkTableRefWithoutDatabase() throws AnalysisException {
        for (TableRef tblRef : abstractBackupTableRefClause.getTableRefList()) {
            if (!Strings.isNullOrEmpty(tblRef.getName().getDb())) {
                throw new AnalysisException("Cannot specify database name on backup objects: "
                        + tblRef.getName().getTbl() + ". Specify database name before label");
            }
            // set db name because we can not persist empty string when writing bdbje log
            tblRef.getName().setDb(labelName.getDbName());
        }
    }

    protected void customAnalyzeTableRefClause() throws AnalysisException {
    }

    protected void analyzeProperties() throws AnalysisException {
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
    }

    public String getDbName() {
        return labelName.getDbName();
    }

    public String getLabel() {
        return labelName.getLabelName();
    }

    public LabelName getLabelName() {
        return labelName;
    }

    public String getRepoName() {
        return repoName;
    }

    public AbstractBackupTableRefClause getAbstractBackupTableRefClause() {
        return abstractBackupTableRefClause;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    public void removeProperty(String key) {
        properties.remove(key);
    }
}
