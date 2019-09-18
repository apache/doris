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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

public class AbstractBackupStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(AbstractBackupStmt.class);

    private final static String PROP_TIMEOUT = "timeout";
    private final static long MIN_TIMEOUT_MS = 600 * 1000L; // 10 min

    protected LabelName labelName;
    protected String repoName;
    protected List<TableRef> tblRefs;
    protected Map<String, String> properties;

    protected long timeoutMs;

    public AbstractBackupStmt(LabelName labelName, String repoName, List<TableRef> tableRefs,
            Map<String, String> properties) {
        this.labelName = labelName;
        this.repoName = repoName;
        this.tblRefs = tableRefs;
        if (this.tblRefs == null) {
            this.tblRefs = Lists.newArrayList();
        }

        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        labelName.analyze(analyzer);
        
        // user need database level priv(not table level), because when doing restore operation,
        // the restore table may be newly created, so we can not judge its privs.
        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(),
                labelName.getDbName(), PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "LOAD");
        }

        checkAndNormalizeBackupObjs();

        analyzeProperties();
    }

    private void checkAndNormalizeBackupObjs() throws AnalysisException {
        for (TableRef tblRef : tblRefs) {
            if (!Strings.isNullOrEmpty(tblRef.getName().getDb())) {
                throw new AnalysisException("Cannot specify database name on backup objects: "
                        + tblRef.getName().getTbl() + ". Sepcify database name before label");
            }
            // set db name because we can not persist empty string when writing bdbje log
            tblRef.getName().setDb(labelName.getDbName());
        }
        
        // normalize
        // table name => table ref
        Map<String, TableRef> tblPartsMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        for (TableRef tblRef : tblRefs) {
            String tblName = tblRef.getName().getTbl();

            if (!tblPartsMap.containsKey(tblName)) {
                tblPartsMap.put(tblName, tblRef);
            } else {
                throw new AnalysisException("Duplicated restore table: " + tblName);
            }
        }
        
        // update table ref
        tblRefs.clear();
        for (TableRef tableRef : tblPartsMap.values()) {
            tblRefs.add(tableRef);
        }

        LOG.debug("table refs after normalization: \n{}", Joiner.on("\n").join(tblRefs));
    }

    protected void analyzeProperties() throws AnalysisException {
        // timeout
        if (properties.containsKey("timeout")) {
            try {
                timeoutMs = Long.valueOf(properties.get(PROP_TIMEOUT));
            } catch (NumberFormatException e) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_COMMON_ERROR,
                                                    "Invalid timeout format: "
                                                            + properties.get(PROP_TIMEOUT));
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

    public List<TableRef> getTableRefs() {
        return tblRefs;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public long getTimeoutMs() {
        return timeoutMs;
    }
}

