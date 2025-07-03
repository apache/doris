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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

// Modify version of specified partition. Only used in emergency.
/*
 *  admin set table db.tbl partition version properties ("key" = "val", ..);
 *      "partition_id" = "20010",
 *      "visible_version" = "101"
 */
public class AdminSetPartitionVersionStmt extends DdlStmt implements NotFallbackInParser {
    private long partitionId = -1;
    private long visibleVersion = -1;
    private final TableName tableName;
    private final Map<String, String> properties;

    public AdminSetPartitionVersionStmt(TableName tableName, Map<String, String> properties) {
        this.tableName = tableName;
        this.properties = properties;
    }

    public String getDatabase() {
        return tableName.getDb();
    }

    public String getTable() {
        return tableName.getTbl();
    }

    public Long getPartitionId() {
        return partitionId;
    }

    public Long getVisibleVersion() {
        return visibleVersion;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        tableName.analyze(analyzer);
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        checkProperties();
    }

    private void checkProperties() throws AnalysisException {
        partitionId = PropertyAnalyzer.analyzePartitionId(properties);
        if (partitionId == -1) {
            throw new AnalysisException("Should specify 'partition_id' property.");
        }
        visibleVersion = PropertyAnalyzer.analyzeVisibleVersion(properties);
        if (visibleVersion == -1) {
            throw new AnalysisException("Should specify 'visible_version' property.");
        }
        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }
    }
}
