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
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

public class AdminRepairTableStmt extends DdlStmt {

    private TableRef tblRef;
    private List<String> partitions = Lists.newArrayList();

    private Map<String, String> properties = Maps.newHashMap();

    private long timeoutS = 0;

    public AdminRepairTableStmt(TableRef tblRef, Map<String, String> properties) {
        this.tblRef = tblRef;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        String dbName = null;
        if (Strings.isNullOrEmpty(tblRef.getName().getDb())) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        } else {
            dbName = ClusterNamespace.getFullName(getClusterName(), tblRef.getName().getDb());
        }

        tblRef.getName().setDb(dbName);

        if (tblRef.getPartitions() != null && !tblRef.getPartitions().isEmpty()) {
            partitions.addAll(tblRef.getPartitions());
        }

        if (properties != null && !properties.isEmpty()) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().equalsIgnoreCase("timeout")) {
                    try {
                        timeoutS = Long.valueOf(entry.getValue());
                    } catch (NumberFormatException e) {
                        throw new AnalysisException("Invalid timeout format: " + entry.getValue());
                    }
                } else {
                    throw new AnalysisException("Unknown property: " + entry.getKey());
                }
            }
        } else {
            timeoutS = 4 * 3600; // default 4 hours
        }
    }

    public String getDbName() {
        return tblRef.getName().getDb();
    }

    public String getTblName() {
        return tblRef.getName().getTbl();
    }

    public List<String> getPartitions() {
        return partitions;
    }

    public long getTimeoutS() {
        return timeoutS;
    }
}
