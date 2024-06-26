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

package org.apache.doris.cloud.analysis;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.ResourceTypeEnum;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a use cluster statement.
 */
public class UseCloudClusterStmt extends StatementBase {
    private static final Logger LOG = LogManager.getLogger(UseCloudClusterStmt.class);
    private String cluster;
    private String database;
    private String catalogName;

    public UseCloudClusterStmt(String cluster) {
        this.cluster = cluster;
    }

    public UseCloudClusterStmt(String cluster, String db) {
        this.cluster = cluster;
        this.database = db;
    }

    public UseCloudClusterStmt(String cluster, String db, String catalogName) {
        this.cluster = cluster;
        this.database = db;
        this.catalogName = catalogName;
    }

    public String getCluster() {
        return cluster;
    }

    public String getDatabase() {
        return database;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public String toSql() {
        return "USE `" + cluster + "`";
    }

    @Override
    public String toString() {
        return toSql();
    }

    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        // check resource usage privilege
        if (Strings.isNullOrEmpty(cluster)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_CLUSTER_ERROR);
        }
        if (!Env.getCurrentEnv().getAuth().checkCloudPriv(ConnectContext.get().getCurrentUserIdentity(),
                cluster, PrivPredicate.USAGE, ResourceTypeEnum.CLUSTER)) {
            throw new AnalysisException("USAGE denied to user '" + ConnectContext.get().getQualifiedUser()
                + "'@'" + ConnectContext.get().getRemoteIP()
                + "' for cloud cluster '" + cluster + "'");
        }

        if (!((CloudSystemInfoService) Env.getCurrentSystemInfo()).getCloudClusterNames().contains(cluster)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLOUD_CLUSTER_ERROR, cluster);
        }

        if (Strings.isNullOrEmpty(database)) {
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkDbPriv(ConnectContext.get(),
                        StringUtils.isEmpty(catalogName) ? InternalCatalog.INTERNAL_CATALOG_NAME : catalogName,
                        database,
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DBACCESS_DENIED_ERROR,
                    analyzer.getQualifiedUser(), database);
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
