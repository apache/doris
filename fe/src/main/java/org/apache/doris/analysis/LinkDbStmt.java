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
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

@Deprecated
public class LinkDbStmt extends DdlStmt {

    private ClusterName src;
    private ClusterName dest;
    private String srcCluster;
    private String destCluster;
    private String srcDb;
    private String destDb;

    LinkDbStmt(ClusterName src, ClusterName dest) {
        this.src = src;
        this.dest = dest;
    }

    public String getSrcCluster() {
        return srcCluster;
    }

    public String getDestCluster() {
        return destCluster;
    }

    public String getSrcDb() {
        return srcDb;
    }

    public String getDestDb() {
        return destDb;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "LINK DATABASE");
        }

        src.analyze(analyzer);
        dest.analyze(analyzer);

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                                                "ADMIN");
        }

        if (Strings.isNullOrEmpty(src.getCluster()) || Strings.isNullOrEmpty(dest.getCluster())
                || Strings.isNullOrEmpty(src.getDb()) || Strings.isNullOrEmpty(dest.getDb())) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PARAMETER);
        }
        srcCluster = src.getCluster();
        srcDb = ClusterNamespace.getFullName(srcCluster, src.getDb());
        destCluster = dest.getCluster();
        destDb = ClusterNamespace.getFullName(destCluster, dest.getDb());
    }

    @Override
    public String toSql() {
        return "LINK DATABASE " + srcCluster + "." + srcDb + " " + destCluster + "." + destDb;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
