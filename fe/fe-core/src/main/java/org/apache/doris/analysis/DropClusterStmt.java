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
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Strings;

@Deprecated
public class DropClusterStmt extends DdlStmt {
    private boolean ifExists;
    private String name;

    public DropClusterStmt(boolean ifExists, String name) {
        this.ifExists = ifExists;
        this.name = name;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        if (Config.disable_cluster_feature) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_INVALID_OPERATION, "DROP CLUSTER");
        }

        if (Strings.isNullOrEmpty(name)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NAME_NULL);
        }

        if (name.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            throw new AnalysisException("Can not drop " + SystemInfoService.DEFAULT_CLUSTER);
        }

        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.OPERATOR)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PERMISSIONS);
        }
    }

    @Override
    public String toSql() {
        return "DROP CLUSTER " + name;
    }

    public String getClusterName() {
        return name;
    }

    public void setClusterName(String clusterName) {
        this.name = clusterName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public void setIfExists(boolean ifExists) {
        this.ifExists = ifExists;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

}
