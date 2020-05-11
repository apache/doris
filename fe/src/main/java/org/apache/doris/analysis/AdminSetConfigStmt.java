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
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import java.util.Map;

// admin set frontend config ("key" = "value");
public class AdminSetConfigStmt extends DdlStmt {
    
    public enum ConfigType {
        FRONTEND,
        BACKEND
    }

    private ConfigType type;
    private Map<String, String> configs;

    private RedirectStatus redirectStatus = RedirectStatus.NO_FORWARD;

    public AdminSetConfigStmt(ConfigType type, Map<String, String> configs) {
        this.type = type;
        this.configs = configs;
        if (this.configs == null) {
            this.configs = Maps.newHashMap();
        }

        // we have to analyze configs here to determine whether to forward it to master
        for (String key : this.configs.keySet()) {
            if (ConfigBase.checkIsMasterOnly(key)) {
                redirectStatus = RedirectStatus.FORWARD_NO_SYNC;
            }
        }
    }

    public ConfigType getType() {
        return type;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);

        if (configs.size() != 1) {
            throw new AnalysisException("config parameter size is not equal to 1");
        }
        // check auth
        if (!Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (type != ConfigType.FRONTEND) {
            throw new AnalysisException("Only support setting Frontend configs now");
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return redirectStatus;
    }
}
