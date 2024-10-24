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
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.plugin.DynamicPluginLoader;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

public class InstallPluginStmt extends DdlStmt implements NotFallbackInParser {

    private String pluginPath;
    private Map<String, String> properties;

    public InstallPluginStmt(String pluginPath, Map<String, String> properties) {
        this.pluginPath = pluginPath;
        this.properties = properties;
    }

    public String getPluginPath() {
        return pluginPath;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getMd5sum() {
        return properties == null ? null : properties.get(DynamicPluginLoader.MD5SUM_KEY);
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        if (!Config.plugin_enable) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_OPERATION_DISABLED, "INSTALL PLUGIN",
                    "Please enable it by setting 'plugin_enable' = 'true'");
        }

        // check operation privilege
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("INSTALL PLUGIN FROM ").append("\"" + pluginPath + "\"");
        if (properties != null && !properties.isEmpty()) {
            sb.append("\nPROPERTIES (");
            sb.append(new PrintableMap<String, String>(properties, " = ", true, true, true));
            sb.append(")");
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSTALL;
    }
}
