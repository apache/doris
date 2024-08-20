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
import org.apache.doris.catalog.Resource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;

public class AlterResourceStmt extends DdlStmt {
    private static final String TYPE = "type";

    private final String resourceName;
    private final Map<String, String> properties;

    public AlterResourceStmt(String resourceName, Map<String, String> properties) {
        this.resourceName = resourceName;
        this.properties = properties;
    }

    public String getResourceName() {
        return resourceName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Resource properties can't be null");
        }

        // check type in properties
        if (properties.containsKey(TYPE)) {
            throw new AnalysisException("Can not change resource type.");
        }

        // check resource existence
        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new AnalysisException("Unknown resource: " + resourceName);
        }
        // check properties
        resource.checkProperties(properties);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER RESOURCE '").append(resourceName).append("' ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false, true)).append(")");
        return sb.toString();
    }

    @Override
    public boolean needAuditEncryption() {
        return true;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }
}
