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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadschedpolicy.WorkloadActionMeta;
import org.apache.doris.resource.workloadschedpolicy.WorkloadConditionMeta;

import java.util.List;
import java.util.Map;

public class CreateWorkloadSchedPolicyStmt extends DdlStmt {

    private final boolean ifNotExists;
    private final String policyName;
    private final List<WorkloadConditionMeta> conditions;
    private final List<WorkloadActionMeta> actions;
    private final Map<String, String> properties;

    public CreateWorkloadSchedPolicyStmt(boolean ifNotExists, String policyName,
            List<WorkloadConditionMeta> conditions, List<WorkloadActionMeta> actions, Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        this.policyName = policyName;
        this.conditions = conditions;
        this.actions = actions;
        this.properties = properties;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        // check name
        FeNameFormat.checkWorkloadSchedPolicyName(policyName);

        if (conditions == null || conditions.size() < 1) {
            throw new DdlException("At least one condition needs to be specified");
        }

        if (actions == null || actions.size() < 1) {
            throw new DdlException("At least one action needs to be specified");
        }
    }

    public String getPolicyName() {
        return policyName;
    }

    public List<WorkloadConditionMeta> getConditions() {
        return conditions;
    }

    public List<WorkloadActionMeta> getActions() {
        return actions;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toSql() {
        String str = "";
        str = str + "CREAYE ";
        str = str + "WORKLOAD SCHEDULE POLICY " + policyName + " ";

        str = str + " CONDITIONS( ";
        if (conditions != null) {
            for (WorkloadConditionMeta wcm : conditions) {
                str += wcm.toString() + ",";
            }
        }
        str = str.substring(0, str.length() - 1);
        str = str + ")";

        str = str + " ACTIONS( ";
        if (actions != null) {
            for (WorkloadActionMeta wam : actions) {
                str = str + wam.toString() + ",";
            }
        }
        str = str.substring(0, str.length() - 1);
        str = str + ")";

        str = str + " PROPERTIES(";
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            str = str + "\"" + entry.getKey() + "\"" + "=" + "\"" + entry.getValue() + "\",";
        }
        str = str.substring(0, str.length() - 1);
        str = str + ")";

        return str;
    }
}
