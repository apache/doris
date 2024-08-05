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
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;

import lombok.Data;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Alter policy
 **/
@Data
public class AlterPolicyStmt extends DdlStmt {
    private final String policyName;
    private final Map<String, String> properties;

    public AlterPolicyStmt(String policyName, Map<String, String> properties) {
        this.policyName = policyName;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // check auth
        // check if can alter policy and use storage_resource
        if (!Env.getCurrentEnv().getAccessManager()
                .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                    PrivPredicate.ADMIN.getPrivs().toString());
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("policy properties can't be null");
        }

        if (Env.getCurrentEnv().getPolicyMgr().findPolicy(this.policyName, PolicyTypeEnum.ROW).isPresent()) {
            throw new AnalysisException("Current not support alter row policy");
        }

        // check resource existence
        List<Policy> policiesByType = Env.getCurrentEnv().getPolicyMgr()
                .getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
        Optional<Policy> hasPolicy = policiesByType.stream()
                .filter(policy -> policy.getPolicyName().equals(this.policyName)).findAny();
        StoragePolicy storagePolicy = (StoragePolicy) hasPolicy.orElseThrow(
                () -> new AnalysisException("Unknown storage policy: " + this.policyName)
        );

        // default storage policy use alter storage policy to add s3 resource.
        if (!policyName.equalsIgnoreCase(StoragePolicy.DEFAULT_STORAGE_POLICY_NAME) && properties.containsKey(
                StoragePolicy.STORAGE_RESOURCE)) {
            throw new AnalysisException("not support change storage policy's storage resource"
                    + ", you can change s3 properties by alter resource");
        }

        // check properties
        storagePolicy.checkProperties(properties);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER POLICY '").append(policyName).append("' ");
        sb.append("PROPERTIES(").append(new PrintableMap<>(properties, " = ", true, false)).append(")");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.ALTER;
    }

}
