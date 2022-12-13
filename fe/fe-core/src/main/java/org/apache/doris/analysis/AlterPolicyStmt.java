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

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
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

        boolean hasCooldownDatetime = false;
        boolean hasCooldownTtl = false;

        if (properties.containsKey(StoragePolicy.COOLDOWN_DATETIME)) {
            hasCooldownDatetime = true;
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                df.parse(properties.get(StoragePolicy.COOLDOWN_DATETIME));
            } catch (ParseException e) {
                throw new AnalysisException(String.format("cooldown_datetime format error: %s",
                    properties.get(StoragePolicy.COOLDOWN_DATETIME)), e);
            }
        }

        if (properties.containsKey(StoragePolicy.COOLDOWN_TTL)) {
            hasCooldownTtl = true;
            if (Integer.parseInt(properties.get(StoragePolicy.COOLDOWN_TTL)) < 0) {
                throw new AnalysisException("cooldown_ttl must >= 0.");
            }
        }

        if (hasCooldownDatetime && hasCooldownTtl) {
            throw new AnalysisException(StoragePolicy.COOLDOWN_DATETIME + " and "
                + StoragePolicy.COOLDOWN_TTL + " can't be set together.");
        }
        if (!hasCooldownDatetime && !hasCooldownTtl) {
            throw new AnalysisException(StoragePolicy.COOLDOWN_DATETIME + " or "
                + StoragePolicy.COOLDOWN_TTL + " must be set");
        }

        do {
            if (policyName.equalsIgnoreCase(StoragePolicy.DEFAULT_STORAGE_POLICY_NAME)) {
                // default storage policy
                if (storagePolicy.getStorageResource() != null && hasCooldownDatetime) {
                    // alter cooldown datetime, can do
                    break;
                }

                if (storagePolicy.getStorageResource() != null && hasCooldownTtl) {
                    // alter cooldown ttl, can do
                    break;
                }

                if (storagePolicy.getStorageResource() == null) {
                    // alter add s3 resource, can do, check must have ttl or datetime.
                    if (hasCooldownTtl == false && hasCooldownDatetime == false) {
                        throw new AnalysisException("please alter default policy to add s3 , ttl or datetime.");
                    }
                    break;
                }
                throw new AnalysisException("default storage policy has been set s3 Resource.");
            }
        } while (false);

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

}
