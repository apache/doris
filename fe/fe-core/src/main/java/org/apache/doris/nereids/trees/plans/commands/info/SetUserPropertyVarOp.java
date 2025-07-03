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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SetUserPropertyVarOp
 */
public class SetUserPropertyVarOp {
    private final String user;
    private final String key;
    private final String value;

    public SetUserPropertyVarOp(String user, String key, String value) {
        this.key = key;
        this.value = value;
        this.user = user != null ? user : ConnectContext.get().getQualifiedUser();
    }

    public String getPropertyKey() {
        return key;
    }

    public String getPropertyValue() {
        return value;
    }

    /**validate*/
    public void validate(ConnectContext ctx) throws UserException {
        if (Strings.isNullOrEmpty(key)) {
            throw new AnalysisException("User property key is null");
        }

        if (value == null) {
            throw new AnalysisException("User property value is null");
        }

        for (Pattern advPattern : UserProperty.ADVANCED_PROPERTIES) {
            Matcher matcher = advPattern.matcher(key);
            if (matcher.find()) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ctx, PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
                }
                return;
            }
        }
        boolean isSelf = user.equals(ctx.getQualifiedUser());
        for (Pattern commPattern : UserProperty.COMMON_PROPERTIES) {
            Matcher matcher = commPattern.matcher(key);
            if (matcher.find()) {
                if (!isSelf && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ctx,
                        PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR,
                            "GRANT");
                }
                if (Config.isCloudMode()) {
                    // check value, clusterName is valid.
                    if (key.equals(UserProperty.DEFAULT_CLOUD_CLUSTER)
                            && !Strings.isNullOrEmpty(value)
                            && !((CloudSystemInfoService) Env.getCurrentSystemInfo())
                            .getCloudClusterNames().contains(value)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_CLOUD_CLUSTER_ERROR, value);
                    }

                    if (key.equals(UserProperty.DEFAULT_COMPUTE_GROUP)
                            && !Strings.isNullOrEmpty(value)
                            && !((CloudSystemInfoService) Env.getCurrentSystemInfo())
                            .getCloudClusterNames().contains(value)) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_CLOUD_CLUSTER_ERROR, value);
                    }
                }
                return;
            }
        }

        throw new AnalysisException("Unknown property key: " + key);
    }

    /**toSql*/
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("'");
        sb.append(key);
        sb.append("' = ");
        if (value != null) {
            sb.append("'");
            sb.append(value);
            sb.append("'");
        } else {
            sb.append("NULL");
        }
        return sb.toString();
    }
}
