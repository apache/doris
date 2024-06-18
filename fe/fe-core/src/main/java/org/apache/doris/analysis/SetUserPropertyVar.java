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
import org.apache.doris.cloud.system.CloudSystemInfoService;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.UserProperty;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SetUserPropertyVar extends SetVar {
    public static final String DOT_SEPARATOR = ".";

    private final String key;
    private final String value;

    public SetUserPropertyVar(String key, String value) {
        this.key = key;
        this.value = value;
        this.varType = SetVarType.SET_USER_PROPERTY_VAR;
    }

    public String getPropertyKey() {
        return key;
    }

    public String getPropertyValue() {
        return value;
    }

    public void analyze(Analyzer analyzer, boolean isSelf) throws AnalysisException {
        if (Strings.isNullOrEmpty(key)) {
            throw new AnalysisException("User property key is null");
        }

        if (value == null) {
            throw new AnalysisException("User property value is null");
        }

        checkAccess(analyzer, isSelf);
    }

    private void checkAccess(Analyzer analyzer, boolean isSelf) throws AnalysisException {
        for (Pattern advPattern : UserProperty.ADVANCED_PROPERTIES) {
            Matcher matcher = advPattern.matcher(key);
            if (matcher.find()) {
                if (!Env.getCurrentEnv().getAccessManager()
                        .checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
                }
                return;
            }
        }

        for (Pattern commPattern : UserProperty.COMMON_PROPERTIES) {
            Matcher matcher = commPattern.matcher(key);
            if (matcher.find()) {
                if (!isSelf && !Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(),
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
                }
                return;
            }
        }

        throw new AnalysisException("Unknown property key: " + key);
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
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
