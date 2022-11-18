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
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;
import java.util.stream.Collectors;

public class AlterResourceQueueStmt extends DdlStmt {
    private final String queueName;
    private final Map<String, String> newConfig;
    private final Map<String, String> newPolicy;

    public AlterResourceQueueStmt(String queueName, Map<String, String> newConfig, Map<String, String> newPolicy) {
        this.queueName = queueName;
        this.newConfig = newConfig;
        this.newPolicy = newPolicy;
    }

    public String getQueueName() {
        return queueName;
    }

    public Map<String, String> getNewConfig() {
        return newConfig;
    }

    public Map<String, String> getNewPolicy() {
        return newPolicy;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (!Env.getCurrentEnv().getAuth().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ALTER RESOURCE QUEUE ");
        stringBuilder.append("`").append(queueName).append("`\n").append("SET RESOURCE (\n");
        stringBuilder.append(newConfig.entrySet().stream().map(
                        kv -> String.format("    \"%s\" = \"%s\"", kv.getKey(), kv.getValue()))
                .collect(Collectors.joining(",\n")));
        stringBuilder.append("\n)\nSET MATCHING POLICY (\n");
        stringBuilder.append(newPolicy.entrySet().stream().map(
                        kv -> String.format("    \"%s\" = \"%s\"", kv.getKey(), kv.getValue()))
                .collect(Collectors.joining(",\n")));
        stringBuilder.append("\n)");
        return stringBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
