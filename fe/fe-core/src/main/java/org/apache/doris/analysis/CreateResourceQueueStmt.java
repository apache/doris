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

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class CreateResourceQueueStmt extends DdlStmt {
    private final boolean ifNotExists;
    private final String name;
    private final Map<String, String> config;
    private final Map<String, String> policy;

    public CreateResourceQueueStmt(boolean ifNotExists, String name, Map<String, String> config,
            Map<String, String> policy) {
        this.ifNotExists = ifNotExists;
        this.name = name;
        this.config = config == null ? Collections.emptyMap() : config;
        this.policy = policy == null ? Collections.emptyMap() : policy;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getName() {
        return name;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public Map<String, String> getPolicy() {
        return policy;
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
        stringBuilder.append("CREATE RESOURCE QUEUE ");
        if (ifNotExists) {
            stringBuilder.append("IF NOT EXISTS ");
        }
        stringBuilder.append("`").append(name).append("`\n").append("WITH RESOURCE (\n");
        stringBuilder.append(config.entrySet().stream().map(
                        kv -> String.format("    \"%s\" = \"%s\"", kv.getKey(), kv.getValue()))
                .collect(Collectors.joining(",\n")));
        stringBuilder.append("\n)\nWITH MATCHING POLICY (\n");
        stringBuilder.append(policy.entrySet().stream().map(
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
