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

import org.apache.doris.common.UserException;
import org.apache.doris.job.SqlJob.JobType;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

public class SubmitAsyncJobStmt extends DdlStmt {
    private final StatementBase execStmt;
    private final JobType type;
    private String label;
    private final Map<String, String> properties;

    public SubmitAsyncJobStmt(StatementBase execStmt, String label, Map<String, String> properties) {
        if (execStmt instanceof InsertStmt) {
            type = JobType.INSERT;
        } else {
            type = JobType.UNKNOWN;
        }
        this.execStmt = execStmt;
        this.label = label;
        this.properties = properties == null ? Collections.emptyMap() : properties;
    }

    public StatementBase getExecStmt() {
        return execStmt;
    }

    public JobType getType() {
        return type;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        execStmt.analyze(analyzer);
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SUBMIT SQL JOB (\n").append(execStmt.toSql()).append("\n)\nPROPERTIES(\n");
        stringBuilder.append(properties.entrySet().stream().map(
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
