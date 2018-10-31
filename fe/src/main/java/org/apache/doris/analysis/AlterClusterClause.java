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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

public class AlterClusterClause extends AlterClause {
    private AlterClusterType type;
    private Map<String, String> properties;
    private int instanceNum;
    private String password;

    public AlterClusterClause(AlterClusterType type, Map<String, String> properties) {
        this.type = type;
        this.properties = properties;
        instanceNum = 0;
        password = "";
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {

        if (properties == null || properties.size() == 0) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_PARAMETER);
        }

        if (!properties.containsKey(CreateClusterStmt.CLUSTER_INSTANCE_NUM)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_INSTANCE_NUM);
        }

        instanceNum = Integer.valueOf(properties.get(CreateClusterStmt.CLUSTER_INSTANCE_NUM));
        password = properties.get(CreateClusterStmt.CLUSTER_SUPERMAN_PASSWORD);
    }

    @Override
    public String toSql() {
        // TODO Auto-generated method stub
        return super.toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        super.readFields(in);
    }

    public int getInstanceNum() {
        return instanceNum;
    }

    public void setInstanceNum(int instanceNum) {
        this.instanceNum = instanceNum;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

}
