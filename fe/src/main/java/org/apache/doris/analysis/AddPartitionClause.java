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

import org.apache.doris.common.AnalysisException;

import java.util.Map;

// clause which is used to add partition
public class AddPartitionClause extends AlterTableClause {

    private SingleRangePartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private Map<String, String> properties;

    public SingleRangePartitionDesc getSingeRangePartitionDesc() {
        return partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return distributionDesc;
    }

    public AddPartitionClause(SingleRangePartitionDesc partitionDesc,
                              DistributionDesc distributionDesc,
                              Map<String, String> properties) {
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
        this.needTableStable = false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {

    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        sb.append(partitionDesc.toSql() + "\n");
        if (distributionDesc != null) {
            sb.append(distributionDesc.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
