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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PrintableMap;

import java.util.List;
import java.util.Map;

// clause which is used to modify partition properties
public class ModifyPartitionClause extends AlterTableClause {

    private List<String> partitionNames;
    private Map<String, String> properties;
    private boolean needExpand = false;

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public ModifyPartitionClause(List<String> partitionNames, Map<String, String> properties) {
        super(AlterOpType.MODIFY_PARTITION);
        this.partitionNames = partitionNames;
        this.properties = properties;
        // ATTN: currently, modify partition only allow 3 kinds of operations:
        // 1. modify replication num
        // 2. modify data property
        // 3. modify in memory
        // And these 3 operations does not require table to be stable.
        // If other kinds of operations be added later, "needTableStable" may be changed.
        this.needTableStable = false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (partitionNames == null || partitionNames.isEmpty()) {
            throw new AnalysisException("Partition names is not set or empty");
        }

        if (partitionNames.stream().anyMatch(entity -> Strings.isNullOrEmpty(entity))) {
            throw new AnalysisException("there are empty partition name");
        }

        if (partitionNames.stream().anyMatch(entity -> entity.equals("*"))) {
            this.needExpand = true;
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Properties is not set");
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    public boolean isNeedExpand() {
        return this.needExpand;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY PARTITION ");
        sb.append("(");
        sb.append(Joiner.on(", ").join(partitionNames));
        sb.append(")");
        sb.append(" SET (");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");
        
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
