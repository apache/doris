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
import org.apache.doris.common.util.PrintableMap;

import com.google.common.base.Strings;

import java.util.Map;

// clause which is used to modify partition properties
public class ModifyPartitionClause extends AlterTableClause {

    private String partitionName;
    private Map<String, String> properties;

    public String getPartitionName() {
        return partitionName;
    }

    public ModifyPartitionClause(String partitionName, Map<String, String> properties) {
        this.partitionName = partitionName;
        this.properties = properties;
        // ATTN: currently, modify partition only allow 3 kinds of operations:
        // 1. modify replication num
        // 2. modify data property
        // 3. modify in memory
        // And these 2 operations does not require table to be stable.
        // If other kinds of operations be added later, "needTableStable" may be changed.
        this.needTableStable = false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName)) {
            throw new AnalysisException("Partition name is not set");
        }

        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Properties is not set");
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("MODIFY PARTITION ");
        sb.append(partitionName);
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
