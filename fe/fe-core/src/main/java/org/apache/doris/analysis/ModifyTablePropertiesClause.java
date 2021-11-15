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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.TableProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.DynamicPartitionUtil;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;

import java.util.Map;

// clause which is used to modify table properties
public class ModifyTablePropertiesClause extends AlterTableClause {

    private Map<String, String> properties;

    public ModifyTablePropertiesClause(Map<String, String> properties) {
        super(AlterOpType.MODIFY_TABLE_PROPERTY);
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Properties is not set");
        }

        if (properties.size() != 1
                && !TableProperty.isSamePrefixProperties(properties, TableProperty.DYNAMIC_PARTITION_PROPERTY_PREFIX)) {
            throw new AnalysisException("Can only set one table property at a time");
        }

        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_COLOCATE_WITH)) {
            this.needTableStable = false;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE).equalsIgnoreCase("column")) {
                throw new AnalysisException("Can only change storage type to COLUMN");
            }
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_DISTRIBUTION_TYPE).equalsIgnoreCase("hash")) {
                throw new AnalysisException("Can only change distribution type to HASH");
            }
            this.needTableStable = false;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK).equalsIgnoreCase("true")) {
                throw new AnalysisException(
                        "Property " + PropertyAnalyzer.PROPERTIES_SEND_CLEAR_ALTER_TASK + " should be set to true");
            }
            this.needTableStable = false;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_COLUMNS)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_BF_FPP)) {
            // do nothing, these 2 properties will be analyzed when creating alter job
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT)) {
            if (!properties.get(PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT).equalsIgnoreCase("v2")) {
                throw new AnalysisException(
                        "Property " + PropertyAnalyzer.PROPERTIES_STORAGE_FORMAT + " should be v2");
            }
        } else if (DynamicPartitionUtil.checkDynamicPartitionPropertiesExist(properties)) {
            // do nothing, dynamic properties will be analyzed in SchemaChangeHandler.process
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)
                || properties.containsKey(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
            ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "");
            properties.put(PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
        } else if (properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_NUM)
                || properties.containsKey("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION)) {
            ReplicaAllocation replicaAlloc = PropertyAnalyzer.analyzeReplicaAllocation(properties, "default");
            properties.put("default." + PropertyAnalyzer.PROPERTIES_REPLICATION_ALLOCATION, replicaAlloc.toCreateStmt());
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_INMEMORY)) {
            this.needTableStable = false;
            this.opType = AlterOpType.MODIFY_TABLE_PROPERTY_SYNC;
        } else if (properties.containsKey(PropertyAnalyzer.PROPERTIES_TABLET_TYPE)) {
            throw new AnalysisException("Alter tablet type not supported");
        } else {
            throw new AnalysisException("Unknown table property: " + properties.keySet());
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PROPERTIES (");
        sb.append(new PrintableMap<String, String>(properties, "=", true, false));
        sb.append(")");
        
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
