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

import org.apache.doris.analysis.PartitionKeyDesc.PartitionRangeType;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.base.Joiner;
import com.google.common.base.Joiner.MapJoiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Map;

public class SingleRangePartitionDesc {
    private boolean isAnalyzed;

    private boolean ifNotExists;

    private String partName;
    private PartitionKeyDesc partitionKeyDesc;
    private Map<String, String> properties;

    private DataProperty partitionDataProperty;
    private Short replicationNum;
    private Pair<Long, Long> versionInfo;

    // cluster name should be set in analysis phase, and is used for creating default replica allocation.
    private String clusterName;

    public SingleRangePartitionDesc(boolean ifNotExists, String partName, PartitionKeyDesc partitionKeyDesc,
                                    Map<String, String> properties) {
        this.ifNotExists = ifNotExists;
        
        this.isAnalyzed = false;

        this.partName = partName;
        this.partitionKeyDesc = partitionKeyDesc;
        this.properties = properties;

        this.partitionDataProperty = DataProperty.DEFAULT_HDD_DATA_PROPERTY;
        this.replicationNum = FeConstants.default_replication_num;
        this.clusterName = SystemInfoService.DEFAULT_CLUSTER;
    }

    public boolean isSetIfNotExists() {
        return ifNotExists;
    }

    public String getPartitionName() {
        return partName;
    }

    public PartitionKeyDesc getPartitionKeyDesc() {
        return partitionKeyDesc;
    }

    public DataProperty getPartitionDataProperty() {
        return partitionDataProperty;
    }

    public short getReplicationNum() {
        return replicationNum;
    }

    public Pair<Long, Long> getVersionInfo() {
        return versionInfo;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void analyze(int partColNum, Map<String, String> otherProperties) throws AnalysisException {
        if (isAnalyzed) {
            return;
        }

        FeNameFormat.checkPartitionName(partName);

        partitionKeyDesc.analyze(partColNum);

        if (otherProperties != null) {
            // use given properties
            if (properties != null && !properties.isEmpty()) {
                MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
                throw new AnalysisException("Unknown properties: " + mapJoiner.join(properties));
            }

            this.properties = otherProperties;
        }

        // analyze data property
        partitionDataProperty = PropertyAnalyzer.analyzeDataProperty(properties,
                                                                     DataProperty.DEFAULT_HDD_DATA_PROPERTY);
        Preconditions.checkNotNull(partitionDataProperty);

        // analyze replication num
        replicationNum = PropertyAnalyzer.analyzeReplicationNum(properties, FeConstants.default_replication_num);
        if (replicationNum == null) {
            throw new AnalysisException("Invalid replication number: " + replicationNum);
        }

        // analyze version info
        versionInfo = PropertyAnalyzer.analyzeVersionInfo(properties);

        if (otherProperties == null) {
            // check unknown properties
            if (properties != null && !properties.isEmpty()) {
                MapJoiner mapJoiner = Joiner.on(", ").withKeyValueSeparator(" = ");
                throw new AnalysisException("Unknown properties: " + mapJoiner.join(properties));
            }
        }

        if (ConnectContext.get() != null && !Strings.isNullOrEmpty(ConnectContext.get().getClusterName())) {
            // after introducing the replica allocation. we need the cluster info, so that we can convert
            // the "replicationNum" to "replicaAllocation"
            this.clusterName = ConnectContext.get().getClusterName();
        }

        this.isAnalyzed = true;
    }

    public boolean isAnalyzed() {
        return this.isAnalyzed;
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION ").append(partName);
        if (partitionKeyDesc.getPartitionType() == PartitionRangeType.LESS_THAN) {
            sb.append(" VALUES LESS THEN ");
        } else {
            sb.append(" VALUES ");
        }
        sb.append(partitionKeyDesc.toSql());

        if (properties != null && !properties.isEmpty()) {
            sb.append(" (");
            sb.append(new PrintableMap<String, String>(properties, "=", true, false));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
